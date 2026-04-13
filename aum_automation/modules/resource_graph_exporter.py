"""
Azure Resource Graph Exporter
Handles querying Azure Resource Graph Explorer and exporting data to CSV
"""

import logging
import subprocess
import json
import csv
import os
import yaml
from datetime import datetime
from typing import Optional, List, Dict
from pathlib import Path
from azure.mgmt.resourcegraph import ResourceGraphClient
from azure.mgmt.resourcegraph.models import QueryRequest, QueryRequestOptions
from azure.mgmt.resource import ResourceManagementClient

logger = logging.getLogger(__name__)


class ResourceGraphExporter:
    """
    Manages Azure Resource Graph queries and exports data to CSV.
    
    Executes saved queries from Azure Resource Graph Explorer and saves results
    to the user's Downloads folder or specified location.
    """
    
    def __init__(self, config, auth_manager):
        self.config = config
        self.auth_manager = auth_manager
        self.downloads_folder = str(Path.home() / "Downloads")
        self.saved_query_lookup_timeout = 90
        self.saved_queries_config_path = os.path.join(
            os.path.dirname(__file__), '..', 'config', 'saved_queries.yaml'
        )
        self._load_saved_queries_from_config()
        
    def _ensure_authenticated(self) -> bool:
        """Verify that user is authenticated"""
        if not self.auth_manager.credential:
            logger.error("Not authenticated to Azure")
            return False
        return True
    
    def _get_subscriptions_in_scope(self) -> List[str]:
        """Get list of subscription IDs in current scope"""
        try:
            result = subprocess.run(
                ['az', 'account', 'list', '--query', '[].id', '-o', 'json'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                subscriptions = json.loads(result.stdout)
                logger.info(f"Found {len(subscriptions)} subscriptions in scope")
                return subscriptions
            else:
                logger.error(f"Failed to get subscriptions: {result.stderr}")
                return []
                
        except Exception as e:
            logger.error(f"Error getting subscriptions: {e}")
            return []
    
    def _execute_resource_graph_query(self, query_name: str, query_text: Optional[str] = None) -> Optional[List[Dict]]:
        """
        Execute a Resource Graph query using Azure SDK.
        
        Args:
            query_name: Name of the saved query or custom query
            query_text: Optional custom query text. If not provided, will try to use query_name
            
        Returns:
            List of result dictionaries or None on failure
        """
        try:
            if not self._ensure_authenticated():
                return None
            
            # Get all subscriptions in scope
            subscriptions = self._get_subscriptions_in_scope()
            if not subscriptions:
                logger.error("No subscriptions available for query")
                return None
            
            logger.info(f"Executing Resource Graph query: {query_name}")
            logger.info(f"Query scope: {len(subscriptions)} subscription(s)")
            
            # Use the Azure SDK instead of CLI for better reliability
            try:
                # Create Resource Graph client
                client = ResourceGraphClient(self.auth_manager.credential)
                
                # Prepare the query request
                query_request = QueryRequest(
                    subscriptions=subscriptions,
                    query=query_text if query_text else query_name,
                    options=QueryRequestOptions(
                        result_format="objectArray"  # Return as array of objects
                    )
                )
                
                # Execute the query
                logger.info("Sending query to Azure Resource Graph...")
                response = client.resources(query_request)
                
                # Extract data from response
                data = list(response.data) if response.data else []
                logger.info(f"Query returned {len(data)} rows")
                
                return data
                
            except Exception as sdk_error:
                # If SDK fails, fall back to CLI
                logger.warning(f"SDK query failed: {sdk_error}, trying CLI fallback")
                return self._execute_query_via_cli(query_name, query_text, subscriptions)
                
        except Exception as e:
            logger.error(f"Error executing query '{query_name}': {e}")
            return None
    
    def _execute_query_via_cli(self, query_name: str, query_text: Optional[str], subscriptions: List[str]) -> Optional[List[Dict]]:
        """
        Fallback method to execute query via Azure CLI.
        
        Args:
            query_name: Name of the query
            query_text: Query text to execute
            subscriptions: List of subscription IDs
            
        Returns:
            List of result dictionaries or None on failure
        """
        try:
            # Build subscription scope
            subscription_args = []
            for sub_id in subscriptions:
                subscription_args.extend(['--subscriptions', sub_id])
            
            # Build the query command
            query_to_execute = query_text if query_text else query_name
            
            logger.info(f"Executing via CLI: {query_name}")
            
            # Execute the query with first=1000 to handle pagination
            cmd = [
                'az', 'graph', 'query',
                '-q', query_to_execute,
                '--first', '1000',
                '--output', 'json'
            ] + subscription_args
            
            logger.debug(f"CLI command: {' '.join(cmd[:4])}...")
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30  # Reduced timeout to 30 seconds
            )
            
            if result.returncode == 0:
                response = json.loads(result.stdout)
                # Azure Resource Graph returns data in a specific format
                if 'data' in response:
                    data = response['data']
                else:
                    data = response
                    
                logger.info(f"CLI query returned {len(data)} rows")
                return data
            else:
                logger.error(f"CLI query failed: {result.stderr}")
                return None
                
        except subprocess.TimeoutExpired:
            logger.error(f"CLI query '{query_name}' timed out after 30 seconds")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse CLI query result: {e}")
            return None
        except Exception as e:
            logger.error(f"Error in CLI query execution: {e}")
            return None
    
    def export_to_csv(self, data: List[Dict], filename: str, output_folder: Optional[str] = None) -> Optional[str]:
        """
        Export query results to CSV file.
        
        Args:
            data: List of dictionaries containing query results
            filename: Base filename (without extension)
            output_folder: Optional output folder path, defaults to Downloads
            
        Returns:
            Full path to created CSV file or None on failure
        """
        try:
            if not data:
                logger.warning("No data to export")
                return None
            
            # Determine output folder
            if output_folder:
                output_dir = output_folder
            else:
                output_dir = self.downloads_folder
            
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_filename = f"{filename}_{timestamp}.csv"
            csv_path = os.path.join(output_dir, csv_filename)
            
            # Get all unique keys from all records
            all_keys = set()
            for record in data:
                all_keys.update(record.keys())
            
            fieldnames = sorted(list(all_keys))
            
            # Write to CSV
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
            
            logger.info(f"Exported {len(data)} rows to {csv_path}")
            return csv_path
            
        except Exception as e:
            logger.error(f"Error exporting to CSV: {e}")
            return None
    
    def _load_saved_queries_from_config(self):
        """Load saved query text from config file (for private Portal queries)."""
        self.manual_queries = {}
        try:
            if os.path.exists(self.saved_queries_config_path):
                with open(self.saved_queries_config_path, 'r') as f:
                    config = yaml.safe_load(f) or {}
                    for query_name, query_data in config.items():
                        if isinstance(query_data, dict) and 'query' in query_data:
                            query_text = query_data['query'].strip()
                            # Skip only placeholder queries (lines with JUST comments, no KQL)
                            if query_text and not (query_text.startswith('// PASTE') or query_text == ''):
                                self.manual_queries[query_name] = query_text
                                logger.info(f"Loaded manual query config: {query_name}")
        except Exception as e:
            logger.warning(f"Could not load saved_queries.yaml: {e}")

    def _get_saved_query_text(self, query_name: str) -> Optional[str]:
        """
        Retrieve a saved private query from Azure Resource Graph Explorer.
        
        Args:
            query_name: Name of the saved query in Azure
            
        Returns:
            Query text or None if not found
        """
        logger.info(f"Searching for saved query: {query_name}")

        # First check manual config (for private Portal queries)
        if query_name in self.manual_queries:
            logger.info(f"Found saved query '{query_name}' in manual config (saved_queries.yaml)")
            return self.manual_queries[query_name]

        query_text = self._get_saved_query_via_sdk(query_name)
        if query_text:
            return query_text

        query_text = self._get_saved_query_via_query_pack_resources(query_name)
        if query_text:
            return query_text

        query_text = self._get_saved_query_via_rest_provider(query_name)
        if query_text:
            return query_text

        query_text = self._get_saved_query_via_rest_subscriptions(query_name)
        if query_text:
            return query_text

        query_text = self._get_saved_query_via_shared_query_cli(query_name)
        if query_text:
            return query_text

        query_text = self._get_saved_query_via_resource_cli(query_name)
        if query_text:
            return query_text

        logger.error(f"Could not find saved query: {query_name}")
        return None

    def _extract_query_text_from_resource(self, resource: Dict) -> Optional[str]:
        """Extract query text from a Resource Graph saved-query resource payload."""
        properties = resource.get("properties") if isinstance(resource, dict) else None
        if not isinstance(properties, dict):
            return None
        return properties.get("query")

    def _resource_matches_query_name(self, resource: Dict, query_name: str) -> bool:
        """Match by ARM name or displayName/title in properties."""
        if not isinstance(resource, dict):
            return False

        if resource.get("name") == query_name:
            return True

        properties = resource.get("properties")
        if isinstance(properties, dict):
            for key in ("displayName", "title", "name"):
                if properties.get(key) == query_name:
                    return True

        return False

    def _get_saved_query_via_sdk(self, query_name: str) -> Optional[str]:
        """Retrieve saved query text using Azure SDK ResourceManagementClient."""
        try:
            credential = self.auth_manager.get_credential()
            subscriptions = self._get_subscriptions_in_scope()
            if not subscriptions:
                logger.error("No subscriptions available while searching saved queries")
                return None

            for subscription_id in subscriptions:
                try:
                    resource_client = ResourceManagementClient(credential, subscription_id)
                    resources = resource_client.resources.list(
                        filter="resourceType eq 'Microsoft.ResourceGraph/queries'"
                    )
                    for resource in resources:
                        if getattr(resource, "name", None) != query_name:
                            continue

                        query_text = None
                        properties = getattr(resource, "properties", None)
                        if isinstance(properties, dict):
                            query_text = properties.get("query")
                        else:
                            query_text = getattr(properties, "query", None)

                        if query_text:
                            logger.info(
                                f"Found saved query '{query_name}' via SDK in subscription {subscription_id}"
                            )
                            return query_text
                except Exception as sub_err:
                    logger.warning(
                        f"Saved query SDK lookup failed in subscription {subscription_id}: {sub_err}"
                    )

            logger.warning("Saved query not found via SDK resource enumeration")
            return None
        except Exception as e:
            logger.warning(f"Saved query lookup via SDK failed: {e}")
            return None

    def _get_saved_query_via_query_pack_resources(self, query_name: str) -> Optional[str]:
        """
        Retrieve saved query text by enumerating Query Pack query resources.

        Resource type: Microsoft.ResourceGraph/queryPacks/queries
        """
        try:
            result = subprocess.run(
                [
                    'az', 'resource', 'list',
                    '--resource-type', 'Microsoft.ResourceGraph/queryPacks/queries',
                    '--output', 'json'
                ],
                capture_output=True,
                text=True,
                timeout=self.saved_query_lookup_timeout
            )

            if result.returncode != 0:
                logger.warning(f"Query-pack resource lookup failed: {result.stderr.strip()}")
                return None

            resources = json.loads(result.stdout or "[]")
            for resource in resources:
                if not self._resource_matches_query_name(resource, query_name):
                    continue
                query_text = self._extract_query_text_from_resource(resource)
                if query_text:
                    logger.info(f"Found saved query '{query_name}' via query-pack resources")
                    return query_text

            logger.warning("Saved query not found via query-pack resources")
            return None
        except subprocess.TimeoutExpired:
            logger.warning(
                f"Query-pack resource lookup timed out after {self.saved_query_lookup_timeout} seconds"
            )
            return None
        except Exception as e:
            logger.warning(f"Saved query lookup via query-pack resources failed: {e}")
            return None

    def _get_saved_query_via_shared_query_cli(self, query_name: str) -> Optional[str]:
        """Retrieve saved query text using `az graph shared-query list`."""
        try:
            result = subprocess.run(
                ['az', 'graph', 'shared-query', 'list', '--output', 'json'],
                capture_output=True,
                text=True,
                timeout=self.saved_query_lookup_timeout
            )

            if result.returncode != 0:
                logger.warning(f"Shared-query CLI lookup failed: {result.stderr.strip()}")
                return None

            queries = json.loads(result.stdout or "[]")
            for query in queries:
                if query.get('name') != query_name:
                    continue
                query_text = query.get('query') or query.get('properties', {}).get('query')
                if query_text:
                    logger.info(f"Found saved query '{query_name}' via shared-query CLI")
                    return query_text

            logger.warning("Saved query not found via shared-query CLI")
            return None
        except subprocess.TimeoutExpired:
            logger.warning(
                f"Shared-query CLI lookup timed out after {self.saved_query_lookup_timeout} seconds"
            )
            return None
        except Exception as e:
            logger.warning(f"Saved query lookup via shared-query CLI failed: {e}")
            return None

    def _get_saved_query_via_rest_provider(self, query_name: str) -> Optional[str]:
        """
        Retrieve saved query text from tenant provider scope using az rest.
        URL: /providers/Microsoft.ResourceGraph/queries
        """
        try:
            result = subprocess.run(
                [
                    'az', 'rest',
                    '--method', 'get',
                    '--url', 'https://management.azure.com/providers/Microsoft.ResourceGraph/queries?api-version=2021-03-01',
                    '--output', 'json'
                ],
                capture_output=True,
                text=True,
                timeout=self.saved_query_lookup_timeout
            )

            if result.returncode != 0:
                logger.warning(f"Provider-scope REST lookup failed: {result.stderr.strip()}")
                return None

            payload = json.loads(result.stdout or "{}")
            queries = payload.get("value", []) if isinstance(payload, dict) else []
            for query_resource in queries:
                if not self._resource_matches_query_name(query_resource, query_name):
                    continue
                query_text = self._extract_query_text_from_resource(query_resource)
                if query_text:
                    logger.info(f"Found saved query '{query_name}' via provider-scope REST")
                    return query_text

            logger.warning("Saved query not found via provider-scope REST")
            return None
        except subprocess.TimeoutExpired:
            logger.warning(
                f"Provider-scope REST lookup timed out after {self.saved_query_lookup_timeout} seconds"
            )
            return None
        except Exception as e:
            logger.warning(f"Saved query lookup via provider-scope REST failed: {e}")
            return None

    def _get_saved_query_via_rest_subscriptions(self, query_name: str) -> Optional[str]:
        """
        Retrieve saved query text from subscription provider scope using az rest.
        URL: /subscriptions/{id}/providers/Microsoft.ResourceGraph/queries
        """
        try:
            subscriptions = self._get_subscriptions_in_scope()
            if not subscriptions:
                logger.warning("No subscriptions available for subscription-scope REST lookup")
                return None

            for subscription_id in subscriptions:
                result = subprocess.run(
                    [
                        'az', 'rest',
                        '--method', 'get',
                        '--url',
                        f'https://management.azure.com/subscriptions/{subscription_id}/providers/Microsoft.ResourceGraph/queries?api-version=2021-03-01',
                        '--output', 'json'
                    ],
                    capture_output=True,
                    text=True,
                    timeout=self.saved_query_lookup_timeout
                )

                if result.returncode != 0:
                    logger.warning(
                        f"Subscription-scope REST lookup failed in {subscription_id}: {result.stderr.strip()}"
                    )
                    continue

                payload = json.loads(result.stdout or "{}")
                queries = payload.get("value", []) if isinstance(payload, dict) else []
                for query_resource in queries:
                    if not self._resource_matches_query_name(query_resource, query_name):
                        continue
                    query_text = self._extract_query_text_from_resource(query_resource)
                    if query_text:
                        logger.info(
                            f"Found saved query '{query_name}' via subscription-scope REST in {subscription_id}"
                        )
                        return query_text

            logger.warning("Saved query not found via subscription-scope REST")
            return None
        except subprocess.TimeoutExpired:
            logger.warning(
                f"Subscription-scope REST lookup timed out after {self.saved_query_lookup_timeout} seconds"
            )
            return None
        except Exception as e:
            logger.warning(f"Saved query lookup via subscription-scope REST failed: {e}")
            return None

    def _get_saved_query_via_resource_cli(self, query_name: str) -> Optional[str]:
        """Retrieve saved query text using `az resource list` fallback."""
        try:
            result = subprocess.run(
                ['az', 'resource', 'list',
                 '--resource-type', 'Microsoft.ResourceGraph/queries',
                 '--query', f"[?name=='{query_name}'].properties.query | [0]",
                 '--output', 'tsv'],
                capture_output=True,
                text=True,
                timeout=self.saved_query_lookup_timeout
            )

            if result.returncode == 0:
                output = result.stdout.strip()
                if output:
                    try:
                        parsed = json.loads(output)
                        if isinstance(parsed, list) and parsed:
                            first = parsed[0]
                            if isinstance(first, str):
                                logger.info(f"Found saved query '{query_name}' via resource-list CLI")
                                return first
                    except Exception:
                        logger.info(f"Found saved query '{query_name}' via resource-list CLI")
                        return output

            if result.returncode != 0:
                logger.warning(f"Resource-list CLI lookup failed: {result.stderr.strip()}")
            else:
                logger.warning("Saved query not found via resource-list CLI")
            return None
        except subprocess.TimeoutExpired:
            logger.warning(
                f"Resource-list CLI lookup timed out after {self.saved_query_lookup_timeout} seconds"
            )
            return None
        except Exception as e:
            logger.warning(f"Saved query lookup via resource-list CLI failed: {e}")
            return None
    
    def export_complete_asset_details(self, output_folder: Optional[str] = None) -> Optional[str]:
        """
        Execute the user's saved 'Complete Asset Details' query and export to CSV.
        
        Retrieves the actual saved private query from Azure Resource Graph Explorer.
        
        Args:
            output_folder: Optional output folder, defaults to Downloads
            
        Returns:
            Path to exported CSV file or None on failure
        """
        logger.info("Starting Complete Asset Details export")
        
        # Get the actual saved query from Azure
        query_text = self._get_saved_query_text("Complete Asset Details")
        
        if not query_text:
            logger.error("Could not retrieve 'Complete Asset Details' saved query from Azure")
            logger.error("Please ensure the query is saved in Azure Resource Graph Explorer")
            return None
        
        logger.info("Executing user's saved query from Azure Resource Graph Explorer")
        data = self._execute_resource_graph_query("Complete Asset Details", query_text)
        
        if data:
            return self.export_to_csv(data, "Complete_Asset_Details", output_folder)
        else:
            logger.error("Failed to execute Complete Asset Details query")
            return None
    
    def export_pre_patch_data(self, output_folder: Optional[str] = None) -> Optional[str]:
        """
        Execute the user's saved 'Pre-Patch Export' query and export to CSV.
        
        Retrieves the actual saved private query from Azure Resource Graph Explorer.
        
        Args:
            output_folder: Optional output folder, defaults to Downloads
            
        Returns:
            Path to exported CSV file or None on failure
        """
        logger.info("Starting Pre-Patch Export")
        
        # Get the actual saved query from Azure
        query_text = self._get_saved_query_text("Pre-Patch Export")
        
        if not query_text:
            logger.error("Could not retrieve 'Pre-Patch Export' saved query from Azure")
            logger.error("Please ensure the query is saved in Azure Resource Graph Explorer")
            return None
        
        logger.info("Executing user's saved query from Azure Resource Graph Explorer")
        data = self._execute_resource_graph_query("Pre-Patch Export", query_text)
        
        if data:
            return self.export_to_csv(data, "Pre_Patch_Export", output_folder)
        else:
            logger.error("Failed to execute Pre-Patch Export query")
            return None
    
    def execute_custom_query(self, query_name: str, query_text: str, output_folder: Optional[str] = None) -> Optional[str]:
        """
        Execute a custom Resource Graph query and export to CSV.
        
        Args:
            query_name: Name for the export file
            query_text: KQL query text to execute
            output_folder: Optional output folder, defaults to Downloads
            
        Returns:
            Path to exported CSV file or None on failure
        """
        logger.info(f"Executing custom query: {query_name}")
        
        data = self._execute_resource_graph_query(query_name, query_text)
        
        if data:
            # Sanitize query name for filename
            safe_name = "".join(c for c in query_name if c.isalnum() or c in (' ', '_', '-')).strip()
            safe_name = safe_name.replace(' ', '_')
            return self.export_to_csv(data, safe_name, output_folder)
        else:
            logger.error(f"Failed to execute custom query: {query_name}")
            return None
