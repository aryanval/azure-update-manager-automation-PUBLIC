"""
Azure Authentication Manager
Handles authentication using Azure CLI credentials.
Supports in-tool device-code login so operators never need to pre-run az login.
"""

import logging
import subprocess
import json
import re
import threading
import uuid
from typing import Optional, Dict, List, Callable

from azure.identity import AzureCliCredential
from azure.core.exceptions import ClientAuthenticationError

logger = logging.getLogger(__name__)


class AzureAuthManager:
    """
    Manages Azure authentication using Azure CLI.

    Per-tenant auth state is tracked so the multi-tenant dashboard knows which
    tenants are ready without re-authenticating.
    """

    def __init__(self, config):
        self.config = config
        self.credential: Optional[AzureCliCredential] = None
        self.current_tenant: Optional[str] = None
        self.current_subscription: Optional[str] = None
        # Per-tenant authentication state
        self._tenant_auth: Dict[str, bool] = {}

    # ------------------------------------------------------------------ #
    # Login state                                                          #
    # ------------------------------------------------------------------ #

    def verify_cli_login(self) -> bool:
        """Return True if az CLI is currently authenticated."""
        try:
            result = subprocess.run(
                ["az", "account", "show"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                info = json.loads(result.stdout)
                logger.info(f"Azure CLI logged in as: {info.get('user', {}).get('name', 'unknown')}")
                return True
            logger.warning("Azure CLI not logged in")
            return False
        except subprocess.TimeoutExpired:
            logger.error("az account show timed out")
            return False
        except FileNotFoundError:
            logger.error("Azure CLI not installed or not on PATH")
            return False
        except Exception as exc:
            logger.error(f"Error checking Azure CLI login: {exc}")
            return False

    def is_tenant_authenticated(self, tenant_name: str) -> bool:
        return self._tenant_auth.get(tenant_name, False)

    # ------------------------------------------------------------------ #
    # In-tool device-code login                                            #
    # ------------------------------------------------------------------ #

    def login_with_device_code(
        self,
        output_callback: Callable[[str], None],
        tenant_id: Optional[str] = None,
        on_complete: Optional[Callable[[bool], None]] = None,
    ) -> subprocess.Popen:
        """
        Spawn 'az login --use-device-code' in a background thread,
        streaming output to output_callback so the GUI can display the URL+code.

        Args:
            output_callback: called with each line of az login output
            tenant_id:       if given, adds --tenant <id> to limit scope
            on_complete:     called with True/False when login finishes

        Returns:
            The Popen process (caller can use it to check/cancel)
        """
        cmd = ["az", "login", "--use-device-code"]
        if tenant_id:
            cmd += ["--tenant", tenant_id]

        logger.info(f"Launching device-code login: {' '.join(cmd)}")

        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        def _stream():
            try:
                for line in proc.stdout:
                    line = line.rstrip()
                    if line:
                        output_callback(line)
                proc.wait()
                success = proc.returncode == 0
                # Invalidate cached credential after new login
                if success:
                    self.credential = None
                    logger.info("Device-code login succeeded; credential refreshed")
                else:
                    logger.warning(f"Device-code login exited with code {proc.returncode}")
                if on_complete:
                    on_complete(success)
            except Exception as exc:
                logger.error(f"Login stream error: {exc}")
                if on_complete:
                    on_complete(False)

        t = threading.Thread(target=_stream, daemon=True)
        t.start()
        return proc

    # ------------------------------------------------------------------ #
    # Subscription / tenant management                                     #
    # ------------------------------------------------------------------ #

    def get_current_subscription(self) -> Optional[Dict]:
        try:
            result = subprocess.run(
                ["az", "account", "show"],
                capture_output=True, text=True, timeout=10,
            )
            return json.loads(result.stdout) if result.returncode == 0 else None
        except Exception as exc:
            logger.error(f"Error getting subscription: {exc}")
            return None

    def get_current_user(self) -> str:
        try:
            result = subprocess.run(
                ["az", "account", "show"],
                capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                return json.loads(result.stdout).get("user", {}).get("name", "unknown")
        except Exception:
            pass
        return "unknown"

    def set_subscription(self, subscription_id: str) -> bool:
        try:
            result = subprocess.run(
                ["az", "account", "set", "--subscription", subscription_id],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0:
                self.current_subscription = subscription_id
                return True
            logger.error(f"Failed to set subscription: {result.stderr.strip()}")
            return False
        except Exception as exc:
            logger.error(f"Error setting subscription: {exc}")
            return False

    def get_credential(self) -> AzureCliCredential:
        """Return (and cache) a shared AzureCliCredential instance."""
        if self.credential is None:
            self.credential = AzureCliCredential()
        return self.credential

    # ------------------------------------------------------------------ #
    # Tenant authentication                                                #
    # ------------------------------------------------------------------ #

    def authenticate_for_tenant(self, tenant_name: str) -> bool:
        """
        Authenticate for a tenant using its first configured subscription.
        Marks tenant as authenticated in per-tenant state dict.
        """
        if not self.verify_cli_login():
            logger.error("Not logged in to Azure CLI")
            return False

        try:
            subscription_id = self.config.get_subscription_id(tenant_name)
        except ValueError as exc:
            logger.error(f"Config error for {tenant_name}: {exc}")
            return False

        if not subscription_id:
            logger.error(f"No subscription configured for tenant: {tenant_name}")
            return False

        if not self.set_subscription(subscription_id):
            return False

        try:
            self.get_credential().get_token("https://management.azure.com/.default")
            self.current_tenant = tenant_name
            self._tenant_auth[tenant_name] = True
            logger.info(f"Authenticated for tenant: {tenant_name}")
            return True
        except ClientAuthenticationError as exc:
            logger.error(f"Authentication failed for {tenant_name}: {exc}")
            self._tenant_auth[tenant_name] = False
            return False
        except Exception as exc:
            logger.error(f"Unexpected auth error for {tenant_name}: {exc}")
            self._tenant_auth[tenant_name] = False
            return False

    def authenticate_for_tenant_subscription(
        self, tenant_name: str, subscription_id: str
    ) -> bool:
        """Authenticate for a specific subscription within a tenant."""
        if not self.verify_cli_login():
            return False
        if not subscription_id:
            logger.error(f"No subscription ID for tenant: {tenant_name}")
            return False
        if not self.set_subscription(subscription_id):
            return False
        try:
            self.get_credential().get_token("https://management.azure.com/.default")
            self.current_tenant = tenant_name
            self._tenant_auth[tenant_name] = True
            return True
        except (ClientAuthenticationError, Exception) as exc:
            logger.error(f"Auth failed for {tenant_name}/{subscription_id}: {exc}")
            self._tenant_auth[tenant_name] = False
            return False

    # ------------------------------------------------------------------ #
    # PIM                                                                  #
    # ------------------------------------------------------------------ #

    def verify_pim_roles(self) -> Dict:
        try:
            result = subprocess.run(
                ["az", "role", "assignment", "list", "--assignee", "@me", "--all"],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0:
                roles = json.loads(result.stdout)
                role_names = [r.get("roleDefinitionName", "") for r in roles]
                return {
                    "total_roles": len(roles),
                    "has_vm_contributor": any("Virtual Machine Contributor" in r for r in role_names),
                    "has_contributor": any("Contributor" in r for r in role_names),
                    "has_owner": any("Owner" in r for r in role_names),
                    "roles": role_names[:10],
                }
            return {"error": result.stderr}
        except Exception as exc:
            return {"error": str(exc)}

    def activate_pim_roles(self, justification: str = "patching") -> Dict:
        """
        Activate all eligible PIM roles for the current login session.
        Uses Azure Resource Manager API via az rest.
        """
        results = {"activated": [], "already_active": [], "failed": [], "not_eligible": []}

        try:
            result = subprocess.run(
                [
                    "az", "rest", "--method", "GET",
                    "--url",
                    "https://management.azure.com/providers/Microsoft.Authorization/"
                    "roleEligibilityScheduleInstances?api-version=2020-10-01&$filter=asTarget()",
                ],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode != 0:
                results["error"] = result.stderr.strip()
                return results

            eligible_roles = json.loads(result.stdout).get("value", [])
            if not eligible_roles:
                return results

            for role in eligible_roles:
                props = role.get("properties", {})
                role_def_id = props.get("roleDefinitionId", "")
                scope = props.get("scope", "")
                principal_id = props.get("principalId", "")
                role_elig_schedule_id = role.get("name", "")
                display_name = (
                    props.get("expandedProperties", {})
                    .get("roleDefinition", {})
                    .get("displayName", role_def_id.split("/")[-1])
                )

                try:
                    # Check if already active
                    check_url = (
                        f"https://management.azure.com{scope}/providers/Microsoft.Authorization/"
                        "roleAssignmentScheduleInstances?api-version=2020-10-01&$filter=asTarget()"
                    )
                    check = subprocess.run(
                        ["az", "rest", "--method", "GET", "--url", check_url],
                        capture_output=True, text=True, timeout=15,
                    )
                    if check.returncode == 0:
                        active = json.loads(check.stdout).get("value", [])
                        already = any(
                            a.get("properties", {}).get("roleDefinitionId") == role_def_id
                            and a.get("properties", {}).get("scope") == scope
                            for a in active
                        )
                        if already:
                            results["already_active"].append(display_name)
                            continue

                    payload = {
                        "properties": {
                            "principalId": principal_id,
                            "roleDefinitionId": role_def_id,
                            "requestType": "SelfActivate",
                            "linkedRoleEligibilityScheduleId": role_elig_schedule_id,
                            "justification": justification,
                            "scheduleInfo": {
                                "expiration": {"type": "AfterDuration", "duration": "PT8H"}
                            },
                        }
                    }
                    activate_url = (
                        f"https://management.azure.com{scope}/providers/Microsoft.Authorization/"
                        f"roleAssignmentScheduleRequests/{uuid.uuid4()}?api-version=2020-10-01"
                    )
                    activate = subprocess.run(
                        [
                            "az", "rest", "--method", "PUT",
                            "--url", activate_url,
                            "--body", json.dumps(payload),
                        ],
                        capture_output=True, text=True, timeout=30,
                    )
                    if activate.returncode == 0:
                        results["activated"].append(display_name)
                    else:
                        results["failed"].append({"role": display_name, "error": activate.stderr.strip()})
                except Exception as exc:
                    results["failed"].append({"role": display_name, "error": str(exc)})

            return results
        except Exception as exc:
            results["error"] = str(exc)
            return results

    def get_tenant_info(self, tenant_name: str) -> Dict:
        try:
            cfg = self.config.get_tenant_config(tenant_name)
            return {
                "name": tenant_name,
                "subscription_id": cfg.get("subscription_id", ""),
                "resource_groups": cfg.get("resource_groups", []) or "All resource groups",
                "authenticated": self._tenant_auth.get(tenant_name, False),
            }
        except Exception as exc:
            return {"error": str(exc)}
