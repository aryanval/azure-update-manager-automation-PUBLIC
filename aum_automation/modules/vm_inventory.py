"""
VM Inventory Manager
Fresh queries from Azure — never cached.
Handles VMs and AVDs, fetches IPs, and syncs to local SQLite DB.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional

from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.network import NetworkManagementClient
from azure.core.exceptions import ResourceNotFoundError

logger = logging.getLogger(__name__)


class VMInventory:
    """
    Manages VM inventory with fresh Azure queries.

    After each refresh, the full list is synced to the local InventoryDB
    so historical records (including retired VMs) are preserved across cycles.
    """

    def __init__(self, config, auth_manager, error_handler, db=None):
        self.config = config
        self.auth_manager = auth_manager
        self.error_handler = error_handler
        self.db = db  # optional InventoryDB — passed in from GUI

    # ------------------------------------------------------------------ #
    # Client factory                                                       #
    # ------------------------------------------------------------------ #

    def _compute(self, subscription_id: Optional[str] = None) -> ComputeManagementClient:
        sub = subscription_id or self.auth_manager.current_subscription
        return ComputeManagementClient(self.auth_manager.get_credential(), sub)

    def _network(self, subscription_id: Optional[str] = None) -> NetworkManagementClient:
        sub = subscription_id or self.auth_manager.current_subscription
        return NetworkManagementClient(self.auth_manager.get_credential(), sub)

    # ------------------------------------------------------------------ #
    # Main inventory method                                                #
    # ------------------------------------------------------------------ #

    def get_fresh_vm_list(
        self,
        tenant_name: str,
        fetch_ips: bool = True,
        sync_db: bool = True,
        subscription_id: Optional[str] = None,
    ) -> List[Dict]:
        """
        Fetch fresh VM inventory from Azure.

        fetch_ips — concurrently fetches NIC IPs (adds ~5-15s for large tenants)
        sync_db   — writes results to local InventoryDB when db is attached
        """
        logger.info(f"Fetching fresh inventory for {tenant_name}...")

        sub_id = subscription_id or self.auth_manager.current_subscription
        compute = self._compute(sub_id)
        resource_groups = self.config.get_resource_groups(tenant_name)
        all_vms: List[Dict] = []

        try:
            if resource_groups:
                for rg in resource_groups:
                    try:
                        vms = list(compute.virtual_machines.list(rg))
                        all_vms.extend(self._process_vms(vms, rg))
                    except ResourceNotFoundError:
                        logger.warning(f"Resource group not found: {rg}")
                    except Exception as exc:
                        logger.error(f"Error listing VMs in {rg}: {exc}")
            else:
                vms = list(compute.virtual_machines.list_all())
                all_vms.extend(self._process_vms(vms))

            # Stamp subscription so per-thread clients use the right sub
            for vm in all_vms:
                if not vm.get("subscription_id"):
                    vm["subscription_id"] = sub_id

            if fetch_ips and all_vms:
                self._enrich_with_ips(all_vms, sub_id)

            if sync_db and self.db:
                try:
                    self.db.sync_tenant_inventory(tenant_name, all_vms, sub_id)
                except Exception as exc:
                    logger.warning(f"DB sync failed (non-fatal): {exc}")

            logger.info(f"Inventory complete: {len(all_vms)} VMs for {tenant_name}")
            return all_vms

        except Exception as exc:
            logger.error(f"Failed to get VM inventory for {tenant_name}: {exc}")
            raise

    # ------------------------------------------------------------------ #
    # IP enrichment                                                        #
    # ------------------------------------------------------------------ #

    def _enrich_with_ips(self, vms: List[Dict], subscription_id: str, max_workers: int = 20):
        """
        Concurrently fetch private/public IPs for all VMs via their NIC.
        Updates vms in-place. Errors are silently skipped (IP stays None).
        """
        network = self._network(subscription_id)

        def _fetch_ip(vm: Dict):
            vm_id = vm.get("id", "")
            try:
                # Parse resource group from VM id
                parts = vm_id.split("/")
                if len(parts) < 9:
                    return
                rg = parts[4]
                compute = self._compute(subscription_id)
                vm_obj = compute.virtual_machines.get(rg, vm["name"])
                if not (vm_obj.network_profile and vm_obj.network_profile.network_interfaces):
                    return
                nic_id = vm_obj.network_profile.network_interfaces[0].id
                nic_parts = nic_id.split("/")
                nic_rg   = nic_parts[4]
                nic_name = nic_parts[-1]
                nic = network.network_interfaces.get(nic_rg, nic_name)
                if not nic.ip_configurations:
                    return
                ipc = nic.ip_configurations[0]
                vm["private_ip"] = ipc.private_ip_address
                if ipc.public_ip_address:
                    pip_id   = ipc.public_ip_address.id
                    pip_parts = pip_id.split("/")
                    pip_rg   = pip_parts[4]
                    pip_name = pip_parts[-1]
                    pip = network.public_ip_addresses.get(pip_rg, pip_name)
                    vm["public_ip"] = pip.ip_address
            except Exception as exc:
                logger.debug(f"IP fetch failed for {vm.get('name')}: {exc}")

        with ThreadPoolExecutor(max_workers=min(max_workers, len(vms))) as pool:
            futures = [pool.submit(_fetch_ip, vm) for vm in vms]
            for f in as_completed(futures):
                pass  # errors already logged inside _fetch_ip

    # ------------------------------------------------------------------ #
    # VM processing                                                        #
    # ------------------------------------------------------------------ #

    def _process_vms(self, vms, resource_group: Optional[str] = None) -> List[Dict]:
        processed = []
        for vm in vms:
            try:
                info = self._extract_vm_info(vm, resource_group)
                skip, reason = self.error_handler.is_vm_skippable_by_tags(info.get("tags", {}))
                info["should_skip"] = skip
                info["skip_reason"] = reason
                processed.append(info)
            except Exception as exc:
                logger.error(f"Error processing VM: {exc}")
        return processed

    def _extract_vm_info(self, vm, resource_group: Optional[str] = None) -> Dict:
        if not resource_group and vm.id:
            parts = vm.id.split("/")
            try:
                resource_group = parts[parts.index("resourceGroups") + 1]
            except (ValueError, IndexError):
                resource_group = "unknown"

        os_type = "Unknown"
        if vm.storage_profile and vm.storage_profile.os_disk:
            os_type = vm.storage_profile.os_disk.os_type or "Unknown"

        vm_size = vm.hardware_profile.vm_size if vm.hardware_profile else "Unknown"

        is_avd = False
        if vm.tags:
            is_avd = any(
                k.lower() in ("cm-resource-parent", "avd", "wvd", "sessionhost")
                for k in vm.tags
            )

        return {
            "name": vm.name,
            "id": vm.id,
            "resource_group": resource_group,
            "location": vm.location,
            "os_type": os_type,
            "vm_size": vm_size,
            "tags": vm.tags or {},
            "provisioning_state": vm.provisioning_state,
            "is_avd": is_avd,
            "type": "AVD" if is_avd else "Server",
            "private_ip": None,
            "public_ip": None,
            "subscription_id": None,  # filled in by get_fresh_vm_list
        }

    # ------------------------------------------------------------------ #
    # Power state                                                          #
    # ------------------------------------------------------------------ #

    def get_vm_power_state(
        self, resource_group: str, vm_name: str, subscription_id: Optional[str] = None
    ) -> str:
        compute = self._compute(subscription_id)
        try:
            iv = compute.virtual_machines.instance_view(resource_group, vm_name)
            for status in iv.statuses:
                if status.code.startswith("PowerState/"):
                    return status.code.split("/")[-1]
            return "unknown"
        except ResourceNotFoundError:
            return "not_found"
        except Exception as exc:
            err = self.error_handler.handle_error(exc, vm_name, "get_power_state")
            logger.error(f"Error getting power state: {err['message']}")
            return "error"

    def get_vm_details(
        self, resource_group: str, vm_name: str, subscription_id: Optional[str] = None
    ) -> Optional[Dict]:
        compute = self._compute(subscription_id)
        try:
            vm = compute.virtual_machines.get(resource_group, vm_name)
            info = self._extract_vm_info(vm, resource_group)
            info["power_state"] = self.get_vm_power_state(resource_group, vm_name, subscription_id)
            return info
        except ResourceNotFoundError:
            return None
        except Exception as exc:
            err = self.error_handler.handle_error(exc, vm_name, "get_vm_details")
            logger.error(f"Error getting VM details: {err['message']}")
            return None

    def categorize_vms(self, vms: List[Dict]) -> Dict[str, List[Dict]]:
        servers = [v for v in vms if not v.get("is_avd")]
        avds    = [v for v in vms if v.get("is_avd")]
        return {"servers": servers, "avds": avds}

    def refresh_vm_states(self, vms: List[Dict], subscription_id: Optional[str] = None) -> List[Dict]:
        """Refresh power states for a list of VMs (sequential — call after inventory)."""
        for vm in vms:
            vm["power_state"] = self.get_vm_power_state(
                vm["resource_group"], vm["name"], vm.get("subscription_id") or subscription_id
            )
        return vms
