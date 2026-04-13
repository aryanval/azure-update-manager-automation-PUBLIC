"""
Patch Executor
Handles patching for VMs not supported by AUM via Azure Run Command API.
Concurrent execution with automatic retry on transient failures.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional

from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import RunCommandInput

logger = logging.getLogger(__name__)

_RETRY_BACKOFF = [30, 60]   # seconds between retry attempts


class PatchExecutor:
    """
    Run-Command-based patching for VMs unsupported by Azure Update Manager.

    All operations:
    - Retry up to max_retries (default 2) on transient failures
    - Fan out concurrently across VMs via ThreadPoolExecutor
    - Never touch SQL Servers
    """

    def __init__(self, config, auth_manager, aum_manager, error_handler):
        self.config = config
        self.auth_manager = auth_manager
        self.aum_manager = aum_manager
        self.error_handler = error_handler
        self.user_exclusions: List[str] = []

    # ------------------------------------------------------------------ #
    # Helpers                                                              #
    # ------------------------------------------------------------------ #

    def _compute(self, subscription_id: Optional[str] = None) -> ComputeManagementClient:
        sub = subscription_id or self.auth_manager.current_subscription
        return ComputeManagementClient(self.auth_manager.get_credential(), sub)

    def set_user_exclusions(self, exclusions: List[str]):
        self.user_exclusions = exclusions

    def should_exclude_patch(self, patch_name: str):
        ok, reason = self.aum_manager.should_exclude_patch(patch_name)
        if ok:
            return True, reason
        for pattern in self.user_exclusions:
            if pattern.lower() in patch_name.lower():
                return True, f"User excluded: {pattern}"
        return False, None

    def filter_patches(self, patches: List[str]) -> Dict:
        allowed, excluded = [], {}
        for p in patches:
            skip, reason = self.should_exclude_patch(p)
            (excluded.__setitem__(p, reason) if skip else allowed.append(p))
        return {"allowed": allowed, "excluded": excluded}

    # ------------------------------------------------------------------ #
    # Run Command with retry                                               #
    # ------------------------------------------------------------------ #

    def _run_command_with_retry(
        self,
        resource_group: str,
        vm_name: str,
        command_id: str,
        script: List[str],
        subscription_id: Optional[str] = None,
        max_retries: int = 2,
        timeout: int = 1800,
    ) -> Dict:
        """Execute a Run Command script with automatic retry on transient errors."""
        last_error = None

        for attempt in range(max_retries + 1):
            if attempt > 0:
                backoff = _RETRY_BACKOFF[min(attempt - 1, len(_RETRY_BACKOFF) - 1)]
                logger.info(f"Retrying Run Command on {vm_name} in {backoff}s (attempt {attempt + 1})")
                time.sleep(backoff)

            try:
                client = self._compute(subscription_id)
                params = RunCommandInput(command_id=command_id, script=script)
                poller = client.virtual_machines.begin_run_command(resource_group, vm_name, params)
                result = poller.result(timeout=timeout)

                output_parts = []
                if result and hasattr(result, "value"):
                    for item in result.value:
                        if hasattr(item, "message") and item.message:
                            output_parts.append(item.message)

                return {
                    "status": "success",
                    "vm_name": vm_name,
                    "output": "\n".join(output_parts),
                    "method": "run_command",
                    "attempt": attempt + 1,
                }

            except Exception as exc:
                err_info = self.error_handler.handle_error(exc, vm_name, "run_command", attempt + 1)
                last_error = err_info["message"]
                action = err_info.get("action", "skip")

                if attempt < max_retries and action in ("retry", "RETRY"):
                    logger.warning(f"Retryable error on {vm_name} attempt {attempt + 1}: {last_error}")
                    continue

                logger.error(f"Run Command failed for {vm_name} after {attempt + 1} attempt(s): {last_error}")
                return {
                    "status": "failed",
                    "vm_name": vm_name,
                    "error": last_error,
                    "attempt": attempt + 1,
                }

        return {"status": "failed", "vm_name": vm_name, "error": last_error, "attempt": max_retries + 1}

    # ------------------------------------------------------------------ #
    # OS-specific patching                                                 #
    # ------------------------------------------------------------------ #

    _LINUX_SCRIPT = [
        "#!/bin/bash\n"
        "set -e\n"
        "echo 'Starting Linux updates...'\n"
        "sudo apt-get update -q\n"
        "sudo DEBIAN_FRONTEND=noninteractive apt-get upgrade -y\n"
        "echo 'Linux updates completed'"
    ]

    _WINDOWS_SCRIPT = [
        "Write-Output 'Starting Windows Update...'\n"
        "if (-not (Get-Module -ListAvailable -Name PSWindowsUpdate)) {\n"
        "    Install-PackageProvider -Name NuGet -Force -Scope CurrentUser | Out-Null\n"
        "    Install-Module -Name PSWindowsUpdate -Force -Scope CurrentUser | Out-Null\n"
        "}\n"
        "Import-Module PSWindowsUpdate\n"
        "Get-WindowsUpdate -NotCategory 'SQL Server' -Install -AcceptAll -AutoReboot -ErrorAction Stop\n"
        "Write-Output 'Windows updates completed'"
    ]

    def execute_linux_patches(
        self,
        resource_group: str,
        vm_name: str,
        subscription_id: Optional[str] = None,
        dry_run: bool = False,
        max_retries: int = 2,
    ) -> Dict:
        if dry_run:
            return {"status": "dry_run", "vm_name": vm_name}
        return self._run_command_with_retry(
            resource_group, vm_name, "RunShellScript", self._LINUX_SCRIPT,
            subscription_id=subscription_id, max_retries=max_retries,
        )

    def execute_windows_patches(
        self,
        resource_group: str,
        vm_name: str,
        subscription_id: Optional[str] = None,
        is_sql_server: bool = False,
        dry_run: bool = False,
        max_retries: int = 2,
    ) -> Dict:
        if is_sql_server:
            return {"status": "skipped", "vm_name": vm_name, "reason": "SQL Server — manual patching only"}
        if dry_run:
            return {"status": "dry_run", "vm_name": vm_name}
        return self._run_command_with_retry(
            resource_group, vm_name, "RunPowerShellScript", self._WINDOWS_SCRIPT,
            subscription_id=subscription_id, max_retries=max_retries,
        )

    def execute_unsupported_vm_patches(
        self,
        vm: Dict,
        dry_run: bool = False,
        max_retries: int = 2,
    ) -> Dict:
        """Detect OS and route to the correct patching method."""
        vm_name = vm["name"]
        rg = vm["resource_group"]
        sub_id = vm.get("subscription_id")
        os_type = (vm.get("os_type") or "").lower()

        is_sql = "sql" in vm_name.lower() or any(
            "sql" in k.lower() for k in (vm.get("tags") or {})
        )

        if "linux" in os_type or "ubuntu" in os_type or "debian" in os_type:
            return self.execute_linux_patches(rg, vm_name, sub_id, dry_run, max_retries)
        elif "windows" in os_type:
            return self.execute_windows_patches(rg, vm_name, sub_id, is_sql, dry_run, max_retries)
        else:
            return {"status": "failed", "vm_name": vm_name, "reason": f"Unknown OS: {os_type}"}

    # ------------------------------------------------------------------ #
    # Concurrent batch execution                                           #
    # ------------------------------------------------------------------ #

    def execute_patches_concurrent(
        self,
        vms: List[Dict],
        dry_run: bool = False,
        max_workers: int = 15,
        max_retries: int = 2,
        progress_callback: Optional[Callable] = None,
        cancel_flag: Optional[list] = None,
    ) -> Dict[str, Dict]:
        """
        Execute Run-Command patching concurrently on multiple VMs.

        progress_callback(vm_name, result, completed, total)
        cancel_flag: list; set [0]=True to abort pending futures.
        """
        results: Dict[str, Dict] = {}
        skipped = [v for v in vms if v.get("should_skip") or v.get("power_state") != "running"]
        eligible = [v for v in vms if not v.get("should_skip") and v.get("power_state") == "running"]

        for vm in skipped:
            results[vm["name"]] = {
                "status": "skipped",
                "reason": vm.get("skip_reason") or "not running",
            }

        if not eligible:
            return results

        completed = 0
        total = len(vms)

        def _patch_one(vm: Dict):
            return vm["name"], self.execute_unsupported_vm_patches(vm, dry_run, max_retries)

        with ThreadPoolExecutor(max_workers=min(max_workers, len(eligible))) as pool:
            future_to_vm = {pool.submit(_patch_one, vm): vm for vm in eligible}
            for future in as_completed(future_to_vm):
                if cancel_flag and cancel_flag[0]:
                    pool.shutdown(wait=False, cancel_futures=True)
                    break
                vm_name, result = future.result()
                results[vm_name] = result
                completed += 1
                if progress_callback:
                    progress_callback(vm_name, result, completed, total)

        return results

    # ------------------------------------------------------------------ #
    # AUM-based batch (keep for compatibility)                             #
    # ------------------------------------------------------------------ #

    def execute_patches_via_aum(self, vms: List[Dict], dry_run: bool = False) -> Dict[str, Dict]:
        """Thin wrapper — AUM patching is now handled directly via aum_manager."""
        results = {}
        for vm in vms:
            if vm.get("should_skip"):
                results[vm["name"]] = {"status": "skipped", "reason": vm.get("skip_reason")}
                continue
            if dry_run:
                results[vm["name"]] = {"status": "dry_run"}
                continue
            op_id = self.aum_manager.install_patches_and_wait(
                vm["resource_group"], vm["name"],
                vm.get("subscription_id") or self.auth_manager.current_subscription,
            )
            results[vm["name"]] = op_id if isinstance(op_id, dict) else {"status": "started", "id": op_id}
        return results
