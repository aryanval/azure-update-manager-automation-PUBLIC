"""
Azure Update Manager (AUM) Client
Concurrent assessment and patch installation with retry logic.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Callable, Dict, List, Optional

from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import VirtualMachineInstallPatchesParameters
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

logger = logging.getLogger(__name__)

# How long to wait between retry attempts (seconds)
_RETRY_BACKOFF = [30, 60]  # attempt 1 waits 30s, attempt 2 waits 60s


class AUMManager:
    """
    Azure Update Manager client.

    Key behaviours:
    - Assessment and patching fan out concurrently across all VMs via ThreadPoolExecutor
    - install_patches_and_wait() blocks until AUM reports completion
    - Auto-retries transient failures up to max_retries (default 2) before marking failed
    """

    def __init__(self, config, auth_manager, error_handler):
        self.config = config
        self.auth_manager = auth_manager
        self.error_handler = error_handler
        self.polling_config = config.get_polling_config()

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _get_compute_client(self, subscription_id: Optional[str] = None) -> ComputeManagementClient:
        sub = subscription_id or self.auth_manager.current_subscription
        return ComputeManagementClient(self.auth_manager.get_credential(), sub)

    # ------------------------------------------------------------------ #
    # Assessment                                                           #
    # ------------------------------------------------------------------ #

    def assess_patches(self, resource_group: str, vm_name: str,
                       subscription_id: Optional[str] = None) -> Optional[Dict]:
        """Assess a single VM (blocking). Returns result dict or None on error."""
        client = self._get_compute_client(subscription_id)
        try:
            logger.info(f"Assessing {vm_name}")
            poller = client.virtual_machines.begin_assess_patches(resource_group, vm_name)
            result = poller.result(timeout=600)
            return _parse_assessment(vm_name, resource_group, result)
        except Exception as exc:
            err = self.error_handler.handle_error(exc, vm_name, "assess_patches")
            logger.error(f"Assessment failed for {vm_name}: {err['message']}")
            return None

    def assess_patches_concurrent(
        self,
        vms: List[Dict],
        max_workers: int = 20,
        progress_callback: Optional[Callable] = None,
        cancel_flag: Optional[list] = None,
    ) -> Dict[str, Dict]:
        """
        Concurrently assess all VMs.

        All VMs are assessed simultaneously — Azure does the actual work,
        we just fan out the API calls and collect results.

        progress_callback(vm_name, result_dict, completed_count, total_count)
        cancel_flag: mutable list; set cancel_flag[0]=True to abort pending futures.
        """
        results: Dict[str, Dict] = {}
        eligible = [v for v in vms if not v.get("should_skip")]
        skipped  = [v for v in vms if v.get("should_skip")]

        for vm in skipped:
            results[vm["name"]] = {"status": "skipped", "reason": vm.get("skip_reason")}

        if not eligible:
            return results

        completed = 0
        total = len(vms)

        def _assess_one(vm: Dict):
            sub_id = vm.get("subscription_id") or self.auth_manager.current_subscription
            client = self._get_compute_client(sub_id)
            try:
                poller = client.virtual_machines.begin_assess_patches(
                    vm["resource_group"], vm["name"]
                )
                result = poller.result(timeout=600)
                return vm["name"], _parse_assessment(vm["name"], vm["resource_group"], result)
            except Exception as exc:
                err = self.error_handler.handle_error(exc, vm["name"], "assess_patches")
                return vm["name"], {"status": "failed", "error": err["message"]}

        with ThreadPoolExecutor(max_workers=min(max_workers, len(eligible))) as pool:
            future_to_vm = {pool.submit(_assess_one, vm): vm for vm in eligible}
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
    # Patching                                                             #
    # ------------------------------------------------------------------ #

    def install_patches_and_wait(
        self,
        resource_group: str,
        vm_name: str,
        subscription_id: Optional[str] = None,
        max_retries: int = 2,
        timeout: int = 7200,
    ) -> Dict:
        """
        Install patches on a VM and block until AUM reports completion.
        Retries up to max_retries times on transient/out-of-sync failures.

        Returns a result dict with at minimum:
            status: 'success' | 'failed' | 'skipped'
            vm_name, patches_installed, patches_failed, attempt, raw_status
        """
        last_error = None

        for attempt in range(max_retries + 1):
            if attempt > 0:
                backoff = _RETRY_BACKOFF[min(attempt - 1, len(_RETRY_BACKOFF) - 1)]
                logger.info(f"Retrying {vm_name} in {backoff}s (attempt {attempt + 1}/{max_retries + 1})")
                time.sleep(backoff)

            try:
                client = self._get_compute_client(subscription_id)
                params = VirtualMachineInstallPatchesParameters(
                    maximum_duration="PT2H",
                    reboot_setting="IfRequired",
                )
                logger.info(f"Starting patch installation on {vm_name} (attempt {attempt + 1})")
                poller = client.virtual_machines.begin_install_patches(
                    resource_group, vm_name, install_patches_input=params
                )
                result = poller.result(timeout=timeout)

                raw_status = getattr(result, "status", "Unknown")
                installed = getattr(result, "installed_patch_count", 0) or 0
                failed_count = getattr(result, "failed_patch_count", 0) or 0
                excluded = getattr(result, "excluded_patch_count", 0) or 0
                not_selected = getattr(result, "not_selected_patch_count", 0) or 0
                pending = getattr(result, "pending_reboot_count", 0) or 0

                success = raw_status in ("Succeeded", "CompletedWithWarnings")

                logger.info(
                    f"{'✓' if success else '✗'} {vm_name}: "
                    f"installed={installed}, failed={failed_count}, "
                    f"status={raw_status}, attempt={attempt + 1}"
                )

                return {
                    "status": "success" if success else "failed",
                    "vm_name": vm_name,
                    "patches_installed": installed,
                    "patches_failed": failed_count,
                    "patches_excluded": excluded,
                    "patches_not_selected": not_selected,
                    "pending_reboot": pending,
                    "attempt": attempt + 1,
                    "raw_status": raw_status,
                }

            except Exception as exc:
                err_info = self.error_handler.handle_error(exc, vm_name, "install_patches", attempt + 1)
                last_error = err_info["message"]
                action = err_info.get("action", "skip")

                if attempt < max_retries and action in ("retry", "RETRY"):
                    logger.warning(f"Retryable error on {vm_name} attempt {attempt + 1}: {last_error}")
                    continue

                # Non-retryable or retries exhausted
                logger.error(f"Patch failed for {vm_name} after {attempt + 1} attempt(s): {last_error}")
                return {
                    "status": "failed",
                    "vm_name": vm_name,
                    "patches_installed": 0,
                    "patches_failed": 0,
                    "attempt": attempt + 1,
                    "error": last_error,
                }

        # Should not reach here
        return {"status": "failed", "vm_name": vm_name, "error": last_error, "attempt": max_retries + 1}

    def install_patches_concurrent(
        self,
        vms: List[Dict],
        max_workers: int = 15,
        max_retries: int = 2,
        progress_callback: Optional[Callable] = None,
        cancel_flag: Optional[list] = None,
        wave: Optional[int] = None,
    ) -> Dict[str, Dict]:
        """
        Concurrently install patches on multiple VMs with retry.

        If wave is specified, only processes VMs in that wave (PatchWave tag).
        progress_callback(vm_name, result, completed, total)
        """
        if wave is not None:
            target_vms = [v for v in vms if _get_vm_wave(v) == wave and not v.get("should_skip")]
            skipped_vms = [v for v in vms if _get_vm_wave(v) != wave or v.get("should_skip")]
        else:
            target_vms = [v for v in vms if not v.get("should_skip")]
            skipped_vms = [v for v in vms if v.get("should_skip")]

        results: Dict[str, Dict] = {}
        for vm in skipped_vms:
            results[vm["name"]] = {"status": "skipped", "reason": vm.get("skip_reason", "not in this wave")}

        if not target_vms:
            return results

        completed = 0
        total = len(target_vms) + len(skipped_vms)

        def _patch_one(vm: Dict):
            sub_id = vm.get("subscription_id") or self.auth_manager.current_subscription
            return vm["name"], self.install_patches_and_wait(
                vm["resource_group"], vm["name"], sub_id, max_retries=max_retries
            )

        with ThreadPoolExecutor(max_workers=min(max_workers, len(target_vms))) as pool:
            future_to_vm = {pool.submit(_patch_one, vm): vm for vm in target_vms}
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
    # Wave-ordered patching                                                #
    # ------------------------------------------------------------------ #

    def install_patches_by_wave(
        self,
        vms: List[Dict],
        max_workers: int = 15,
        max_retries: int = 2,
        failure_threshold_pct: float = 30.0,
        wave_progress_callback: Optional[Callable] = None,
        vm_progress_callback: Optional[Callable] = None,
        cancel_flag: Optional[list] = None,
    ) -> Dict[str, Dict]:
        """
        Execute patches in wave order (1 → 2 → 3).

        After each wave, check: if failure rate > failure_threshold_pct, abort remaining waves.
        wave_progress_callback(wave, status_str)  — called when a wave starts/ends
        vm_progress_callback(vm_name, result, completed, total)
        """
        all_results: Dict[str, Dict] = {}

        waves = sorted({_get_vm_wave(v) for v in vms if not v.get("should_skip")})
        if not waves:
            # Everything skipped
            for vm in vms:
                all_results[vm["name"]] = {"status": "skipped", "reason": vm.get("skip_reason")}
            return all_results

        for wave_num in waves:
            if cancel_flag and cancel_flag[0]:
                break

            wave_vms = [v for v in vms if _get_vm_wave(v) == wave_num and not v.get("should_skip")]
            if not wave_vms:
                continue

            if wave_progress_callback:
                wave_progress_callback(wave_num, f"Starting wave {wave_num} ({len(wave_vms)} VMs)")

            wave_results = self.install_patches_concurrent(
                wave_vms,
                max_workers=max_workers,
                max_retries=max_retries,
                progress_callback=vm_progress_callback,
                cancel_flag=cancel_flag,
            )
            all_results.update(wave_results)

            # Failure gate
            failures = sum(1 for r in wave_results.values() if r.get("status") == "failed")
            total = len(wave_results)
            failure_pct = (failures / total * 100) if total else 0

            if wave_progress_callback:
                wave_progress_callback(
                    wave_num,
                    f"Wave {wave_num} complete: {total - failures}/{total} succeeded "
                    f"({failure_pct:.0f}% failure rate)",
                )

            if failure_pct > failure_threshold_pct and wave_num < max(waves):
                msg = (
                    f"Wave {wave_num} failure rate {failure_pct:.0f}% exceeds "
                    f"threshold {failure_threshold_pct:.0f}% — halting remaining waves"
                )
                logger.warning(msg)
                if wave_progress_callback:
                    wave_progress_callback(wave_num, f"HALTED: {msg}")

                # Mark remaining-wave VMs as aborted
                remaining_waves = [w for w in waves if w > wave_num]
                for rw in remaining_waves:
                    for vm in vms:
                        if _get_vm_wave(vm) == rw and not vm.get("should_skip"):
                            all_results[vm["name"]] = {
                                "status": "aborted",
                                "reason": f"Wave {wave_num} failure threshold exceeded",
                            }
                break

        # Mark explicitly skipped VMs
        for vm in vms:
            if vm.get("should_skip") and vm["name"] not in all_results:
                all_results[vm["name"]] = {"status": "skipped", "reason": vm.get("skip_reason")}

        return all_results

    # ------------------------------------------------------------------ #
    # Remaining legacy helpers                                             #
    # ------------------------------------------------------------------ #

    def get_last_assessment_time(self, resource_group: str, vm_name: str) -> Optional[str]:
        try:
            client = self._get_compute_client()
            iv = client.virtual_machines.instance_view(resource_group, vm_name)
            if hasattr(iv, "patch_status") and iv.patch_status:
                ps = iv.patch_status
                if hasattr(ps, "available_patch_summary") and ps.available_patch_summary:
                    s = ps.available_patch_summary
                    if hasattr(s, "last_modified_time") and s.last_modified_time:
                        return s.last_modified_time.strftime("%Y-%m-%d %H:%M:%S")
        except Exception as exc:
            logger.debug(f"Could not get last assessment time for {vm_name}: {exc}")
        return None

    def filter_sql_patches(self, patches: List[str]):
        sql_kw = ["sql", "sqlserver", "sql server", "database engine"]
        excluded, allowed = [], []
        for p in patches:
            (excluded if any(k in p.lower() for k in sql_kw) else allowed).append(p)
        return allowed, excluded

    def should_exclude_patch(self, patch_name: str):
        for pattern in self.config.get_always_exclude_patches():
            if pattern.lower().replace("*", "") in patch_name.lower():
                return True, f"Matches exclusion pattern: {pattern}"
        return False, None

    def get_always_exclude_patterns(self):
        return self.config.get_always_exclude_patches()


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _parse_assessment(vm_name: str, resource_group: str, result) -> Dict:
    info: Dict = {
        "vm_name": vm_name,
        "resource_group": resource_group,
        "assessment_time": datetime.now().isoformat(),
        "status": "completed",
        "available_patch_count": 0,
        "critical_count": 0,
        "security_count": 0,
        "other_count": 0,
    }
    if hasattr(result, "available_patch_count_by_classification"):
        cl = result.available_patch_count_by_classification
        info["critical_count"]   = getattr(cl, "critical", 0) or 0
        info["security_count"]   = getattr(cl, "security", 0) or 0
        info["other_count"]      = getattr(cl, "other",    0) or 0
        info["available_patch_count"] = (
            info["critical_count"] + info["security_count"] + info["other_count"]
        )
    return info


def _get_vm_wave(vm: Dict) -> int:
    tags = vm.get("tags") or {}
    for k in tags:
        if k.lower() in ("patchwave", "patch_wave", "wave"):
            try:
                return max(1, min(3, int(tags[k])))
            except (ValueError, TypeError):
                pass
    return 1
