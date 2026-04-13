"""
Pre-Patch VM Health Gate
Checks each VM for conditions that would cause patching to fail before we waste
time trying to patch it.

Checks (all via Azure Run Command so no extra network access needed):
  1. VM agent responsiveness  — lightweight echo; unresponsive agent = Run Command fails
  2. Disk space               — < 10 % free on system drive blocks patches silently
  3. Pending reboot (Windows) — existing pending reboot prevents new patches from applying

Results feed the patch wave executor: VMs that fail the gate are skipped with a clear
reason written to Excel and the local DB rather than failing mid-patch-cycle.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Callable

from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import RunCommandInput

logger = logging.getLogger(__name__)

# Minimum free disk percentage before we flag the VM
DISK_FREE_THRESHOLD_PCT = 10

_AGENT_CHECK_SCRIPT_WINDOWS = ["Write-Output 'agent-ok'"]
_AGENT_CHECK_SCRIPT_LINUX   = ["echo 'agent-ok'"]

_DISK_CHECK_SCRIPT_WINDOWS = [
    "$drive = Get-PSDrive -Name C -PSProvider FileSystem; "
    "$pct = [math]::Round(($drive.Free / ($drive.Used + $drive.Free)) * 100, 1); "
    "Write-Output \"FREE_PCT:$pct\""
]
_DISK_CHECK_SCRIPT_LINUX = [
    "pct=$(df / | awk 'NR==2{gsub(/%/,\"\",$5); print 100-$5}'); echo \"FREE_PCT:$pct\""
]

_REBOOT_CHECK_SCRIPT_WINDOWS = [
    "$pending = $false; "
    "if (Test-Path 'HKLM:\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Component Based Servicing\\RebootPending') { $pending = $true }; "
    "if (Test-Path 'HKLM:\\SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\WindowsUpdate\\Auto Update\\RebootRequired') { $pending = $true }; "
    "if ((Get-ItemProperty 'HKLM:\\SYSTEM\\CurrentControlSet\\Control\\Session Manager' "
    "-Name 'PendingFileRenameOperations' -ErrorAction SilentlyContinue).PendingFileRenameOperations) { $pending = $true }; "
    "Write-Output \"REBOOT_PENDING:$pending\""
]


class HealthGateResult:
    __slots__ = ("vm_name", "passed", "checks", "fail_reasons")

    def __init__(self, vm_name: str):
        self.vm_name = vm_name
        self.passed = True
        self.checks: Dict[str, str] = {}   # check_name -> "pass" / "fail: <reason>" / "skip"
        self.fail_reasons: List[str] = []

    def fail(self, check: str, reason: str):
        self.passed = False
        self.checks[check] = f"fail: {reason}"
        self.fail_reasons.append(f"{check}: {reason}")

    def ok(self, check: str, detail: str = ""):
        self.checks[check] = f"pass{': ' + detail if detail else ''}"

    def skip(self, check: str, reason: str = ""):
        self.checks[check] = f"skip{': ' + reason if reason else ''}"

    def summary(self) -> str:
        if self.passed:
            return "PASS"
        return "FAIL — " + "; ".join(self.fail_reasons)

    def to_dict(self) -> Dict:
        return {
            "vm_name": self.vm_name,
            "passed": self.passed,
            "checks": self.checks,
            "fail_reasons": self.fail_reasons,
            "summary": self.summary(),
        }


class HealthGate:
    """
    Pre-patch health checker.
    All checks use Azure Run Command so they work without direct network access to VMs.
    """

    def __init__(self, auth_manager, error_handler, run_command_timeout: int = 120):
        self.auth_manager = auth_manager
        self.error_handler = error_handler
        self.run_command_timeout = run_command_timeout

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def check_vm(self, vm: Dict, subscription_id: Optional[str] = None) -> HealthGateResult:
        """
        Run all health checks for a single VM.
        Returns a HealthGateResult regardless of outcome.
        """
        vm_name = vm["name"]
        result = HealthGateResult(vm_name)

        sub_id = vm.get("subscription_id") or subscription_id or self.auth_manager.current_subscription
        if not sub_id:
            result.fail("setup", "No subscription ID available")
            return result

        client = ComputeManagementClient(self.auth_manager.get_credential(), sub_id)
        rg = vm["resource_group"]
        os_type = (vm.get("os_type") or "").lower()

        # 1. Agent responsiveness
        self._check_agent(client, rg, vm_name, os_type, result)
        if not result.passed:
            # Agent dead — no point running further checks
            return result

        # 2. Disk space
        self._check_disk(client, rg, vm_name, os_type, result)

        # 3. Pending reboot (Windows only)
        if "windows" in os_type:
            self._check_pending_reboot(client, rg, vm_name, result)
        else:
            result.skip("pending_reboot", "Linux — not checked")

        return result

    def check_batch(
        self,
        vms: List[Dict],
        subscription_id: Optional[str] = None,
        max_workers: int = 15,
        progress_callback: Optional[Callable] = None,
    ) -> Dict[str, HealthGateResult]:
        """
        Run health checks on multiple VMs concurrently.

        progress_callback(vm_name, result, completed, total)
        """
        results: Dict[str, HealthGateResult] = {}
        eligible = [v for v in vms if not v.get("should_skip") and v.get("power_state") == "running"]
        skipped  = [v for v in vms if v.get("should_skip") or v.get("power_state") != "running"]

        for vm in skipped:
            r = HealthGateResult(vm["name"])
            r.skip("all", vm.get("skip_reason") or "not running")
            results[vm["name"]] = r

        if not eligible:
            return results

        completed = 0
        total = len(vms)

        with ThreadPoolExecutor(max_workers=min(max_workers, len(eligible))) as pool:
            future_to_vm = {
                pool.submit(self.check_vm, vm, subscription_id): vm
                for vm in eligible
            }
            for future in as_completed(future_to_vm):
                vm = future_to_vm[future]
                try:
                    r = future.result()
                except Exception as exc:
                    r = HealthGateResult(vm["name"])
                    r.fail("exception", str(exc))

                results[vm["name"]] = r
                completed += 1

                if progress_callback:
                    progress_callback(vm["name"], r, completed, total)

        passed  = sum(1 for r in results.values() if r.passed)
        failed  = sum(1 for r in results.values() if not r.passed and r.fail_reasons)
        logger.info(f"Health gate: {passed} passed, {failed} failed out of {len(results)}")
        return results

    # ------------------------------------------------------------------ #
    # Individual checks                                                    #
    # ------------------------------------------------------------------ #

    def _run_command(
        self,
        client: ComputeManagementClient,
        rg: str,
        vm_name: str,
        command_id: str,
        script: List[str],
    ) -> Optional[str]:
        """Execute a Run Command and return stdout text, or None on failure."""
        try:
            params = RunCommandInput(command_id=command_id, script=script)
            poller = client.virtual_machines.begin_run_command(rg, vm_name, params)
            result = poller.result(timeout=self.run_command_timeout)
            if result and hasattr(result, "value"):
                for item in result.value:
                    if hasattr(item, "message") and item.message:
                        return item.message.strip()
            return ""
        except Exception as exc:
            logger.debug(f"Run Command failed for {vm_name}: {exc}")
            return None

    def _check_agent(
        self,
        client: ComputeManagementClient,
        rg: str,
        vm_name: str,
        os_type: str,
        result: HealthGateResult,
    ):
        is_windows = "windows" in os_type
        cmd_id = "RunPowerShellScript" if is_windows else "RunShellScript"
        script = _AGENT_CHECK_SCRIPT_WINDOWS if is_windows else _AGENT_CHECK_SCRIPT_LINUX

        output = self._run_command(client, rg, vm_name, cmd_id, script)
        if output is None:
            result.fail("agent", "VM agent unresponsive — Run Command failed")
        elif "agent-ok" in output:
            result.ok("agent", "responsive")
        else:
            result.ok("agent", f"responded (output: {output[:40]})")

    def _check_disk(
        self,
        client: ComputeManagementClient,
        rg: str,
        vm_name: str,
        os_type: str,
        result: HealthGateResult,
    ):
        is_windows = "windows" in os_type
        cmd_id = "RunPowerShellScript" if is_windows else "RunShellScript"
        script = _DISK_CHECK_SCRIPT_WINDOWS if is_windows else _DISK_CHECK_SCRIPT_LINUX

        output = self._run_command(client, rg, vm_name, cmd_id, script)
        if output is None:
            result.skip("disk_space", "Run Command unavailable — skipping disk check")
            return

        for line in output.splitlines():
            if "FREE_PCT:" in line:
                try:
                    pct = float(line.split("FREE_PCT:")[-1].strip())
                    if pct < DISK_FREE_THRESHOLD_PCT:
                        result.fail(
                            "disk_space",
                            f"Only {pct:.1f}% free on system drive (threshold {DISK_FREE_THRESHOLD_PCT}%)",
                        )
                    else:
                        result.ok("disk_space", f"{pct:.1f}% free")
                except ValueError:
                    result.skip("disk_space", f"Could not parse: {line}")
                return

        result.skip("disk_space", "Disk check script returned no parseable output")

    def _check_pending_reboot(
        self,
        client: ComputeManagementClient,
        rg: str,
        vm_name: str,
        result: HealthGateResult,
    ):
        output = self._run_command(
            client, rg, vm_name, "RunPowerShellScript", _REBOOT_CHECK_SCRIPT_WINDOWS
        )
        if output is None:
            result.skip("pending_reboot", "Could not check reboot state")
            return

        for line in output.splitlines():
            if "REBOOT_PENDING:" in line:
                value = line.split("REBOOT_PENDING:")[-1].strip().lower()
                if value == "true":
                    result.fail(
                        "pending_reboot",
                        "VM has a pending reboot — patches may not apply correctly",
                    )
                else:
                    result.ok("pending_reboot", "no pending reboot")
                return

        result.skip("pending_reboot", "Reboot check returned no parseable output")
