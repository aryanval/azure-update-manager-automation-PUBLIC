"""
Integration test script for AUM Automation Tool.
Validates module imports, config loading, and DB initialisation
without requiring Azure credentials.

Run from the aum_automation/ directory:
    python3 test_integration.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

PASS = "\033[92m  PASS\033[0m"
FAIL = "\033[91m  FAIL\033[0m"
results = []


def check(name, fn):
    try:
        fn()
        print(f"{PASS}  {name}")
        results.append(True)
    except Exception as exc:
        print(f"{FAIL}  {name}: {exc}")
        results.append(False)


# Module imports
check("Import: config",           lambda: __import__("modules.config",           fromlist=["get_config"]))
check("Import: auth_manager",     lambda: __import__("modules.auth_manager",     fromlist=["AzureAuthManager"]))
check("Import: aum_manager",      lambda: __import__("modules.aum_manager",      fromlist=["AUMManager"]))
check("Import: patch_executor",   lambda: __import__("modules.patch_executor",   fromlist=["PatchExecutor"]))
check("Import: vm_inventory",     lambda: __import__("modules.vm_inventory",     fromlist=["VMInventory"]))
check("Import: vm_power_manager", lambda: __import__("modules.vm_power_manager", fromlist=["VMPowerManager"]))
check("Import: health_gate",      lambda: __import__("modules.health_gate",      fromlist=["HealthGate"]))
check("Import: db_manager",       lambda: __import__("modules.db_manager",       fromlist=["InventoryDB"]))
check("Import: excel_tracker",    lambda: __import__("modules.excel_tracker",    fromlist=["ExcelTracker"]))
check("Import: error_handler",    lambda: __import__("modules.error_handler",    fromlist=["ErrorHandler"]))
check("Import: notifications",    lambda: __import__("modules.notifications",    fromlist=["NotificationManager"]))


def _load_config():
    from modules.config import get_config
    cfg = get_config()
    tenants = cfg.get_tenant_list()
    assert isinstance(tenants, list)
    assert len(tenants) > 0, "configure at least one tenant in config.yaml"

check("Config: loads config.yaml and finds tenants", _load_config)


def _db_roundtrip():
    import tempfile
    from pathlib import Path
    from modules.db_manager import InventoryDB, new_run_id
    with tempfile.TemporaryDirectory() as tmpdir:
        db = InventoryDB(Path(tmpdir) / "test.db")
        vms = [{"name": "vm-01", "resource_group": "rg-test", "os_type": "Windows",
                "type": "Server", "location": "eastus", "vm_size": "Standard_D2s_v3",
                "private_ip": "10.0.0.4", "tags": {"PatchWave": "1"}}]
        res = db.sync_tenant_inventory("Test", vms, "sub-000")
        assert res["upserted"] == 1
        db.record_patch_run(run_id=new_run_id(), tenant="Test", vm_name="vm-01",
                            resource_group="rg-test", status="success", patches_applied=5)
        history = db.get_patch_history("Test", "vm-01")
        assert history[0]["status"] == "success"

check("DB: upsert VM and record patch run", _db_roundtrip)


def _error_handler():
    from modules.config import get_config
    from modules.error_handler import ErrorHandler
    cfg = get_config()
    handler = ErrorHandler(cfg)
    info = handler.handle_error(Exception("vm busy"), "vm-01", "test_op")
    assert "message" in info

check("ErrorHandler: categorises error", _error_handler)


print()
passed = sum(results)
total  = len(results)
print(f"Results: {passed}/{total} checks passed")
sys.exit(0 if passed == total else 1)
