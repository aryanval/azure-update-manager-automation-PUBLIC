"""
Local SQLite Inventory Database
Stores full AUM inventory per tenant including IPs, new and retired machines.

SECURITY: This database is LOCAL ONLY.
- Never transmitted to any external service
- Path is gitignored (state/aum_inventory.db)
- Write-only from automation standpoint; human operator reads directly via SQLite client
- Contains PII (VM names, IPs) — treat as internal confidential data
"""

import sqlite3
import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from contextlib import contextmanager
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

# DB lives in state/ which is already gitignored
_DEFAULT_DB_PATH = Path(__file__).parent.parent / "state" / "aum_inventory.db"


class InventoryDB:
    """
    Local SQLite database for AUM VM inventory and patch history.

    Schema intent:
    - vms: every machine ever seen, across all tenants; soft-deleted when gone
    - patch_runs: one row per VM per patch cycle attempt (with retry_count)

    Usage:
        db = InventoryDB()
        db.sync_tenant_inventory("ClientA", vms_list, subscription_id="abc-123")
        db.record_patch_run(run_id, tenant="ClientA", vm_name="vm-prod-01", ...)
    """

    def __init__(self, db_path: Path = _DEFAULT_DB_PATH):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        logger.info(f"InventoryDB initialised at {self.db_path}")

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(str(self.db_path), timeout=15)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS vms (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    tenant              TEXT    NOT NULL,
                    subscription_id     TEXT,
                    resource_group      TEXT    NOT NULL,
                    name                TEXT    NOT NULL,
                    location            TEXT,
                    os_type             TEXT,
                    vm_type             TEXT    DEFAULT 'Server',
                    vm_size             TEXT,
                    private_ip          TEXT,
                    public_ip           TEXT,
                    tags                TEXT,
                    patch_wave          INTEGER DEFAULT 1,
                    first_seen          TEXT    NOT NULL,
                    last_seen           TEXT    NOT NULL,
                    is_active           INTEGER DEFAULT 1,
                    UNIQUE(tenant, subscription_id, resource_group, name)
                );

                CREATE TABLE IF NOT EXISTS patch_runs (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id              TEXT    NOT NULL,
                    run_date            TEXT    NOT NULL,
                    tenant              TEXT    NOT NULL,
                    subscription_id     TEXT,
                    resource_group      TEXT    NOT NULL,
                    vm_name             TEXT    NOT NULL,
                    patches_available   INTEGER DEFAULT 0,
                    patches_applied     INTEGER DEFAULT 0,
                    patches_excluded    INTEGER DEFAULT 0,
                    status              TEXT,
                    error_detail        TEXT,
                    duration_seconds    INTEGER,
                    health_gate_passed  INTEGER DEFAULT 1,
                    wave                INTEGER DEFAULT 1
                );

                CREATE INDEX IF NOT EXISTS idx_vms_tenant
                    ON vms(tenant);
                CREATE INDEX IF NOT EXISTS idx_vms_active
                    ON vms(tenant, is_active);
                CREATE INDEX IF NOT EXISTS idx_patch_runs_tenant_date
                    ON patch_runs(tenant, run_date);
                CREATE INDEX IF NOT EXISTS idx_patch_runs_vm
                    ON patch_runs(tenant, vm_name);
            """)

    # ------------------------------------------------------------------ #
    # Inventory sync                                                        #
    # ------------------------------------------------------------------ #

    def sync_tenant_inventory(
        self,
        tenant: str,
        vms: List[Dict],
        subscription_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Full sync of a tenant's current VM inventory.

        Marks any VMs not in `vms` as inactive (retired/deleted).
        Upserts all current VMs.

        Returns counts: {'upserted': N, 'retired': M}
        """
        now = datetime.utcnow().isoformat()
        current_keys = set()

        with self._conn() as conn:
            for vm in vms:
                sub_id = vm.get("subscription_id") or subscription_id
                rg = vm.get("resource_group", "")
                name = vm.get("name", "")
                key = (tenant, sub_id, rg, name)
                current_keys.add(key)

                tags_json = json.dumps(vm.get("tags", {}))
                patch_wave = _extract_wave_tag(vm.get("tags", {}))

                conn.execute(
                    """
                    INSERT INTO vms
                        (tenant, subscription_id, resource_group, name, location,
                         os_type, vm_type, vm_size, private_ip, public_ip, tags,
                         patch_wave, first_seen, last_seen, is_active)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                    ON CONFLICT(tenant, subscription_id, resource_group, name) DO UPDATE SET
                        location    = excluded.location,
                        os_type     = excluded.os_type,
                        vm_type     = excluded.vm_type,
                        vm_size     = excluded.vm_size,
                        private_ip  = excluded.private_ip,
                        public_ip   = excluded.public_ip,
                        tags        = excluded.tags,
                        patch_wave  = excluded.patch_wave,
                        last_seen   = excluded.last_seen,
                        is_active   = 1
                    """,
                    (
                        tenant,
                        sub_id,
                        rg,
                        name,
                        vm.get("location"),
                        vm.get("os_type"),
                        vm.get("type", "Server"),
                        vm.get("vm_size"),
                        vm.get("private_ip"),
                        vm.get("public_ip"),
                        tags_json,
                        patch_wave,
                        now,
                        now,
                    ),
                )

            # Retire VMs that were NOT in this sync
            # (only retire within same subscription_id scope if provided)
            if subscription_id:
                rows = conn.execute(
                    "SELECT subscription_id, resource_group, name FROM vms "
                    "WHERE tenant=? AND subscription_id=? AND is_active=1",
                    (tenant, subscription_id),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT subscription_id, resource_group, name FROM vms "
                    "WHERE tenant=? AND is_active=1",
                    (tenant,),
                ).fetchall()

            retired = 0
            for row in rows:
                key = (tenant, row["subscription_id"], row["resource_group"], row["name"])
                if key not in current_keys:
                    conn.execute(
                        "UPDATE vms SET is_active=0, last_seen=? "
                        "WHERE tenant=? AND subscription_id=? AND resource_group=? AND name=?",
                        (now, tenant, row["subscription_id"], row["resource_group"], row["name"]),
                    )
                    retired += 1

        counts = {"upserted": len(vms), "retired": retired}
        logger.info(
            f"DB sync [{tenant}]: {counts['upserted']} upserted, {counts['retired']} retired"
        )
        return counts

    # ------------------------------------------------------------------ #
    # Patch run recording                                                   #
    # ------------------------------------------------------------------ #

    def record_patch_run(
        self,
        *,
        run_id: str,
        tenant: str,
        vm_name: str,
        resource_group: str,
        subscription_id: Optional[str] = None,
        patches_available: int = 0,
        patches_applied: int = 0,
        patches_excluded: int = 0,
        status: str = "unknown",
        error_detail: Optional[str] = None,
        duration_seconds: Optional[int] = None,
        health_gate_passed: bool = True,
        wave: int = 1,
    ):
        """Record one VM's outcome from a patch cycle.

        Note: retry attempt counts are surfaced in the GUI log and results
        dialog rather than stored here — the DB records the final outcome only.
        """
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO patch_runs
                    (run_id, run_date, tenant, subscription_id, resource_group, vm_name,
                     patches_available, patches_applied, patches_excluded,
                     status, error_detail, duration_seconds,
                     health_gate_passed, wave)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    datetime.utcnow().isoformat(),
                    tenant,
                    subscription_id,
                    resource_group,
                    vm_name,
                    patches_available,
                    patches_applied,
                    patches_excluded,
                    status,
                    error_detail,
                    duration_seconds,
                    1 if health_gate_passed else 0,
                    wave,
                ),
            )

    # ------------------------------------------------------------------ #
    # Read helpers (for human / diagnostic use)                            #
    # ------------------------------------------------------------------ #

    def get_vm_count(self, tenant: Optional[str] = None, active_only: bool = True) -> int:
        with self._conn() as conn:
            q = "SELECT COUNT(*) FROM vms"
            params: list = []
            conditions = []
            if tenant:
                conditions.append("tenant=?")
                params.append(tenant)
            if active_only:
                conditions.append("is_active=1")
            if conditions:
                q += " WHERE " + " AND ".join(conditions)
            return conn.execute(q, params).fetchone()[0]

    def get_all_known_vms(self, tenant: Optional[str] = None) -> List[Dict]:
        """Return ALL VMs ever seen (active and retired) — useful for reporting."""
        with self._conn() as conn:
            if tenant:
                rows = conn.execute(
                    "SELECT * FROM vms WHERE tenant=? ORDER BY tenant, name", (tenant,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM vms ORDER BY tenant, name"
                ).fetchall()
            return [dict(row) for row in rows]

    def get_patch_history(self, tenant: str, vm_name: Optional[str] = None, limit: int = 50) -> List[Dict]:
        """Recent patch run records for a tenant (or specific VM)."""
        with self._conn() as conn:
            if vm_name:
                rows = conn.execute(
                    "SELECT * FROM patch_runs WHERE tenant=? AND vm_name=? "
                    "ORDER BY run_date DESC LIMIT ?",
                    (tenant, vm_name, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM patch_runs WHERE tenant=? ORDER BY run_date DESC LIMIT ?",
                    (tenant, limit),
                ).fetchall()
            return [dict(row) for row in rows]

    def get_tenants(self) -> List[str]:
        """List all tenants with data in the DB."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT DISTINCT tenant FROM vms ORDER BY tenant"
            ).fetchall()
            return [r["tenant"] for r in rows]

    def get_db_path(self) -> str:
        return str(self.db_path)


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def _extract_wave_tag(tags: Dict) -> int:
    """Read PatchWave tag (1/2/3). Defaults to 1 if absent or invalid."""
    if not tags:
        return 1
    for key in tags:
        if key.lower() in ("patchwave", "patch_wave", "wave"):
            try:
                return max(1, min(3, int(tags[key])))
            except (ValueError, TypeError):
                pass
    return 1


def new_run_id() -> str:
    """Generate a unique ID for a patch cycle run."""
    return str(uuid.uuid4())
