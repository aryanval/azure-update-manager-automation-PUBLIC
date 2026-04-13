# Azure Update Manager Automation

A production-grade Python desktop application for automating Azure VM patch management across multiple tenants. Built for managed service providers and enterprise teams who manage Azure environments at scale.

---

## What This Is

Azure Update Manager (AUM) Automation replaces manual, error-prone Azure Portal patching workflows with a concurrent, safety-first desktop application. It handles authentication, VM inventory, pre-patch health validation, patch execution with wave sequencing, and audit-grade Excel/SQLite tracking — all through a dark-themed GUI that runs on any workstation with Python and the Azure CLI.

The core insight driving this tool: the Azure Update Manager API is fully asynchronous, but the Portal forces you to interact with it sequentially. This tool fans out all VM operations concurrently, compressing a patch cycle that could take 8–12 hours of serial Portal work into 45–90 minutes of supervised automation.

---

## Key Capabilities

### Concurrent Patch Execution
All VM assessment and patch operations execute simultaneously via `ThreadPoolExecutor`. The patch window duration equals the **slowest single VM**, not the sum of all VMs. Up to 20 concurrent assessment workers and 15 concurrent patch workers by default.

### Pre-Patch Health Gate
Before touching any VM, the tool runs three concurrent health checks via Azure Run Command:
- **VM agent responsiveness** — an unresponsive agent means Run Command will silently fail
- **Disk space** — less than 10% free on the system drive blocks patches from applying correctly
- **Pending reboot (Windows)** — an existing pending reboot prevents new patches from being processed

VMs that fail any check are automatically excluded from the patch cycle with the specific reason logged and recorded.

### Patch Wave Sequencing
VMs are assigned to patch waves via an Azure tag (`PatchWave=1`, `PatchWave=2`, `PatchWave=3`). The tool executes waves in order with a configurable failure-rate gate between them. If more than 30% of wave 1 VMs fail, wave 2 and 3 are automatically halted — limiting blast radius before it reaches critical infrastructure.

```
Wave 1 (web/DMZ tier)     → all concurrent → failure gate check
Wave 2 (application tier) → all concurrent → failure gate check
Wave 3 (database tier)    → all concurrent
```

No equivalent capability exists in Azure Portal or base AUM.

### Automatic Retry with Backoff
Azure Update Manager occasionally returns transient errors (agent out-of-sync, operation in progress, VM temporarily busy). Both AUM-based and Run Command patching automatically retry up to **2 times** with 30s → 60s backoff before marking a VM as failed. Retry attempts are surfaced in real time in the GUI log; the audit record stores only the final outcome.

### In-Tool Device-Code Login
No pre-running `az login` required. A "Login to Azure" button launches an in-tool device-code flow that streams the authentication URL and code to the GUI, automatically opens the browser, and detects login completion. The entire workflow from first launch to patch execution runs without touching a terminal.

### Multi-Tenant Concurrent Dashboard
An "All Tenants" dashboard tab shows all configured tenants simultaneously. Authenticate, refresh inventory, and run health gates for all tenants concurrently from a single view. Patch window for N tenants is the time for the **slowest tenant**, not the sum.

### Local Inventory Database
A local SQLite database (`state/aum_inventory.db`) accumulates the full VM inventory across all tenants and patch cycles:
- Every VM ever seen, including retired/deleted machines
- Private and public IP addresses (fetched concurrently via Azure Network SDK)
- Patch history per VM across all cycles
- Health gate pass/fail per cycle
- Never transmitted anywhere — write-only from automation, read directly by operators via any SQLite client

### Intelligent SQL Server Protection
SQL Servers are automatically detected and excluded from patching via multiple detection patterns (VM name, tags, OS roles). SQL exclusions are enforced at both the AUM API layer and the Run Command layer.

### Power State Management
The tool records which VMs were deallocated before the patch cycle, starts them automatically, patches them, and restores them to their original power state afterward. State is persisted to JSON so an interrupted cycle can be recovered.

### Audit-Ready Excel Reporting
Each patch cycle generates a color-coded Excel workbook per tenant with structured sheets for Servers and AVDs (Azure Virtual Desktop). Historical VM records are preserved even when VMs are deleted mid-cycle.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Tkinter GUI (dark theme)                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌────────────────┐  │
│  │Inventory │  │Patching  │  │Monitoring│  │ All Tenants    │  │
│  │+ Health  │  │+ Waves   │  │+ Logs    │  │ Dashboard      │  │
│  │  Gate    │  │          │  │          │  │                │  │
│  └──────────┘  └──────────┘  └──────────┘  └────────────────┘  │
└───────────────────────────┬─────────────────────────────────────┘
                            │
         ┌──────────────────┼──────────────────┐
         │                  │                  │
┌────────▼────────┐  ┌──────▼──────┐  ┌───────▼───────┐
│  Azure Managers │  │   Health    │  │  Local SQLite │
│                 │  │    Gate     │  │   Inventory   │
│ • AuthManager   │  │             │  │   Database    │
│ • VMInventory   │  │ • Agent     │  │               │
│ • AUMManager    │  │ • Disk      │  │ • VM history  │
│ • PatchExecutor │  │ • Reboot    │  │ • Patch runs  │
│ • PowerManager  │  │             │  │ • IP records  │
│ • ExcelTracker  │  └─────────────┘  └───────────────┘
└────────┬────────┘
         │
┌────────▼──────────────────────┐
│   ThreadPoolExecutor          │
│   Concurrent fan-out          │
│   (20 assessment / 15 patch)  │
└───────────────────────────────┘
```

### Technology Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.9+ |
| GUI | Tkinter (cross-platform, no extra install) |
| Azure SDK | azure-mgmt-compute, azure-mgmt-network, azure-identity |
| Concurrency | concurrent.futures.ThreadPoolExecutor |
| Local DB | SQLite (WAL mode, gitignored) |
| Excel | openpyxl |
| Configuration | YAML |
| Authentication | Azure CLI + device-code flow |

---

## Setup

### Prerequisites

- Python 3.9 or later
- Azure CLI (`az` on PATH) — [install guide](https://docs.microsoft.com/cli/azure/install-azure-cli)
- Contributor or Virtual Machine Contributor role on target subscriptions (or PIM-eligible)

### Installation

```bash
git clone https://github.com/aryanval/azure-update-manager-automation-PUBLIC.git
cd azure-update-manager-automation-PUBLIC/aum_automation
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Configuration

```bash
cp config/config.example.yaml config/config.yaml
```

Edit `config.yaml` with your tenant names and subscription IDs:

```yaml
tenants:
  Production:
    subscription_id: "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    resource_groups: []          # empty = scan all resource groups
    avd_enabled: false

  Staging:
    subscription_ids:            # multiple subscriptions supported
      - "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
      - "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    avd_enabled: true
```

### Run

```bash
./launch.sh          # macOS / Linux
launch.bat           # Windows
# or: cd aum_automation && python3 main_gui.py
```

---

## Patch Wave Configuration

Tag VMs in Azure to control patching order:

| Tag | Value | Meaning |
|-----|-------|---------|
| `PatchWave` | `1` | First wave (DMZ, web, non-critical) |
| `PatchWave` | `2` | Second wave (application tier) |
| `PatchWave` | `3` | Third wave (database, domain controllers) |

Untagged VMs default to wave 1. The failure threshold (default 30%) is configurable in `config.yaml`.

---

## Local Database Queries

The inventory database at `aum_automation/state/aum_inventory.db` is gitignored and never leaves the machine. Query it directly:

```sql
-- All active VMs across tenants with IPs
SELECT tenant, name, resource_group, private_ip, os_type, last_seen
FROM vms WHERE is_active = 1 ORDER BY tenant, name;

-- Patch history for a specific VM
SELECT run_date, status, patches_applied, error_detail, wave
FROM patch_runs WHERE vm_name = 'vm-prod-01' ORDER BY run_date DESC;

-- VMs that failed the health gate
SELECT tenant, vm_name, status
FROM patch_runs WHERE health_gate_passed = 0
ORDER BY run_date DESC;

-- Retired VMs (decommissioned, historical record preserved)
SELECT tenant, name, private_ip, last_seen
FROM vms WHERE is_active = 0 ORDER BY last_seen DESC;
```

---

## Security Design

| Property | Implementation |
|----------|----------------|
| No stored credentials | Azure CLI credential; tokens managed by `az` |
| PIM support | In-tool PIM role discovery and activation via ARM API |
| Device-code login | Most phishing-resistant interactive Azure auth flow |
| Local-only audit data | SQLite DB is gitignored, never transmitted |
| SQL protection | Multi-layer detection at API and Run Command level |
| Health gate | Prevents silent patch non-application |
| Wave failure gate | Limits blast radius of a bad patch to one tier |

---

## Project Structure

```
aum_automation/
├── main_gui.py
├── requirements.txt
├── launch.sh / launch.bat
├── config/
│   ├── config.yaml              # Your config (gitignored)
│   └── config.example.yaml
└── modules/
    ├── auth_manager.py           # Auth + device-code login + PIM
    ├── aum_manager.py            # Concurrent assessment + wave patching + retry
    ├── patch_executor.py         # Run Command patching + concurrent + retry
    ├── vm_inventory.py           # Inventory + IP enrichment + DB sync
    ├── vm_power_manager.py       # Start/stop VMs + state persistence
    ├── health_gate.py            # Pre-patch health checks
    ├── db_manager.py             # Local SQLite inventory database
    ├── excel_tracker.py          # Audit-ready Excel reporting
    ├── error_handler.py          # Error categorisation + retry logic
    ├── config.py                 # YAML config loader
    ├── notifications.py          # In-app notification queue
    └── resource_graph_exporter.py # Azure Resource Graph + CSV export
```

---

## License

MIT
