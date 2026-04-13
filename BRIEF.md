# Project Brief: Azure Update Manager Automation

## Overview

This is a production Python desktop application that automates Azure VM patch management for security and infrastructure teams operating multi-tenant Azure environments. It was built to solve a specific class of operational problem: Azure's Update Manager API is fully concurrent, but every existing interface to it — the Portal, CLI scripts, and basic automation runbooks — forces operators to interact with it sequentially, turning a task that Azure could complete in minutes into one that takes hours of manual supervision.

The tool makes concurrency the default. Every operation that can be parallelized — VM assessment, patch installation, health checks, IP address fetching, tenant authentication — runs concurrently via Python's `ThreadPoolExecutor`. The result is that a patch cycle covering dozens of VMs across multiple tenants completes in roughly the time it takes to patch a single VM manually.

---

## Problem Statement

Enterprise Azure patch management at scale has several structural problems that Azure's built-in tooling does not address:

**Sequential bottleneck.** Patching 50 VMs through the Portal or a basic script means waiting for each VM's assessment (up to 10 minutes) and patching (up to 30 minutes) to complete before starting the next. The operations are independent — there is no technical reason they can't run in parallel — but the tooling doesn't do it.

**No pre-patch validation.** Azure Update Manager will attempt to patch any running VM regardless of whether its agent is responsive, its disk is full, or it has a pending reboot that will interfere with patch application. These conditions cause silent or ambiguous failures that are discovered after the fact and require manual investigation.

**No blast radius control.** There is no native way in AUM to define that database servers should only be patched after application servers succeed, or to automatically halt a patch cycle if an unusually high number of VMs in one tier fail. Without this, a bad patch can cascade across all tiers before an operator notices.

**No persistent inventory.** Azure Portal shows current state. There is no native record of VMs that existed last month, their historical IP addresses, or which were successfully patched across past cycles. Compliance evidence and incident response ("what was running on that IP in March?") require manual record-keeping.

**Authentication friction.** In PIM-gated environments, operators must switch Azure accounts, activate roles, and authenticate to multiple tenants before starting work — all outside the patching tool. This creates a workflow that's easy to get wrong under time pressure.

---

## Technical Design Decisions

### Concurrency model
Python's `concurrent.futures.ThreadPoolExecutor` was chosen over `asyncio` because the Azure Python SDK uses synchronous blocking calls (poller.result()). Wrapping these in async would require either running a sync executor inside an event loop or rewriting the SDK interaction layer. ThreadPoolExecutor is transparent, debuggable, and directly compatible with the existing SDK pattern.

Worker counts are tuned conservatively (20 for assessment, 15 for patching) to avoid Azure API throttling while still achieving near-full parallelism for typical fleet sizes.

### Health gate design
All three health checks (agent responsiveness, disk space, pending reboot) use Azure Run Command rather than direct VM network access. This means the health gate works without VPN, without opening firewall rules to the management workstation, and without SSH/WinRM credentials — the same security model as AUM itself.

The disk check uses a threshold of 10% free on the system drive. Below this, Windows Update and apt both exhibit unreliable behaviour — patches may appear to succeed while failing to fully apply.

### Wave sequencing
The wave system uses a single Azure tag (`PatchWave`) rather than a separate configuration file. This keeps the wave assignment in Azure Resource Manager where it belongs — alongside other VM metadata — rather than in a local config that can drift from the actual environment. The failure gate threshold (default 30%) is configurable because the right value depends on environment risk tolerance.

### Local SQLite database
The database uses WAL journal mode so concurrent threads writing patch results don't block each other. The schema stores IP addresses alongside inventory records because IP-to-hostname mapping changes over time (VMs are deprovisioned, IPs reassigned) and the historical record is needed for security incident investigation. The database is never transmitted anywhere by design — it is a local-only audit artifact.

### Retry logic
The retry mechanism uses different backoff values (30s, 60s) for different failure modes. AUM out-of-sync errors typically resolve within 30 seconds. Agent-busy errors may take longer. Two retries were chosen as the minimum that catches the vast majority of transient failures without creating unacceptable delays in the overall patch window.

---

## Security Engineering Perspective

From a security engineering standpoint, this tool addresses several common failure modes in enterprise patch management:

**Silent patch non-application.** When a VM agent is unresponsive, Azure Update Manager may report a patch operation as "Succeeded" while nothing was actually applied. The health gate's agent responsiveness check prevents patching from proceeding on VMs where this would occur, converting a silent failure into an explicit skip with documented reason.

**Compliance evidence gaps.** Patch compliance evidence typically requires proof that a specific VM was patched by a specific date. When VMs are decommissioned between patch cycles, their records disappear from Azure. The local database preserves historical records indefinitely, maintaining the evidentiary chain even after VM deletion.

**Credential exposure surface.** The device-code authentication flow means no passwords or tokens ever pass through the application. The Azure CLI manages token lifecycle. PIM activation is handled via the ARM API without requiring the operator to navigate to the Portal under time pressure.

**Blast radius control.** Wave-based patch sequencing with automatic failure halting implements a principle familiar from secure deployment practices: validate each tier before exposing the next. A bad patch (or a patch interaction with a specific application version) is contained to the first wave rather than propagating to database or domain controller tiers.

**Audit trail integrity.** The local SQLite database is append-only in practice (records are written once, never updated) and is explicitly excluded from version control and network transmission. It can be copied to read-only storage for compliance archival without risk of modification.

---

## Operational Impact

The tool compresses the operational work of a monthly patch cycle in three ways:

1. **Parallel execution** reduces wall-clock time from the sum of per-VM patch times to the maximum single-VM patch time. For a 50-VM environment, this is typically an 80–90% reduction in active waiting time.

2. **Pre-patch health gating** eliminates the debugging cycle after a patch cycle where some VMs show ambiguous failure states. Known-bad VMs are identified before patching starts, not discovered afterward.

3. **Persistent inventory and reporting** eliminates the manual transcription of patch results to compliance records. The local database and Excel output are the compliance record, generated automatically at the end of each cycle.

---

## What This Is Not

This tool is not a replacement for Azure Update Manager itself — it is a control plane that sits on top of AUM and the Azure Run Command API. All patch operations are executed by Azure's managed service; this tool orchestrates which VMs get patched, in what order, with what pre-validation, and produces a structured record of the results.

It is also not a multi-user system. It is designed for a single operator or small team working from a shared workstation. For multi-user scenarios, the configuration and database would need to be moved to a shared network location and the authentication model would need to be redesigned around service principals rather than interactive device-code flow.

---

*This project is open source under the MIT license. Contributions, issues, and feature requests are welcome.*
