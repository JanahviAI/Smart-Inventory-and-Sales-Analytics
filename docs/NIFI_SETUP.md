# Apache NiFi Ingestion & Landing Guide (Person 1)

## 1. Overview & Architectural Role

In the Smart Inventory & Sales Analytics Platform, **Apache NiFi** acts as the ingestion boundary between upstream source file drops and the downstream data platform.

- **Source Location:** `data/source/` (mounted in NiFi container as `/opt/nifi/data/source`)
- **Landing Location:** `data/landing/` (mounted in NiFi container as `/opt/nifi/data/landing`)
- **Handoff Contract:** NiFi guarantees **atomic, non-destructive file writes** so that Apache Airflow never reads partial or mid-transfer files.

```
+-------------------+      +-----------------------------------------+      +--------------------+
|   data/source/    | ---> |        Apache NiFi Ingestion Flow       | ---> |   data/landing/    |
| (Source CSV Drop) |      | ListFile -> FetchFile -> PutFile(.tmp)  |      | (Ready for Airflow)|
+-------------------+      +-----------------------------------------+      +--------------------+
                                                                                       |
                                                                                       v
                                                                            +--------------------+
                                                                            | Airflow Batch DAG  |
                                                                            | (check_input_files)|
                                                                            +--------------------+
```

---

## 2. NiFi Processors & Flow Design

The complete process group specification is provided in `nifi/flow_retail_ingest.json`.

### A. Processor 1: `ListFile` (Detect Source Drop)
- **Type:** `org.apache.nifi.processors.standard.ListFile`
- **Input Directory:** `/opt/nifi/data/source`
- **File Filter:** `(sales|products|inventory)\.csv$`
- **Recurse Subdirectories:** `false`
- **Minimum File Age:** `0 sec`
- **Scheduling:** Timer Driven, `10 sec`
- **Responsibility:** Continuously tracks arrival of new or updated raw CSV datasets.

### B. Processor 2: `FetchFile` (Retrieve Payload)
- **Type:** `org.apache.nifi.processors.standard.FetchFile`
- **File to Retrieve:** `${absolute.path}/${filename}`
- **Completion Strategy:** `None` (preserves raw source files in `data/source/` without deleting or modifying them)
- **Incoming Relationship:** `success` from `ListFile`

### C. Processor 3: `PutFile` (Atomic Landing Route)
- **Type:** `org.apache.nifi.processors.standard.PutFile`
- **Directory:** `/opt/nifi/data/landing`
- **Conflict Resolution Strategy:** `replace`
- **Create Missing Directories:** `true`
- **Temporary Directory:** `/opt/nifi/data/landing/.tmp`
- **Permissions:** `0644`
- **Incoming Relationship:** `success` from `FetchFile`
- **Atomicity Safeguard:** Files are initially buffered in the `.tmp` staging subfolder and moved atomically into `data/landing/` upon completion.

---

## 3. Web UI Access & Import Procedure

1. **Start Services:**
   ```bash
   docker compose up -d nifi
   ```
2. **Access Web UI:**
   - URL: `https://localhost:8443/nifi`
   - Default credentials are set in `.env` / `docker-compose.yml` (`NIFI_SECURITY_USERNAME` and `NIFI_SECURITY_PASSWORD`).
3. **Import Flow Definition:**
   - In the top toolbar, drag the **Process Group** icon onto the canvas.
   - Click the **Upload** icon in the dialog.
   - Browse and select `nifi/flow_retail_ingest.json`.
   - Click **Add**.
4. **Start the Flow:**
   - Right-click the newly added Process Group and click **Start** (or select the Play icon).

---

## 4. Verification & Testing Tools

### A. Simulating Ingestion with CLI (Local & CI Verification)
To verify landing ingestion without running the heavy NiFi Java container:
```powershell
# Route source files to landing atomically
python scripts/simulate_nifi_ingest.py

# Verify files are landed and non-empty
python scripts/simulate_nifi_ingest.py --verify

# Clean landing directory (used for testing AT-02 missing-input handling)
python scripts/simulate_nifi_ingest.py --clean
```

### B. Observable Readiness Check
Airflow's task `check_input_files` looks for all 3 files:
1. `products.csv`
2. `inventory.csv`
3. `sales.csv`

All three must exist and have non-zero file size. If any file is missing, the task fails immediately, safely halting the batch before staging is touched.
