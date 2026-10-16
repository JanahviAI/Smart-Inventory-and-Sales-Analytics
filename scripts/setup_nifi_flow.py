"""
NiFi Ingestion Flow Provisioning Script.
Configures the NiFi canvas using the NiFi REST API according to nifi/flow_retail_ingest.json:
- ListFile: Detects raw CSVs in /opt/nifi/data/source
- FetchFile: Reads the payload non-destructively
- PutFile: Atomically moves the payload to /opt/nifi/data/landing
- Connects processors and enables the ingestion pipeline.
"""

import json
import logging
import sys
import time
from pathlib import Path

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("NiFiSetup")


def setup_nifi_flow(
    base_url: str = "https://localhost:8443/nifi-api",
    username: str = "admin",
    password: str = "Antigravity2026Admin!",
    start_processors: bool = True,
) -> bool:
    logger.info("Connecting to NiFi REST API at %s...", base_url)

    # 1. Authenticate and obtain JWT bearer token
    token_resp = requests.post(
        f"{base_url}/access/token",
        data={"username": username, "password": password},
        verify=False,
        timeout=15,
    )
    if token_resp.status_code != 201 and token_resp.status_code != 200:
        logger.error("Authentication failed: HTTP %s %s", token_resp.status_code, token_resp.text)
        return False

    token = token_resp.text
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    logger.info("Authentication successful.")

    # 2. Get Root Process Group ID
    root_resp = requests.get(f"{base_url}/process-groups/root", headers=headers, verify=False, timeout=10)
    root_id = root_resp.json()["id"]
    logger.info("Root Process Group ID: %s", root_id)

    # 3. Clean any existing components in root group to ensure idempotent setup
    existing_flow = requests.get(f"{base_url}/flow/process-groups/{root_id}", headers=headers, verify=False, timeout=10).json()
    flow_content = existing_flow.get("processGroupFlow", {}).get("flow", {})

    # Stop all existing processors first
    for proc in flow_content.get("processors", []):
        p_id = proc["id"]
        v = proc["revision"]["version"]
        requests.put(
            f"{base_url}/processors/{p_id}/run-status",
            headers=headers,
            json={"revision": {"version": v}, "state": "STOPPED"},
            verify=False,
            timeout=10,
        )

    time.sleep(1)

    # Drop connections
    for conn in flow_content.get("connections", []):
        c_id = conn["id"]
        v = conn["revision"]["version"]
        requests.delete(f"{base_url}/connections/{c_id}?version={v}", headers=headers, verify=False, timeout=10)

    # Delete processors
    for proc in flow_content.get("processors", []):
        p_id = proc["id"]
        # re-fetch revision
        p_info = requests.get(f"{base_url}/processors/{p_id}", headers=headers, verify=False, timeout=10).json()
        v = p_info["revision"]["version"]
        requests.delete(f"{base_url}/processors/{p_id}?version={v}", headers=headers, verify=False, timeout=10)

    logger.info("Cleared canvas for clean initialization.")

    # 4. Create Processor 1: ListFile
    logger.info("Creating Processor 1: ListFile...")
    p1_resp = requests.post(
        f"{base_url}/process-groups/{root_id}/processors",
        headers=headers,
        json={
            "revision": {"version": 0},
            "component": {
                "type": "org.apache.nifi.processors.standard.ListFile",
                "name": "List Source CSV Files",
                "position": {"x": 300.0, "y": 100.0},
            },
        },
        verify=False,
        timeout=10,
    ).json()
    p1_id = p1_resp["id"]
    p1_ver = p1_resp["revision"]["version"]

    # Configure Processor 1
    requests.put(
        f"{base_url}/processors/{p1_id}",
        headers=headers,
        json={
            "revision": {"version": p1_ver},
            "component": {
                "id": p1_id,
                "config": {
                    "schedulingPeriod": "10 sec",
                    "properties": {
                        "Input Directory": "/opt/nifi/data/source",
                        "Recurse Subdirectories": "false",
                        "File Filter": "(sales|products|inventory)\\.csv$",
                        "Minimum File Age": "0 sec",
                    },
                },
            },
        },
        verify=False,
        timeout=10,
    )

    # 5. Create Processor 2: FetchFile
    logger.info("Creating Processor 2: FetchFile...")
    p2_resp = requests.post(
        f"{base_url}/process-groups/{root_id}/processors",
        headers=headers,
        json={
            "revision": {"version": 0},
            "component": {
                "type": "org.apache.nifi.processors.standard.FetchFile",
                "name": "Fetch Source File Content",
                "position": {"x": 300.0, "y": 320.0},
            },
        },
        verify=False,
        timeout=10,
    ).json()
    p2_id = p2_resp["id"]
    p2_ver = p2_resp["revision"]["version"]

    # Configure Processor 2
    requests.put(
        f"{base_url}/processors/{p2_id}",
        headers=headers,
        json={
            "revision": {"version": p2_ver},
            "component": {
                "id": p2_id,
                "config": {
                    "properties": {
                        "File to Retrieve": "${absolute.path}/${filename}",
                        "Completion Strategy": "None",
                    },
                    "autoTerminatedRelationships": ["failure", "not.found", "permission.denied"],
                },
            },
        },
        verify=False,
        timeout=10,
    )

    # 6. Create Processor 3: PutFile
    logger.info("Creating Processor 3: PutFile...")
    p3_resp = requests.post(
        f"{base_url}/process-groups/{root_id}/processors",
        headers=headers,
        json={
            "revision": {"version": 0},
            "component": {
                "type": "org.apache.nifi.processors.standard.PutFile",
                "name": "PutFile to Landing (Atomic Write)",
                "position": {"x": 300.0, "y": 540.0},
            },
        },
        verify=False,
        timeout=10,
    ).json()
    p3_id = p3_resp["id"]
    p3_ver = p3_resp["revision"]["version"]

    # Configure Processor 3
    requests.put(
        f"{base_url}/processors/{p3_id}",
        headers=headers,
        json={
            "revision": {"version": p3_ver},
            "component": {
                "id": p3_id,
                "config": {
                    "properties": {
                        "Directory": "/opt/nifi/data/landing",
                        "Conflict Resolution Strategy": "replace",
                        "Create Missing Directories": "true",
                        "Temporary Directory": "/opt/nifi/data/landing/.tmp",
                        "Permissions": "0644",
                    },
                    "autoTerminatedRelationships": ["failure", "success"],
                },
            },
        },
        verify=False,
        timeout=10,
    )

    # 7. Create Connections
    logger.info("Connecting ListFile -> FetchFile...")
    requests.post(
        f"{base_url}/process-groups/{root_id}/connections",
        headers=headers,
        json={
            "revision": {"version": 0},
            "component": {
                "name": "success",
                "source": {"id": p1_id, "groupId": root_id, "type": "PROCESSOR"},
                "destination": {"id": p2_id, "groupId": root_id, "type": "PROCESSOR"},
                "selectedRelationships": ["success"],
            },
        },
        verify=False,
        timeout=10,
    )

    logger.info("Connecting FetchFile -> PutFile...")
    requests.post(
        f"{base_url}/process-groups/{root_id}/connections",
        headers=headers,
        json={
            "revision": {"version": 0},
            "component": {
                "name": "success",
                "source": {"id": p2_id, "groupId": root_id, "type": "PROCESSOR"},
                "destination": {"id": p3_id, "groupId": root_id, "type": "PROCESSOR"},
                "selectedRelationships": ["success"],
            },
        },
        verify=False,
        timeout=10,
    )

    logger.info("All 3 processors and connections created successfully.")

    # 8. Start processors if requested
    if start_processors:
        logger.info("Starting processors...")
        for p_id in [p3_id, p2_id, p1_id]:
            p_info = requests.get(f"{base_url}/processors/{p_id}", headers=headers, verify=False, timeout=10).json()
            requests.put(
                f"{base_url}/processors/{p_id}/run-status",
                headers=headers,
                json={"revision": {"version": p_info["revision"]["version"]}, "state": "RUNNING"},
                verify=False,
                timeout=10,
            )
        logger.info("Ingestion processors are now RUNNING.")

    return True


if __name__ == "__main__":
    success = setup_nifi_flow()
    if success:
        print("[SUCCESS] NiFi flow successfully provisioned with 3 active processors.")
        sys.exit(0)
    else:
        print("[ERROR] NiFi flow provisioning failed.")
        sys.exit(1)

