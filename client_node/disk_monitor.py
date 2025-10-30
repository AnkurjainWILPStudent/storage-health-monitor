#!/usr/bin/env python3
import os
import subprocess
import json
import socket
import tempfile
import shutil
from pathlib import Path

import psutil

from utils import setup_logger, load_config, timestamp, write_json_atomic

CONFIG_PATH = Path(__file__).parent / "config.json"

def get_hostname():
    return socket.gethostname()

def collect_disk_usage():
    partitions = []
    for p in psutil.disk_partitions(all=False):
        try:
            usage = psutil.disk_usage(p.mountpoint)
        except PermissionError:
            continue
        partitions.append({
            "device": p.device,
            "mountpoint": p.mountpoint,
            "fstype": p.fstype,
            "opts": p.opts,
            "total": usage.total,
            "used": usage.used,
            "free": usage.free,
            "percent": usage.percent
        })
    return partitions

def list_block_disks():
    # return list like ['/dev/sda', '/dev/sdb']
    out = subprocess.run(["lsblk", "-ndo", "NAME,TYPE"], capture_output=True, text=True)
    disks = []
    if out.returncode == 0:
        for line in out.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) == 2 and parts[1] == "disk":
                disks.append("/dev/" + parts[0])
    return disks

def smart_check(device):
    # returns dict with status and raw smartctl output (may require sudo)
    try:
        res = subprocess.run(["smartctl", "-H", device], capture_output=True, text=True, timeout=20)
        if res.returncode == 0:
            status = "OK" if "PASSED" in res.stdout or "SMART overall-health self-assessment test result: PASSED" in res.stdout else "UNKNOWN"
        else:
            status = "ERROR"
        return {"device": device, "returncode": res.returncode, "stdout": res.stdout, "stderr": res.stderr, "status": status}
    except FileNotFoundError:
        return {"device": device, "error": "smartctl-not-installed"}
    except Exception as e:
        return {"device": device, "error": str(e)}

def run():
    cfg = load_config(CONFIG_PATH)
    logger = setup_logger(cfg["log_path"])
    hostname = get_hostname()
    logger.info("Starting disk monitor run on host %s", hostname)

    payload = {
        "collected_at": timestamp(),
        "host": hostname,
        "disk_partitions": collect_disk_usage(),
        "smart": [],
        "notes": []
    }

    # SMART checks
    disks = list_block_disks()
    if not disks:
        payload["notes"].append("no-block-disks-found-or-lsblk-failed")
    else:
        for d in disks:
            sc = smart_check(d)
            payload["smart"].append(sc)

    # write to temp file
    tmpdir = tempfile.mkdtemp(prefix="client_disk_")
    try:
        filename = f"{hostname}_{payload['collected_at'].replace(':','-')}.json"
        local_path = os.path.join(tmpdir, filename)
        write_json_atomic(local_path, payload)
        logger.info("Wrote payload to %s", local_path)

        # SCP to monitoring node
        monitor_ip = cfg["monitoring_node_ip"]
        monitor_user = cfg["monitoring_node_user"]
        monitor_dir = cfg["monitoring_node_receive_dir"]
        monitor_port = cfg.get("port", 22)

        # remote path: <monitor_dir>/<hostname>/
        remote_subdir = f"{monitor_dir.rstrip('/')}/{hostname}"
        # Prepare remote directory (via ssh mkdir -p)
        mkdir_cmd = ["ssh", "-o", "StrictHostKeyChecking=no", "-p", str(monitor_port),
             f"{monitor_user}@{monitor_ip}", f"mkdir -p {remote_subdir} && chmod 750 {remote_subdir}"]
        logger.info("Ensuring remote directory exists: %s", remote_subdir)
        mk = subprocess.run(mkdir_cmd, capture_output=True, text=True)
        if mk.returncode != 0:
            logger.warning("Remote mkdir failed: rc=%s stderr=%s", mk.returncode, mk.stderr.strip())
            payload["notes"].append("remote_mkdir_failed")
        else:
            # scp file
            scp_cmd = ["scp", "-o", "StrictHostKeyChecking=no", "-P", str(monitor_port),
           local_path, f"{monitor_user}@{monitor_ip}:{remote_subdir}/"]
            scp = subprocess.run(scp_cmd, capture_output=True, text=True)
            if scp.returncode != 0:
                logger.error("SCP failed: rc=%s stderr=%s", scp.returncode, scp.stderr.strip())
                payload["notes"].append("scp_failed")
            else:
                logger.info("SCP succeeded")
        # optionally keep local copy
        if cfg.get("keep_local_copy", False):
            dest_local = os.path.expanduser("~/client_node/sent_payloads")
            os.makedirs(dest_local, exist_ok=True)
            shutil.copy(local_path, os.path.join(dest_local, os.path.basename(local_path)))
        else:
            os.remove(local_path)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
    logger.info("Run finished.")

if __name__ == "__main__":
    run()
