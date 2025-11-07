#!/bin/bash

# Quick Start Guide Display Script
# Run this to get oriented with the project

clear

cat << 'EOF'
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║         Storage Health Monitor - VirtualBox Lab Setup               ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝

📋 PROJECT OVERVIEW
───────────────────────────────────────────────────────────────────────
This is a distributed storage health monitoring system for VirtualBox:
  • 1 Monitoring Node VM  → Receives & analyzes reports
  • 2 Client VMs (user1, user2) → Collect & send disk metrics

All scripts are production-ready with full error handling and logging.

🗂️  WHAT'S IN THIS PROJECT
───────────────────────────────────────────────────────────────────────
Documentation:
  📄 README.md                 - Project overview & features
  📄 VIRTUALBOX_SETUP.md       - Complete VM setup guide (START HERE!)
  📄 DEPLOYMENT_CHECKLIST.md   - Step-by-step deployment checklist
  📄 QUICKREF.md               - Quick command reference
  📄 PROJECT_SUMMARY.md        - What was created and why

Scripts:
  🔧 test_setup.sh             - Verify system configuration

Client Node (Deploy on user1 & user2):
  📁 client_node/
     ├── disk_monitor.py       - Collects disk metrics & SMART status
     ├── utils.py              - Utility functions
     ├── config.json           - Configuration file
     ├── requirements.txt      - Python dependencies
     └── scripts/
         └── setup_client.sh   - Automated client setup

Monitoring Node (Deploy on monitoring VM):
  📁 monitoring_node/
     ├── scripts/
     │   ├── analyze_storage_health.py - Main analyzer
     │   └── setup_monitoring.sh       - Automated monitoring setup
     ├── config/
     │   ├── analyzer_config.json      - Analyzer configuration
     │   └── thresholds.json           - Alert thresholds
     ├── utils.py              - Utility functions
     ├── data_validator.py     - Data validation
     ├── alert_handler.py      - Alert management
     └── requirements.txt      - Python dependencies

🚀 QUICK START (3 STEPS)
───────────────────────────────────────────────────────────────────────

STEP 1: Setup Monitoring VM
  1. Copy monitoring_node/ directory to your monitoring VM
  2. Run: cd ~/storage-health-monitor/monitoring_node/scripts
  3. Run: chmod +x setup_monitoring.sh && ./setup_monitoring.sh
  4. Note the IP address displayed at the end

STEP 2: Setup Client VMs (user1 and user2)
  1. Copy client_node/ directory to each client VM
  2. Run: cd ~/storage-health-monitor/client_node/scripts
  3. Run: chmod +x setup_client.sh && ./setup_client.sh
  4. Enter monitoring node IP when prompted
  5. Copy SSH key: ssh-copy-id -i ~/.ssh/id_ed25519.pub admin@MONITORING_IP

STEP 3: Verify Everything Works
  1. On each VM, run: cd ~/storage-health-monitor && ./test_setup.sh
  2. Should see "All tests passed!"

📚 DETAILED DOCUMENTATION
───────────────────────────────────────────────────────────────────────

For Network Setup:
  → Read: VIRTUALBOX_SETUP.md (section: Network Configuration)
  → Configure Host-Only or Bridged networking
  → Set static IPs or DHCP reservations
  → Update /etc/hosts on all VMs

For Complete Deployment:
  → Read: DEPLOYMENT_CHECKLIST.md
  → Follow each checkbox step-by-step
  → Includes pre-deployment, deployment, and verification

For Daily Operations:
  → Read: QUICKREF.md
  → Common commands, troubleshooting, config changes
  → Keep this handy!

For Project Understanding:
  → Read: PROJECT_SUMMARY.md
  → What was created and why
  → Architecture and data flow

🔍 TESTING YOUR SETUP
───────────────────────────────────────────────────────────────────────

Run the verification script on each VM:
  $ cd ~/storage-health-monitor
  $ chmod +x test_setup.sh
  $ ./test_setup.sh

This checks:
  ✓ Dependencies installed
  ✓ Network connectivity
  ✓ SSH configuration
  ✓ File permissions
  ✓ Service status

📊 EXAMPLE WORKFLOW
───────────────────────────────────────────────────────────────────────

1. Client (user1) collects metrics every 5 minutes:
   disk_monitor.py → measures disk usage → generates JSON

2. Client sends to monitoring node via SCP:
   user1 VM → SSH/SCP → monitoring VM: ~/monitor_data/user1/file.json

3. Monitoring node analyzes every 10 minutes:
   analyzer.py → validates → checks thresholds → alerts if needed

4. You review results:
   tail -f ~/storage-health-monitor/monitoring_node/logs/analyzer.log
   ls -l ~/storage-health-monitor/monitoring_node/reports/

⚙️  KEY CONFIGURATION
───────────────────────────────────────────────────────────────────────

Client Config (client_node/config.json):
  • monitoring_node_ip: IP of your monitoring VM
  • monitoring_node_user: Username on monitoring VM
  • interval_minutes: How often to collect (default: 5)

Monitoring Config (monitoring_node/config/analyzer_config.json):
  • data_directory: Where clients send files
  • retention_days: How long to keep archives (default: 30)

Thresholds (monitoring_node/config/thresholds.json):
  • disk_usage warning: 75%, critical: 90%
  • data_freshness: 15 minutes max age
  • Fully customizable!

🛠️  TROUBLESHOOTING
───────────────────────────────────────────────────────────────────────

Can't SSH from client to monitoring?
  → Check: ping MONITORING_IP
  → Check: ssh -v -i ~/.ssh/id_ed25519 user@MONITORING_IP
  → Fix: chmod 600 ~/.ssh/id_ed25519

No data appearing on monitoring node?
  → Check client logs: tail client_node/logs/client_log.log
  → Check data directory matches config
  → Check SSH key authentication

Analyzer failing?
  → Check analyzer logs: tail monitoring_node/logs/analyzer.log
  → Run manually: python3 monitoring_node/scripts/analyze_storage_health.py
  → Check Python version: python3 --version (need 3.8+)

For more troubleshooting, see QUICKREF.md

📞 WHERE TO GET HELP
───────────────────────────────────────────────────────────────────────

1. Check the logs (most issues show up there)
2. Run test_setup.sh to identify configuration issues
3. Review VIRTUALBOX_SETUP.md troubleshooting section
4. Check QUICKREF.md for common commands

🎯 YOUR NEXT STEPS
───────────────────────────────────────────────────────────────────────

If you're just starting:
  1. Read VIRTUALBOX_SETUP.md (20 minutes)
  2. Follow DEPLOYMENT_CHECKLIST.md step-by-step
  3. Run test_setup.sh on each VM
  4. Verify end-to-end data flow

If you have VMs ready:
  1. Run setup_monitoring.sh on monitoring VM
  2. Run setup_client.sh on each client VM
  3. Copy SSH keys
  4. Test!

If you want to understand the code:
  1. Read PROJECT_SUMMARY.md
  2. Review disk_monitor.py (client collection)
  3. Review analyze_storage_health.py (monitoring analysis)
  4. Check config files to understand settings

╔══════════════════════════════════════════════════════════════════════╗
║  Ready to begin? Start with: VIRTUALBOX_SETUP.md                    ║
║  Need quick commands? Check: QUICKREF.md                            ║
║  Step-by-step deployment? Use: DEPLOYMENT_CHECKLIST.md              ║
╚══════════════════════════════════════════════════════════════════════╝

EOF

echo ""
echo "📂 Available documentation files:"
ls -1 *.md 2>/dev/null || echo "Run this script from the project root directory"
echo ""
echo "🚀 Happy monitoring!"
echo ""
