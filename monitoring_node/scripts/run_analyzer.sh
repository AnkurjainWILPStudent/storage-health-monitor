#!/bin/bash
#
# Storage Health Analyzer - Run Script
# Executes the analyzer with proper environment and error handling
#

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
ANALYZER_SCRIPT="$PROJECT_ROOT/monitoring_node/scripts/analyze_storage_health.py"
CONFIG_FILE="$PROJECT_ROOT/monitoring_node/config/analyzer_config.json"
THRESHOLDS_FILE="$PROJECT_ROOT/monitoring_node/config/thresholds.json"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Check if Python 3 is available
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}Error: Python 3 is not installed${NC}"
    exit 1
fi

# Check if analyzer script exists
if [ ! -f "$ANALYZER_SCRIPT" ]; then
    echo -e "${RED}Error: Analyzer script not found at $ANALYZER_SCRIPT${NC}"
    exit 1
fi

# Check if config files exist
if [ ! -f "$CONFIG_FILE" ]; then
    echo -e "${YELLOW}Warning: Config file not found at $CONFIG_FILE${NC}"
fi

if [ ! -f "$THRESHOLDS_FILE" ]; then
    echo -e "${YELLOW}Warning: Thresholds file not found at $THRESHOLDS_FILE${NC}"
fi

# Set Python path
export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

# Parse command line arguments
VERBOSE=""
if [ "$1" == "--verbose" ] || [ "$1" == "-v" ]; then
    VERBOSE="--verbose"
fi

# Run the analyzer
echo -e "${GREEN}Starting Storage Health Analyzer...${NC}"
echo "Project root: $PROJECT_ROOT"
echo "Config: $CONFIG_FILE"
echo "Thresholds: $THRESHOLDS_FILE"
echo ""

python3 "$ANALYZER_SCRIPT" \
    --config "$CONFIG_FILE" \
    --thresholds "$THRESHOLDS_FILE" \
    $VERBOSE

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}Analyzer completed successfully${NC}"
else
    echo -e "${RED}Analyzer exited with code $EXIT_CODE${NC}"
fi

exit $EXIT_CODE
