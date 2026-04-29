#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "Generating reports.."

python3 accumulators_report.py
echo "Accumulators generated.."

python3 entities_report.py
echo "Entities generated.."

python3 hnw_report.py
echo "HNW generated.."

python3 institutional_report.py
echo "Institutional generated.."

echo "All programs completed!"
