#!/bin/bash
set -euo pipefail

source ./venv/bin/activate

echo "=== Building database ==="
pylint scripts/build_projectdb.py

python scripts/build_projectdb.py

deactivate

echo "=== Importing to HDFS ==="
bash scripts/import2hdfs.sh