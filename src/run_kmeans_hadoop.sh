#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
INPUT_FILE="$PROJECT_DIR/forestfireinput.csv"
OUTPUT_DIR="${1:-$PROJECT_DIR/kmeans_results}"
HDFS_BASE="/user/${USER:-$(whoami)}/forestfire-kmeans"
HDFS_INPUT="$HDFS_BASE/input"
HDFS_DATA="$HDFS_INPUT/forestfireinput.csv"

if [[ ! -f "$INPUT_FILE" ]]; then
  echo "Arquivo de entrada não encontrado: $INPUT_FILE" >&2
  exit 1
fi

hdfs dfs -mkdir -p "$HDFS_INPUT"
hdfs dfs -put -f "$INPUT_FILE" "$HDFS_INPUT/"

# O Hadoop fornece os registros pelo stdin; o processo Python faz a análise.
hdfs dfs -cat "$HDFS_DATA" |
  python3 "$SCRIPT_DIR/kmeans_hadoop" --input - --output "$OUTPUT_DIR"
