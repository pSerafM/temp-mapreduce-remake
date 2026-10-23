#!/usr/bin/env bash
# Executa as correlações Pearson com Hadoop Streaming.
set -e

SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(cd -- "$SCRIPT_DIR/.." && pwd)
INPUT_FILE="$PROJECT_DIR/forestfireinput.csv"
# Isola os arquivos do projeto no HDFS em uma pasta por usuário.
HDFS_BASE="/user/${USER:-$(whoami)}/forestfire"
HDFS_INPUT="$HDFS_BASE/input"
HDFS_DATA="$HDFS_INPUT/forestfireinput.csv"
HDFS_OUTPUT="$HDFS_BASE/output"

cd "$SCRIPT_DIR"
# Prepara entrada e remove uma saída antiga; Hadoop exige que a pasta de saída
# ainda não exista antes de iniciar um job.
hdfs dfs -mkdir -p "$HDFS_INPUT"
hdfs dfs -put -f "$INPUT_FILE" "$HDFS_INPUT/"
hdfs dfs -rm -r -f "$HDFS_OUTPUT"

# correlation.py contém as funções compartilhadas importadas por mapper,
# combiner e reducer; -files copia esses scripts para os nós executores.
hadoop jar "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files mapper.py,combiner.py,reducer.py,correlation.py \
  -mapper  "python3 mapper.py" \
  -combiner "python3 combiner.py" \
  -reducer "python3 reducer.py" \
  -input  "$HDFS_DATA" \
  -output "$HDFS_OUTPUT"

hdfs dfs -cat "$HDFS_OUTPUT"/part-*
