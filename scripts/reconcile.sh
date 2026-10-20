#!/usr/bin/env bash
# Independently recounts lost jobs for every run in a benchmark directory:
# a job is lost if its token is in submitted.csv(.gz) but not in
# completed.csv(.gz). This does not trust the load generator's own count.
#
#   scripts/reconcile.sh bench/results/<run>
set -euo pipefail
DIR=${1:?usage: reconcile.sh RESULTS_DIR}

cat_csv() { if [[ -f "$1.gz" ]]; then gzip -dc "$1.gz"; else cat "$1"; fi; }

total=0
lost_total=0
while IFS= read -r run; do
  submitted=$(cat_csv "$run/submitted.csv" | tail -n +2 | wc -l)
  lost=$(awk -F, 'NR==FNR { done[$1] = 1; next } !($1 in done) { n++ } END { print n + 0 }' \
    <(cat_csv "$run/completed.csv" | tail -n +2) <(cat_csv "$run/submitted.csv" | tail -n +2))
  printf '%-40s submitted %7d  lost %d\n' "${run#"$DIR"/}" "$submitted" "$lost"
  total=$((total + submitted))
  lost_total=$((lost_total + lost))
done < <(find "$DIR" -name 'submitted.csv*' -printf '%h\n' | sort)
echo "total: $total acknowledged submits, $lost_total lost"
(( lost_total == 0 ))
