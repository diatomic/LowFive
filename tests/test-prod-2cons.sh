#! /bin/bash
set -u

bin_dir=$(cd -- "$1" && pwd -P) || exit 1
num_procs=$2
producer=$3
consumer1=$4
consumer2=$5
memory=$6
file=$7
shared=$8
mpi_launcher=$9
numproc_flag=${10}
shift 10

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/lowfive-prod-2cons.XXXXXX") || exit 1
trap 'rm -rf -- "$work_dir"' EXIT
cd -- "$work_dir" || exit 1

command=("$mpi_launcher" "$numproc_flag" "$num_procs" "$@"
         "$bin_dir/prod-2cons-multidata-test" -m "$memory" -f "$file")
if [[ "$shared" != 0 ]]; then command+=(-s); fi
command+=(--prod_exec "$bin_dir/$producer" --con1_exec "$bin_dir/$consumer1" --con2_exec "$bin_dir/$consumer2")
printf '%q ' "${command[@]}"
printf '\n'
"${command[@]}"
exit "$?"
