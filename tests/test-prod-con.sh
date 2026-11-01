#! /bin/bash
set -u

bin_dir=$(cd -- "$1" && pwd -P) || exit 1
num_procs=$2
producer=$3
consumer=$4
memory=$5
file=$6
shared=$7
mpi_launcher=$8
numproc_flag=$9
shift 9

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/lowfive-prod-con.XXXXXX") || exit 1
trap 'rm -rf -- "$work_dir"' EXIT
cd -- "$work_dir" || exit 1

command=("$mpi_launcher" "$numproc_flag" "$num_procs" "$@"
         "$bin_dir/prod-con-multidata-test" -m "$memory" -f "$file")
if [[ "$shared" != 0 ]]; then command+=(-s); fi
command+=(--prod_exec "$bin_dir/$producer" --con_exec "$bin_dir/$consumer")
printf '%q ' "${command[@]}"
printf '\n'
"${command[@]}"
exit "$?"
