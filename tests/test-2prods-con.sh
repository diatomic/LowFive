#! /bin/bash
set -u

bin_dir=$(cd -- "$1" && pwd -P) || exit 1
num_procs=$2
producer1=$3
producer2=$4
consumer=$5
memory=$6
file=$7
shared=$8
mpi_launcher=$9
numproc_flag=${10}
shift 10

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/lowfive-2prods-con.XXXXXX") || exit 1
trap 'rm -rf -- "$work_dir"' EXIT
cd -- "$work_dir" || exit 1

command=("$mpi_launcher" "$numproc_flag" "$num_procs" "$@"
         "$bin_dir/2prods-con-multidata-test" -m "$memory" -f "$file")
if [[ "$shared" != 0 ]]; then command+=(-s); fi
command+=(--prod1_exec "$bin_dir/$producer1" --prod2_exec "$bin_dir/$producer2" --con_exec "$bin_dir/$consumer")
printf '%q ' "${command[@]}"
printf '\n'
"${command[@]}"
exit "$?"
