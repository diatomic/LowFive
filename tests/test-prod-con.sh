#! /bin/bash

bin_dir=$1
num_procs=$2
producer=$3
consumer=$4
memory=$5
file=$6
shared=$7

if [ $shared == 0 ]; then
    echo "mpiexec -n ${num_procs} ${bin_dir}/prod-con-multidata-test -m ${memory} -f ${file} --prod_exec ${bin_dir}/${producer} --con_exec ${bin_dir}/${consumer}"
    mpiexec -n $num_procs $bin_dir/prod-con-multidata-test -m $memory -f $file --prod_exec $bin_dir/$producer --con_exec $bin_dir/$consumer
else
    echo "mpiexec -n ${num_procs} ${bin_dir}/prod-con-multidata-test -m ${memory} -f ${file} -s --prod_exec ${bin_dir}/${producer} --con_exec ${bin_dir}/${consumer}"
    mpiexec -n $num_procs $bin_dir/prod-con-multidata-test -m $memory -f $file -s --prod_exec $bin_dir/$producer --con_exec $bin_dir/$consumer
fi

retval=$?

if [ -f "$bin_dir/outfile.h5" ]; then
    rm "$bin_dir/outfile.h5"
fi

if [ $retval == 0 ]; then
    exit 0
else
    exit 1
fi


