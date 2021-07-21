#! /bin/bash

bin_dir=$1
num_procs=$2
producer=$3
consumer1=$4
consumer2=$5
memory=$6
file=$7
shared=$8

if [ $shared == 0 ]; then
    mpiexec -n $num_procs $bin_dir/prod-2cons-multidata-test -m $memory -f $file --prod_exec $bin_dir/$producer --con1_exec $bin_dir/$consumer1 --con2_exec $bin_dir/$consumer2
else
    mpiexec -n $num_procs $bin_dir/prod-2cons-multidata-test -m $memory -f $file -s --prod_exec $bin_dir/$producer --con1_exec $bin_dir/$consumer1 --con2_exec $bin_dir/$consumer2
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


