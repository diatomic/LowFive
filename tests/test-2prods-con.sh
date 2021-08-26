#! /bin/bash

bin_dir=$1
num_procs=$2
producer1=$3
producer2=$4
consumer=$5
memory=$6
file=$7
shared=$8

if [ $shared == 0 ]; then
    mpiexec -n $num_procs $bin_dir/2prods-con-multidata-test -m $memory -f $file --prod1_exec $bin_dir/$producer1 --prod2_exec $bin_dir/$producer2 --con_exec $bin_dir/$consumer
else
    mpiexec -n $num_procs $bin_dir/2prods-con-multidata-test -m $memory -f $file -s --prod1_exec $bin_dir/$producer1 --prod2_exec $bin_dir/$producer2 --con_exec $bin_dir/$consumer
fi

retval=$?

if [ -f "$bin_dir/outfile.h5" ]; then
    rm "$bin_dir/outfile.h5"
fi

if [ -f "$bin_dir/outfile1.h5" ]; then
    rm "$bin_dir/outfile1.h5"
fi

if [ -f "$bin_dir/outfile2.h5" ]; then
    rm "$bin_dir/outfile2.h5"
fi

if [ $retval == 0 ]; then
    exit 0
else
    exit 1
fi


