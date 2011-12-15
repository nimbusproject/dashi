#!/bin/bash

sender_host=$1
receiver_host=$2
amqp_host=$3

out_dir="thoughput".`date +%s`
mkdir $out_dir
out_file=$out_dir/throughput.size.data
l_conf_file=`pwd`/conf.yml
r_conf_file=/tmp/conf.yml
py=/home/bresnaha/DASHITESTS/bin/python

l_pgm_file=`pwd`/throughput.py
r_pgm_file=/tmp/throughput.py

echo $l_conf_file $sender_host:$r_conf_file
scp $l_conf_file $sender_host:$r_conf_file
scp $l_conf_file $receiver_host:$r_conf_file

scp $l_pgm_file $sender_host:$r_pgm_file
scp $l_pgm_file $receiver_host:$r_pgm_file

trials=4
entry_size="1"
entry_count="1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192"

touch $out_file
date >> $out_file

for sz in $entry_size
do
    for cnt in $entry_count
    do

        for i in `seq $trials`
        do
            echo "running: $i $cnt $sz"

            cpu_file=$out_file.$i.$cnt.$sz.recvcpu
            ssh $receiver_host top -b -d 1 > $cpu_file&
            kill_pid1=$!
            cpu_file=$out_file.$i.$cnt.$sz.sendcpu
            ssh $sender_host top -b -d 1 > $cpu_file&
            kill_pid2=$!

            cmd_line_args="--test.message.entry_size=$sz --test.message.entry_count=$cnt --server.amqp.host=$amqp_host"

            ssh $receiver_host $py $r_pgm_file --test.type=R $cmd_line_args $r_conf_file >> $out_file &
            recv_pid=$!
            sleep 2
            ssh $sender_host $py $r_pgm_file --test.type=S $cmd_line_args $r_conf_file >> $out_file

            echo "Sender finished, waiting for receiver"
            echo "Receiver finished"
            wait $recv_pid
            kill $kill_pid1
            kill $kill_pid2
            wait
            sleep 2

        done

    done

done
