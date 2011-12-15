#!/bin/bash

pinger_host=$1
ponger_host=$2
amqp_host=$3

exchange=`uuidgen`

out_dir="call_pingpong".`date +%s`
mkdir $out_dir
out_file=$out_dir/pingpong.data
l_conf_file=`pwd`/ping.yml
r_conf_file=/tmp/conf.yml
py=/home/bresnaha/DASHITESTS/bin/python

l_pgm_file=`pwd`/call_pingpong.py
r_pgm_file=/tmp/call_pingpong.py

scp $l_conf_file $pinger_host:$r_conf_file
scp $l_conf_file $ponger_host:$r_conf_file

scp $l_pgm_file $pinger_host:$r_pgm_file
scp $l_pgm_file $ponger_host:$r_pgm_file

trials=1

touch $out_file
date >> $out_file

for i in `seq $trials`
do
    echo "running: $i"

    cpu_file=$out_file.pong.$i
    ssh $ponger_host top -b -d 1 > $cpu_file&
    kill_pid1=$!
    cpu_file=$out_file.$i.$cnt.$sz.sendcpu
    ssh $pinger_host top -b -d 1 > $cpu_file&
    kill_pid2=$!

    cmd_line_args="--server.amqp.host=$amqp_host --dashi.exchange=$exchange"

    ssh $ponger_host $py $r_pgm_file --test.type=pong $cmd_line_args $r_conf_file &
    recv_pid=$!
    sleep 2
    ssh $pinger_host $py $r_pgm_file --test.type=ping $cmd_line_args $r_conf_file >> $out_file

    echo "pinger finished, waiting for ponger"
    wait $recv_pid
    echo "ponger finished"
    kill $kill_pid1
    kill $kill_pid2
    echo "kill sent, wiating"
    wait
    sleep 2

done
