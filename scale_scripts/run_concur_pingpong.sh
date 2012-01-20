#!/bin/bash

pinger_host=$1
ponger_host=$2
amqp_host=$3

exchange=`uuidgen`

out_dir="concur_pingpong".`date +%s`
mkdir $out_dir
out_file=$out_dir/pingpong.data
l_conf_file=`pwd`/ping.yml
r_conf_file=/tmp/conf.yml
py=$DASHITESTS_PY

l_pgm_file=`pwd`/concur_pingpong.py
r_pgm_file=/tmp/concur_pingpong.py

scp $l_conf_file $pinger_host:$r_conf_file
scp $l_conf_file $ponger_host:$r_conf_file

scp $l_pgm_file $pinger_host:$r_pgm_file
scp $l_pgm_file $ponger_host:$r_pgm_file

trials=1
con_cur="1 8 16 64 128 199"

touch $out_file
date >> $out_file

for cur in $con_cur
do
for i in `seq $trials`
do
    echo "running: $i"

    ssh $pinger_host pkill python
    ssh $ponger_host pkill python

    cpu_file=$out_file.pong.$i
    ssh $ponger_host top -b -d 1 > $cpu_file&
    kill_pid1=$!
    cpu_file=$out_file.$i.$cnt.$sz.sendcpu
    ssh $pinger_host top -b -d 1 > $cpu_file&
    kill_pid2=$!

    cmd_line_args="--server.amqp.host=$amqp_host --dashi.exchange=$exchange --test.concur=$cur"

    ponger_cmd="ssh $ponger_host $py $r_pgm_file --test.type=pong $cmd_line_args $r_conf_file"
    $ponger_cmd &
    recv_pid=$!
    echo $ponger_cmd
    sleep 2

    ssh $pinger_host $py $r_pgm_file --test.type=ping $cmd_line_args $r_conf_file | tee $out_file

    echo "pinger finished, waiting for ponger"
    kill $recv_pid
    echo "ponger finished"
    kill $kill_pid1
    kill $kill_pid2
    echo "kill sent, wiating"
    wait
    sleep 2

done
done
