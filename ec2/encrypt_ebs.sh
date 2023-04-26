#!/bin/bash

timestamp() {
  date "+%Y-%m-%d %H:%M:%S" # current time
}

timestamp
# aws ec2 describe-instances --region ap-south-1
instance_id=`aws ec2 describe-instances --region ap-south-1 --filters "Name=instance-type,Values=t2.micro" --query "Reservations[].Instances[].InstanceId[]"`
echo $instance_id
# echo $instance_id | sed -i "s,'[',' ',g" #|awk {print $0}
# awk -F',' '{ for( i=1; i<=NF; i++ ) print $i }' <<<"$instance_id"
for i in $instance_id
do
    echo $i
    aws ec2 describe-volumes --filters Name=attachment.instance-id,Values=$i Name=attachment.delete-on-termination,Values=true
done
# PID=`ps -aef | grep node | grep -v grep | awk '{print $2}'`
# aws ec2 describe-volumes --filters Name=attachment.instance-id,Values=$instance_id Name=attachment.delete-on-termination,Values=true