#!/bin/bash

sudo mkdir -p /mnt/hugepages
gid=`id -g`
uid=`id -u`
sudo mount -t hugetlbfs -o uid=$uid -o gid=$gid none /mnt/hugepages
sudo bash -c "echo $gid > /proc/sys/vm/hugetlb_shm_group"
# 5000 is the number of 2MB hugepages to use. Total plasma memory is equal to 2 MB * nr_hugepages.
sudo bash -c "echo 5000 > /proc/sys/vm/nr_hugepages"
