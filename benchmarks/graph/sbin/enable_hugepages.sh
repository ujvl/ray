hugepages_dir="/mnt/hugepages"
sudo mkdir -p $hugepages_dir 
gid=`id -g`
uid=`id -u`

sudo mount -t hugetlbfs -o uid=$uid -o gid=$gid none $hugepages_dir 
sudo bash -c "echo $gid > /proc/sys/vm/hugetlb_shm_group"
sudo bash -c "echo 10000 > /proc/sys/vm/nr_hugepages"
