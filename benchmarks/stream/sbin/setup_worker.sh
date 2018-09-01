sbin="`dirname "$0"`"
sbin="`cd "$sbin"; pwd`"
conf="`cd "$sbin/../conf/"; pwd`"
ray_root="`cd "$sbin/../../../"; pwd`"

echo "Installing dependencies..."
lsof /var/lib/dpkg/lock
sudo pkill -9 apt-get || true
sudo pkill -9 dpkg || true
sudo dpkg --configure -a
lsof /var/lib/dpkg/lock
sudo apt-get update
sudo apt-get install -y cmake pkg-config build-essential autoconf curl libtool unzip flex bison psmisc python 
sudo apt-get install -y python3-dev 
sudo apt-get install -y python-pip 

pip install cmake
pip install ujson
pip install cython==0.27.3
pip install boto3

echo "Building..."
rm -rf $ray_root/thirdparty/build/arrow
rm -rf $ray_root/thirdparty/pkg/arrow
rm -rf $ray_root/thirdparty/build/parquet-cpp
rm -rf $ray_root/python/ray/core
rm -rf $ray_root/python/ray/pyarrow_files

cd $ray_root && git checkout python/ray/core
cd $ray_root && git checkout python/ray/pyarrow_files
cd $ray_root && ./build.sh;

cd $ray_root/python && pip install --user -e . --verbose
