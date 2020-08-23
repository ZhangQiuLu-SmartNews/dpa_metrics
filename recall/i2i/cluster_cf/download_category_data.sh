set +e
set -x
dt=$1
path=`pwd`
cd ~/smart-ad-dmp/azkaban-flow/datafeed
s3_prefix="smartad-dmp/warehouse/user/yukimura/cluster_item_category/dt=${dt}"
cluster_data="/mnt1/train/${s3_prefix}/merged.data"
if [ -e $cluster_data ]; then
    rm $cluster_data
fi
bash bin/s3_sync_mnt "/mnt1/train" ${s3_prefix}
du -khs ${cluster_data}

mv $cluster_data $path

