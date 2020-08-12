# upload positive item file
set +e
set -x
dt=$1
cd ~/smart-ad-dmp/azkaban-flow/datafeed
s3_prefix="smartad-dmp/warehouse/user/yukimura/dpa_rakuten_user_item_positive_d/dt=${dt}"
item_data="/mnt1/train/${s3_prefix}/merged.data"
if [ -e $item_data ]; then
    rm $item_data
fi
bash bin/s3_sync_mnt "/mnt1/train" ${s3_prefix}
du -khs ${item_data}

if [ -e $item_data ]; then
    aws_u2i_postive_data="s3://smartad-dmp/warehouse/user/yukimura/dpa/u2i/two_tower/positive_item/dt=${dt}/positive_item.csv"
    aws s3 cp $item_data $aws_u2i_postive_data
    rm $item_data
fi