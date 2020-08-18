# upload positive item file
set +e
set -x
dt=$1
cd ~/smart-ad-dmp/azkaban-flow/datafeed
s3_prefix="smartad-dmp/warehouse/user/yukimura/dpa_rakuten_i2i_user_item_score_test_d/dt=${dt}"
item_data="/mnt1/train/${s3_prefix}/merged.data"
if [ -e $item_data ]; then
    rm $item_data
fi
bash bin/s3_sync_mnt "/mnt1/train" ${s3_prefix}
du -khs ${item_data}
cp ${item_data} ./train_user_item_$1