set +e
set -x
dt=$1
cd ~/smart-ad-dmp/azkaban-flow/datafeed
s3_prefix="smartad-dmp/warehouse/user/seanchuang/i2i_offline_w2v_train_data/dt=${dt}"
w2v_data="/mnt1/train/${s3_prefix}/merged.data"
if [ -e $w2v_data ]; then
    rm $w2v_data
fi
bash bin/s3_sync_mnt "/mnt1/train" ${s3_prefix}
du -khs ${w2v_data}

mv $w2v_data `pwd`

