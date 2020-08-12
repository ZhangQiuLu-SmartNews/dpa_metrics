# upload train csv file
set +e
set -x
startdate=$1
enddate=$2
curr="$startdate"
while true; do
    echo "$curr"

    dt=$curr
    cd /mnt1/smart-ad-dmp/azkaban-flow/datafeed
    s3_prefix="smartad-dmp/warehouse/user/yukimura/dpa_u2i_rakten_two_tower_train_sample_d/dt=${dt}"
    data="/mnt1/train/${s3_prefix}/merged.data"
    if [ -e $data ]; then
        rm $data
    fi
    bash bin/s3_sync_mnt "/mnt1/train" ${s3_prefix}
    du -khs ${data}

    if [ -e $data ]; then
        aws_u2i_train_data="s3://smartad-dmp/warehouse/user/yukimura/dpa/u2i/two_tower/train_data/dt=${dt}/train_sample.csv"
        aws s3 cp $data $aws_u2i_train_data
        rm $data
    fi
    [ "$curr" \< "$enddate" ] || break
    curr=$( date +%Y-%m-%d --date "$curr +1 day" )
done