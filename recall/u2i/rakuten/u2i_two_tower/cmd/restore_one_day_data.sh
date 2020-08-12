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


# download train csv file
set +e
set -x
startdate=$1
enddate=$2
curr="$startdate"
while true; do
    echo "$curr"

    dt=$curr
    data_path="/mnt1/u2i/train/"
    [ ! -d $data_path ] && mkdir $data_path
    data_local=${data_path}train_sample_${dt}.csv
    aws_u2i_train_data="s3://smartad-dmp/warehouse/user/yukimura/dpa/u2i/two_tower/train_data/dt=${dt}/train_sample.csv"
    aws s3 cp $aws_u2i_train_data $data_local
    [ "$curr" \< "$enddate" ] || break
    curr=$( date +%Y-%m-%d --date "$curr +1 day" )
done

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

# download positive item file
set +e
set -x
dt=$1
data_path="/mnt1/u2i/train/"
[ ! -d $data_path ] && mkdir $data_path
data_local=${data_path}positive_item_${dt}.csv
aws_u2i_train_data="s3://smartad-dmp/warehouse/user/yukimura/dpa/u2i/two_tower/positive_item/dt=${dt}/positive_item.csv"
aws s3 cp $aws_u2i_train_data $data_local


# nohup python3 -u ./u2i_two_tower_train.py ./train/*.tfrecord ./validation/*.tfrecord ./postive_item_2020-08-07.csv > train.log &
# ps ax | grep ./u2i_two_tower_train.py