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