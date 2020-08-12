# download positive item file
set +e
set -x
dt=$1
data_path="/mnt1/u2i/train/"
[ ! -d $data_path ] && mkdir $data_path
data_local=${data_path}positive_item_${dt}.csv
aws_u2i_train_data="s3://smartad-dmp/warehouse/user/yukimura/dpa/u2i/two_tower/positive_item/dt=${dt}/positive_item.csv"
aws s3 cp $aws_u2i_train_data $data_local
