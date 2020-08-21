input=$1
output=$2
nohup python3 -m train.w2v_train --input w2v_train_data/$input --output w2v_train_result/Sinput.$output > w2v_train_log/Sinput.$output.log &