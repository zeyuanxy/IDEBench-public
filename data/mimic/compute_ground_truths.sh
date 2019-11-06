# create table
mclient < create_table.sql

# compute ground truths
cd ../../
python idebench.py --settings-dataset mimic --settings-size 10M --driver-name monetdb --groundtruth
cd -
