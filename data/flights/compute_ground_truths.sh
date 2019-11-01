# create table
mclient < create_table.sql

# compute ground truths
cd ../../
python idebench.py --settings-dataset flights --settings-size 1M --driver-name monetdb --groundtruth
cd -
