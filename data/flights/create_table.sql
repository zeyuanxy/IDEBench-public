DROP TABLE IF EXISTS tbl_flights;
CREATE TABLE tbl_flights (YEAR_DATE int,UNIQUE_CARRIER char(100),ORIGIN char(100),ORIGIN_STATE_ABR char(2),DEST char(100),DEST_STATE_ABR char(2),DEP_DELAY double,TAXI_OUT double,TAXI_IN double,ARR_DELAY double,AIR_TIME double,DISTANCE double);
COPY OFFSET 2 INTO tbl_flights FROM '/Users/zeyuan.shang/Coding/IDEBench-public/data/flights/flights.csv' DELIMITERS ',','\n','"';
