import os
import json

import pandas as pd
from pandas.api.types import is_string_dtype


df = pd.read_csv('sample.csv')
fields = []
dimensions = []
sql_types = []
for column in df.columns:
    field = {
        'field': column
    }
    sql_type = column + ' '
    if column == 'idx':
        field['type'] = 'categorical'
        field['cast'] = 'int'
        sql_type += 'int'
    elif column in ['infectious', 'metabolic', 'blood', 'neurologic', 'heart_hypertensive', 'heart_ischemic',
                    'heart_failure', 'pulmonary', 'digestive', 'renal_insufficiency']:
        field['type'] = 'categorical'
        field['cast'] = 'int'
        sql_type += 'int'
    elif is_string_dtype(df[column]):
        field['type'] = 'categorical'
        sql_type += 'char(100)'
    else:
        field['type'] = 'quantitative'
        sql_type += 'double'
    fields.append(field)

    dimension = {
        'name': column,
        'p': 1
    }
    dimensions.append(dimension)

    sql_types.append(sql_type)

output = {
    "tables": {
        "fact": {
            "name": "tbl_mimic",
            "fields": fields
        }
    }
}

with open('sample.json', 'w') as f:
    json.dump(output, f, indent=4)

for file in os.listdir("workflowtypes"):
    with open(os.path.join("workflowtypes", file), 'r') as f:
        workflow = json.load(f)
    workflow['dimensions'] = dimensions
    with open(os.path.join("workflowtypes", file), 'w') as f:
        json.dump(workflow, f, indent=4)

with open('create_table.sql', 'w') as f:
    f.write("DROP TABLE IF EXISTS tbl_mimic;\n")
    f.write("CREATE TABLE tbl_mimic ({});\n".format(','.join(sql_types)))
    file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "mimic.csv")
    f.write("COPY OFFSET 2 INTO tbl_mimic FROM '{}' DELIMITERS ',','\\n','\"';\n".format(file_path))

with open('create_table_duckdb.sql', 'w') as f:
    f.write("DROP TABLE IF EXISTS tbl_mimic;\n")
    f.write("CREATE TABLE tbl_mimic ({});\n".format(','.join(sql_types)))
    file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), "mimic.csv")
    f.write("COPY tbl_mimic FROM '{}' WITH HEADER\n".format(file_path))
