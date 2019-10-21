import os
import json

import pandas as pd
from pandas.api.types import is_string_dtype


df = pd.read_csv('sample.csv')
fields = []
dimensions = []
for column in df.columns:
    field = {
        'field': column
    }
    if column == 'idx':
        field['type'] = 'categorical'
        field['cast'] = 'int'
    elif column in ['infectious', 'metabolic', 'blood', 'neurologic', 'heart_hypertensive', 'heart_ischemic',
                    'heart_failure', 'pulmonary', 'digestive', 'renal_insufficiency']:
        field['type'] = 'categorical'
        field['cast'] = 'int'
    elif is_string_dtype(df[column]):
        field['type'] = 'categorical'
    else:
        field['type'] = 'quantitative'
    fields.append(field)

    dimension = {
        'name': column,
        'p': 1
    }
    dimensions.append(dimension)

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
