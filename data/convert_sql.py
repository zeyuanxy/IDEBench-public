import sys
import json


def workflow_to_sql(dataset, workflow):
    table = 'tbl_{}'.format(dataset)

    bins = []
    for bin_desc in workflow['binning']:
        dimension = bin_desc["dimension"]

        if "width" in bin_desc:
            bin_width = bin_desc["width"]
            bins.append("FLOOR({}/{})".format(dimension, bin_width))
        else:
            bins.append(dimension)

    aggs = []
    for per_bin_aggregate_desc in workflow['perBinAggregates']:
        if per_bin_aggregate_desc['type'] == 'count':
            aggs.append('COUNT(*) as count')
        else:
            assert per_bin_aggregate_desc['type'] == 'avg'
            aggs.append('AVG({dimension}) as average_{dimension}'.format(dimension=per_bin_aggregate_desc['dimension']))

    sql = "SELECT {} ".format(', '.join(bins + aggs))
    sql += "FROM {} ".format(table)
    if 'filter' in workflow:
        sql += "WHERE {} ".format(workflow['filter'])
    sql += "GROUP BY {}".format(','.join(bins))

    return sql


if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # jobs
    with open(input_path, 'r') as f:
        workflows = json.load(f)
    sqls = []
    viz = {}
    for workflow in workflows['interactions']:
        if workflow['name'] not in viz:
            viz[workflow['name']] = workflow
        else:
            workflow = {**workflow, **viz[workflow['name']]}
        sql = workflow_to_sql(workflows['dataset'], workflow)
        sqls.append(sql)

    with open(output_path, 'w') as f:
        f.write('\n'.join(sqls))
