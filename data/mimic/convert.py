import sys
import json

from google.protobuf.json_format import MessageToJson
from davos.api import job_pb2, step_pb2, operators, operator_pb2, data_source_pb2


def workflow_to_job(workflow):
    job = job_pb2.JobDescription()

    StepDescription = step_pb2.StepDescription
    StepInputDescription = step_pb2.StepInputDescription
    InputType = step_pb2.StepInputDescription.InputType

    # sampling strategy
    sampling_strategy = data_source_pb2.SamplingStrategy()
    sampling_strategy.full.SetInParent()
    job.sampling_strategy.MergeFrom(sampling_strategy)

    # filter
    has_filter = False
    if 'filter' in workflow:
        FilterDescription = operators.filter_pb2.Filter
        filter = FilterDescription(filter=workflow['filter'])
        filter_step = StepDescription(
            operator=operator_pb2.OperatorDescription(filter=filter),
            inputs=[StepInputDescription(type=InputType.Value('DATA'), index=0)])
        job.steps.extend([filter_step])
        has_filter = True

    # binning
    BinningDescription = operators.binning_pb2.Binning
    BinningDimension = BinningDescription.BinningDimension
    dimensions = []
    group_by_columns = []
    for binning in workflow['binning']:
        dimension = BinningDimension(column=binning['dimension'],
                                     num_bins=int(binning['width']),
                                     binned_column='{}_bin'.format(binning['dimension']))
        dimensions.append(dimension)
        group_by_columns.append('{}_bin'.format(binning['dimension']))
    binning = BinningDescription(dimensions=dimensions)

    if has_filter:
        input_type = InputType.Value('STEP')
    else:
        input_type = InputType.Value('DATA')
    binning_step = StepDescription(
        operator=operator_pb2.OperatorDescription(binning=binning),
        inputs=[StepInputDescription(type=input_type, index=0)])
    job.steps.extend([binning_step])

    # aggregation
    AggregationDescription = operators.aggregation_pb2.Aggregation
    AggregationDimension = AggregationDescription.AggregationDimension
    AggregationMethod = AggregationDescription.AggregationMethod

    aggregation = AggregationDescription(
        dimensions=[AggregationDimension(column='height', method=AggregationMethod.Value('AVERAGE_WITH_CI'),
                                         aggregation_column='height_aggregation'),
                    AggregationDimension(column='sex', method=AggregationMethod.Value('COUNT_WITH_CI'),
                                         aggregation_column='sex_aggregation')],
        group_by_columns=group_by_columns)

    dimensions = []
    for per_bin_aggregate in workflow['perBinAggregates']:
        if per_bin_aggregate['type'] == 'count':
            dimension = AggregationDimension(column=workflow['binning'][0]['dimension'],
                                             method=AggregationMethod.Value('COUNT_WITH_CI'),
                                             aggregation_column=workflow['binning'][0]['dimension'])
        else:
            assert per_bin_aggregate['type'] == 'avg'
            dimension = AggregationDimension(column=per_bin_aggregate['dimension'],
                                             method=AggregationMethod.Value('AVERAGE_WITH_CI'),
                                             aggregation_column=per_bin_aggregate['dimension'])
        dimensions.append(dimension)
    aggregation = AggregationDescription(dimensions=dimensions,
                                         group_by_columns=group_by_columns)

    if has_filter:
        input_index = 0
    else:
        input_index = 1
    aggregation_step = StepDescription(
        operator=operator_pb2.OperatorDescription(aggregation=aggregation),
        inputs=[StepInputDescription(type=InputType.Value('STEP'), index=input_index)])
    job.steps.extend([aggregation_step])

    return job


if __name__ == "__main__":
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    # jobs
    with open(input_path, 'r') as f:
        workflows = json.load(f)
    jobs = []
    viz = {}
    for workflow in workflows['interactions']:
        if workflow['name'] not in viz:
            viz[workflow['name']] = workflow
        else:
            workflow = {**workflow, **viz[workflow['name']]}
        job = workflow_to_job(workflow)
        jobs.append(MessageToJson(job))

    with open(output_path, 'w') as f:
        json.dump(jobs, f)
