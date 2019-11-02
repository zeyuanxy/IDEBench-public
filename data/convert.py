import sys
import json

from google.protobuf.json_format import MessageToJson
from davos.api import job_pb2, step_pb2, operators, operator_pb2, data_source_pb2, data_pb2



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
    next_input = StepInputDescription(type=InputType.Value('DATA'), index=0)
    if 'filter' in workflow:
        FilterDescription = operators.filter_pb2.Filter
        filter_str = workflow['filter'].replace(' = ', ' == ')
        filter = FilterDescription(filter=filter_str)
        filter_step = StepDescription(
            operator=operator_pb2.OperatorDescription(filter=filter),
            inputs=[next_input])
        job.steps.extend([filter_step])
        next_input = StepInputDescription(type=InputType.Value('STEP'), index=0)

    # binning
    BinningDescription = operators.binning_pb2.Binning
    BinningDimension = BinningDescription.BinningDimension
    dimensions = []
    group_by_columns = []
    for binning in workflow['binning']:
        if 'width' in binning:
            width = float(binning['width'])
            tensor = operators.base_pb2.Tensor()
            tensor.dim.extend([3])
            tensor.type = data_pb2.DataType.Value('FLOAT')
            tensor.is_null.extend([True, True, False])
            tensor.float_value.extend([0, 0, width])

            dimension = BinningDimension(column=binning['dimension'],
                                         range=tensor,
                                         binned_column='{}_bin'.format(binning['dimension']))
            dimensions.append(dimension)
            group_by_columns.append('{}_bin'.format(binning['dimension']))
        else:
            group_by_columns.append(binning['dimension'])

    if dimensions:
        binning = BinningDescription(dimensions=dimensions)

        binning_step = StepDescription(
            operator=operator_pb2.OperatorDescription(binning=binning),
            inputs=[next_input])
        job.steps.extend([binning_step])

        if next_input.type == InputType.Value('DATA'):
            next_input.type = InputType.Value('STEP')
            next_input.index = 0
        else:
            next_input.index += 1

    # aggregation
    AggregationDescription = operators.aggregation_pb2.Aggregation
    AggregationDimension = AggregationDescription.AggregationDimension
    AggregationMethod = AggregationDescription.AggregationMethod

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

    aggregation_step = StepDescription(
        operator=operator_pb2.OperatorDescription(aggregation=aggregation),
        inputs=[next_input])
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
