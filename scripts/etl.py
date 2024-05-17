from dagster import op, job, graph, GraphIn, DynamicOut, DynamicOutput
from hydra import compose, initialize
import extract_load as extract_load
from loguru import logger


with initialize(version_base=None, config_path="config", job_name="pipeline"):
    cfg = compose(config_name="hydra.yaml")

dv3f_instance = extract_load.dv3f()


@op(out={"scope_code": DynamicOut()})
def generate_dynamic_args():
    for scope, codes in cfg.args.scope.items():
        for code in codes:
            logger.info(
                f"In generate_dynamic_args @op : scope is {scope} and code is {code}"
            )
            yield DynamicOutput(
                (scope, code), output_name="scope_code", mapping_key=f"{scope}_{code}"
            )


@op
def extract(scope_code):
    try:
        scope, code = scope_code
        return dv3f_instance.get_data(scope=scope, code=code, scope_code=scope_code)
    except Exception as e:
        logger.error(f"Error in extract op for scope_code {scope_code}: {e}")
        return None  # Return None or a sentinel value to indicate failure


@op
def transform(extract):
    if extract is None:
        return None  # Skip transform if extract failed
    data, scope_code = extract
    try:
        return dv3f_instance.transform_data(data=data, scope_code=scope_code)
    except Exception as e:
        logger.error(f"Error in transform op for scope_code {scope_code}: {e}")
        return None


@op
def load(transform):
    if transform is None:
        return  # Skip load if transform failed
    data, scope_code = transform
    try:
        dv3f_instance.load_data(data=data, scope_code=scope_code)
    except Exception as e:
        logger.error(f"Error in load op for scope_code {scope_code}: {e}")


@graph(ins={"scope_code": GraphIn()})
def etl_graph(scope_code):
    load(transform(extract(scope_code)))


@job
def etl_job():
    dynamic_results = generate_dynamic_args()
    dynamic_results.map(etl_graph)


if __name__ == "__main__":
    result = etl_job.execute_in_process()