from dagster import op, job, graph, In, GraphIn, Out, DynamicOut, DynamicOutput
from hydra import compose, initialize
import scripts.extract_load as extract_load
from loguru import logger


with initialize(version_base=None, config_path="config", job_name="pipeline"):
    cfg = compose(config_name="hydra.yaml")

dv3f_instance = extract_load.dv3f()

@op(out={"scope_code": DynamicOut()})
def generate_dynamic_args():
    for scope, codes in cfg.args.scope.items():
        for code in codes:
            logger.info(f"In generate_dynamic_args @op : scope is {scope} and code is {code}")
            yield DynamicOutput((scope, code), output_name="scope_code", mapping_key=f"{scope}_{code}")

@op
def extract(scope_code):
    scope, code = scope_code
    return dv3f_instance.get_data(scope=scope, code=code, scope_code=scope_code)
    
@op
def transform(extract):
    data,scope_code = extract
    return dv3f_instance.transform_data(data=data,scope_code=scope_code)
    
@op
def load(transform):
    data, scope_code = transform
    dv3f_instance.load_data(data=data,scope_code=scope_code)

@graph(ins={"scope_code": GraphIn()}) 
def etl_graph(scope_code):
    load(transform(extract(scope_code)))

@job
def etl_job():
    dynamic_results = generate_dynamic_args()
    dynamic_results.map(etl_graph)

# ### Create a dict and a job
# etl_job = define_asset_job(name='etl_job')

# defs = Definitions(
#     assets=[extract,transform,load],
#     jobs=[etl_job]
# )

# basic_schedule = ScheduleDefinition(job=etl_job, cron_schedule="@monthly")