from dagster import Definitions, asset, define_asset_job, ScheduleDefinition
# from pendulum import datetime
import scripts.extract_load as extract_load

dv3f_instance = extract_load.dv3f()

@asset
def extract():
    return dv3f_instance.get_data(annee=2022, scope='dep', code='59')
    
@asset
def transform(extract):
    return dv3f_instance.transform_data(extract)
    
@asset
def load(transform):
    dv3f_instance.load_data(transform)
    return None

etl_job = define_asset_job(name='etl_job')

defs = Definitions(
    assets=[extract,transform,load],
    jobs=[etl_job]
)

basic_schedule = ScheduleDefinition(job=etl_job, cron_schedule="@monthly")