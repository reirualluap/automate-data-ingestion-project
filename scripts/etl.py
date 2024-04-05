from dagster import asset, AssetMaterialization
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

# @pipeline(mode_defs=[ModeDefinition(resource_defs={})])
# def etl_pipeline():
#     loaded_data = load_data(transform_data(extract_data()))

# Execute the pipeline
# if __name__ == "__main__":
#     result = execute_pipeline(etl_pipeline, tags={"dev_test"})
