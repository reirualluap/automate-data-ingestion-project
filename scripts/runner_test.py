from extract_load import dv3f

new_instance = dv3f()

new_instance.get_data(scope="dep", code=59, annee=2022)
new_instance.transform_data()
new_instance.load_data()