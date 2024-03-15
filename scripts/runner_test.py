from extract_load import dv3f

new_instance = dv3f()
# new_instance.get_data()
# new_instance.print()
new_instance.get_data(scope="dep", coddep=59)
# print(new_instance)
# print(new_instance.__repr__().head(5))
new_instance.load_data()