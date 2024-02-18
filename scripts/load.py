from extract import dv3f
# import dlt
# https://dlthub.com/docs/intro
# https://dlthub.com/docs/dlt-ecosystem/destinations/duckdb

# import duckdb
# https://duckdb.org/docs/installation/?version=latest&environment=python

new_dv = dv3f()
new_dv.get_data(scope="dep", coddep=59)

print(new_dv)

