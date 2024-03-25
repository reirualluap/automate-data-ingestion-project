# import logging
import requests
from loguru import logger
import pandas as pd
import duckdb
from hydra import compose, initialize
from omegaconf import OmegaConf
import hashlib

# https://hydra.cc/docs/intro/

# import dlt
# https://dlthub.com/docs/intro
# https://dlthub.com/docs/dlt-ecosystem/destinations/duckdb
# https://duckdb.org/docs/installation/?version=latest&environment=python

with initialize(version_base=None, config_path="config", job_name="pipeline"):
    cfg = compose(config_name="hydra.yaml")

logger.add(f"{cfg.log.path}/{cfg.log.name}")

class dv3f():
    def __init__(self):
        """
        Initializes the object and sets up logging configuration.
        
        This method configures the logging system with basic settings, including log level,
        output file, and format. It initializes a logger object for use within the class.
        """
    
    def get_data(self, annee=None, scope=None,coddep=None, codreg=None, **kwargs):
        """
        Retrieves data from an API endpoint based on specified parameters.
        
        Args:
            annee (int, optional): The year for which data is to be retrieved.
            scope (str, optional): The scope of the data, either 'region' or 'departement'.
            coddep (str, optional): The department code.
            codreg (str, optional): The region code.
            **kwargs: Additional keyword arguments for filtering and pagination.

        Raises:
            ValueError: If an invalid scope value is provided.

        Returns:
            dict: The retrieved data from the API endpoint.

        This method constructs the API endpoint URL based on the provided scope and codes,
        sends a GET request to the endpoint with optional query parameters, and processes
        the response. It logs relevant information such as the endpoint URL, response status,
        and the number of results received.
        """
        self.kwargs = kwargs
        self.scope = scope
        self.annee = annee
        self.coddep = coddep
        self.codreg = codreg
        self.api_endpoint = None
 
        if self.scope in ["region","reg"]:
            self.api_endpoint = f"https://apidf-preprod.cerema.fr/indicateurs/dv3f/regions/annuel/{self.codreg}/"
        elif self.scope in ["departement","dep"]:
            self.api_endpoint = f"https://apidf-preprod.cerema.fr/indicateurs/dv3f/departements/annuel/{self.coddep}/"
        else:
            raise ValueError("Invalid scope value. Valid values are 'region' or 'departement'.")
        logger.info(f"Annee : {self.annee}, Scope: {self.scope}, Kwargs: {kwargs}")

        self.params = {
            'annee': self.annee,
            'ordering': kwargs.get("ordering"),
            'page': kwargs.get("page"),
            'page_size': kwargs.get("page_size")
        }

        self.params = {key: value for key, value in self.params.items() if value}
        response = requests.get(self.api_endpoint, params=self.params)
        logger.info(f"api_endpoint : {response.url} , Response : {response.status_code}, Response_nb : {response.json()['count']}")

        if response.status_code == 200:
            nb_results = len(response.json()['results'])
            if nb_results == 0:
                raise BaseException("La requête a abouti mais le contenu est vide")
            else: 
                data = response.json()
                if nb_results == 1:
                    print(f"{nb_results} résultat a été trouvé")
                else:
                    print(f"{nb_results} résultats ont été trouvés")

                self.data = pd.json_normalize(data["results"])
        else:
            print("La requête a échoué avec le code de statut:", response.status_code)

    def __repr__(self):
        if 'data' in dir(self):
           return self.data
        else:
            raise ValueError("The object is empty. Cannot process empty data, please use get_data() method first.")
            
    def __str__(self):
        if 'data' in dir(self):
           return str(self.data.head(5))
        else:
            raise ValueError("The object is empty. Cannot process empty data, please use get_data() method first.")
    
    def transform_data(self):
        logger.info("Starting transform task")
        
        m = hashlib.sha256()
        df = (self.data)

        # Liste des colonnes à garder inchangées
        id_vars = ['annee', 'dep', 'libdep']

        # Réorganiser les données en utilisant melt et extraire le suffixe
        df_melted = df.melt(id_vars=id_vars, var_name='cod_full', value_name='valeur')

        # Extraire le suffixe de la colonne cod_full
        df_melted[['cod_full','cod']] = df_melted['cod_full'].str.rsplit('_', n=1, expand=True)
        df_melted['cod'] = df_melted.apply(lambda row: row['cod'].replace('cod',''), axis=1)

        # Pivoter la colonne cod_full
        df_pivoted = df_melted.pivot_table(index=id_vars + ['cod'], columns='cod_full', values='valeur', aggfunc='first').reset_index()

        # Ajout d'une clé de déduplication (UID)
        df_pivoted['uid'] = df_pivoted.apply(
            lambda row: hashlib.sha256(
                str(row['annee']).encode('utf-8') + 
                str(row['dep']).encode('utf-8') + 
                str(row['cod']).encode('utf-8')
            ).hexdigest(), 
            axis=1
        )

        # Renommer les colonnes si nécessaire
        df_pivoted.columns.name = None

        self.data = df_pivoted
        logger.info("Transform task ended")

    def test(self):
        # print(f"{cfg.schema.columns.keys()}")
        # print(f"""CREATE TABLE IF NOT EXISTS {cfg.db.schema_name}.{cfg.db.tables.staging} ({', '.join([f"{key} {value.type}" for key, value in cfg.schema.columns.items()])}, PRIMARY KEY ({', '.join(cfg.schema.primary_keys)}));""")
        pass

    ## Add an assert test to match schema, this to avoid to create/insert data that not match transformed
    def load_data(self):
        logger.info("Starting load task")

        with duckdb.connect(f"data/{cfg.db.db_name}.db") as con:
            insertion_table = self.data
            logger.info(f"Using {cfg.db.schema_name}.{cfg.db.tables.staging} to insert data")
            con.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.db.schema_name}")

            try:
                con_obj = con.table(f"{cfg.db.schema_name}.{cfg.db.tables.staging}")
                logger.success(f"Table {cfg.db.schema_name}.{cfg.db.tables.staging} exists")
            except Exception as e:
                logger.warning(e)
                logger.info(f"Creating new table as {cfg.db.schema_name}.{cfg.db.tables.staging}")
                con.sql(f"""CREATE TABLE IF NOT EXISTS {cfg.db.schema_name}.{cfg.db.tables.staging} ({', '.join([f"{key} {value.type}" for key, value in cfg.db.tables.staging.schema.columns.items()])}, PRIMARY KEY ({', '.join(cfg.db.tables.staging.schema.primary_keys)}));""")
                logger.success(f"{cfg.db.schema_name}.{cfg.db.tables.staging} created")
                
            ### Use Ruff as a linter
                
            try:
                logger.info(f"Inserting into {cfg.db.schema_name}.{cfg.db.tables.staging}")
                con.sql(f"INSERT OR REPLACE INTO {cfg.db.schema_name}.{cfg.db.tables.staging} BY NAME SELECT * FROM insertion_table;")
                logger.info(f"Load task ended")
            except Exception as e:
                logger.error(e)
            
            try:
                con.sql("SELECT estimated_size,column_count,index_count FROM duckdb_tables();").show()
            except Exception as e:
                logger.error(e)

# def pipeline():


# if __name__ == "main":
#     pipeline()

# new_dv = dv3f()
# new_dv.get_data(scope="dep", coddep=59)
# new_dv.load_data()
# print(new_dv)

# Switch coddep&codreg into code or raise error if scope=dep and correg
            

            