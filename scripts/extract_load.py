import logging
import requests
import json
import duckdb
from hydra import compose, initialize
from omegaconf import OmegaConf

# https://hydra.cc/docs/intro/

# import dlt
# https://dlthub.com/docs/intro
# https://dlthub.com/docs/dlt-ecosystem/destinations/duckdb
# https://duckdb.org/docs/installation/?version=latest&environment=python

with initialize(version_base=None, config_path="config", job_name="pipeline"):
    cfg = compose(config_name="hydra.yaml")

class dv3f():
    def __init__(self):
        """
        Initializes the object and sets up logging configuration.
        
        This method configures the logging system with basic settings, including log level,
        output file, and format. It initializes a logger object for use within the class.
        """
        logging.basicConfig(level=logging.INFO, filename='log/dv3f.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
    
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
        self.logger.info(f"Annee : {self.annee}, Scope: {self.scope}, Kwargs: {kwargs}")

        self.params = {
            'annee': self.annee,
            'ordering': kwargs.get("ordering"),
            'page': kwargs.get("page"),
            'page_size': kwargs.get("page_size")
        }

        self.params = {key: value for key, value in self.params.items() if value}
        response = requests.get(self.api_endpoint, params=self.params)
        self.logger.info(f"api_endpoint : {response.url} , Response : {response.status_code}, Response_nb : {response.json()['count']}")

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

                self.data = data["results"]
        else:
            print("La requête a échoué avec le code de statut:", response.status_code)

    def __repr__(self):
        if self.data: 
            return json.dumps(self.data)
        else:
            print("No data yet, use get_data method first")

    def load_data(self):
        self.logger.info(f"Starting load task")
        with duckdb.connect(f"data/{cfg.db.db_name}.db") as con:
            # con.sql("RESET LOCAL home_directory")
            con.sql("RESET LOCAL extension_directory")
            con.sql(f"CREATE TABLE IF NOT EXISTS {cfg.db.table_name} (i INTEGER)")
            self.logger.info(f"{cfg.db.table_name}")
            con.sql(f"INSERT INTO {cfg.db.table_name} VALUES (12)")
            con_obj = con.table(f"{cfg.db.table_name}").show()
            self.logger.info(f"Table :{con_obj}")


# def pipeline():


# if __name__ == "main":
#     pipeline()

# new_dv = dv3f()
# new_dv.get_data(scope="dep", coddep=59)
# new_dv.load_data()
# print(new_dv)

# Switch coddep&codreg into code or raise error if scope=dep and correg