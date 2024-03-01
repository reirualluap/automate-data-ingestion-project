import logging
import requests
import json
import duckdb

# import hydra
# https://hydra.cc/docs/intro/

# import dlt
# https://dlthub.com/docs/intro
# https://dlthub.com/docs/dlt-ecosystem/destinations/duckdb


# https://duckdb.org/docs/installation/?version=latest&environment=python

class dv3f():
    def __init__(self):
        logging.basicConfig(level=logging.INFO, filename='dv3f.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

    def get_data(self, annee=None, scope=None,coddep=None, codreg=None, **kwargs):
        
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
            # self.logger.info(f"nb_results : {nb_results}")
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
        # return data["results"]
        ### voir comment retourner les résultats dans l'object
    def __repr__(self):
        return json.dumps(self.data)
    def load_data(self):
        self.logger.info(f"Starting load task")
        with duckdb.connect("dv3f.db") as con:
            con.sql("CREATE TABLE test_raw (i INTEGER)")
            con.insert("INSERT INTO test_raw VALUES (12)")
            con_obj = con.table("test_raw").show()
            self.logger.info(f"Table :{con_obj}")


new_dv = dv3f()
# new_dv.get_data(scope="dep", coddep=59)
new_dv.load_data()
print(new_dv)

# Switch coddep&codreg into code or raise error if scope=dep and correg