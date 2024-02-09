import requests
import json

region_code = 44
annee = 2022
ordering = None
page = None
page_size = None

# URL de l'API
url = f"https://apidf-preprod.cerema.fr/indicateurs/dv3f/regions/annuel/{region_code}/"

# Paramètres de la requête (si nécessaire)
params = {
    'annee': str(annee),
    'ordering': str(ordering),
    'page':page,
    'page_size':page
}

params = {key: value for key, value in params.items() if value}

# Envoi de la requête GET à l'API
response = requests.get(url, params=params)

# Vérification du code de statut HTTP
if response.status_code == 200:
    nb_results = len(response.json()['results'])
    if nb_results == 0:
        raise BaseException("La requête a abouti mais le contenu est vide")
    # Conversion de la réponse JSON en dictionnaire Python
    else: 
        data = response.json()
        if nb_results == 1:
            print(f"{nb_results} résultat a été trouvé")
        else:
            print(f"{nb_results} résultats ont été trouvés")
    # Enregistrement des résultats dans un fichier
        with open('results.json', 'w') as f:
            json.dump(data["results"], f, indent=4)
        
        print("Les résultats ont été enregistrés dans le fichier 'results.json'.")
else:
    print("La requête a échoué avec le code de statut:", response.status_code)



