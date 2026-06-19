import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

# Dossier utilisé comme source de données pour Spark.
# Le générateur va ajouter des fichiers JSON dedans en continu.
dossier_flux = Path("stream_data")
dossier_flux.mkdir(exist_ok=True)

# Quelques valeurs possibles pour rendre les événements plus réalistes.
villes = [
    "Paris",
    "Lyon",
    "Marseille",
    "Lille",
    "Bordeaux",
    "Toulouse",
    "Cergy",
    "Nanterre",
    "Nice",
    "Rennes"
]

categories_produits = [
    "Vehicules",
    "Immobilier",
    "Electronique",
    "Maison",
    "Mode",
    "Sport",
    "Loisirs"
]

# Les trois actions demandées dans le sujet.
# VOUT correspond à une intention d'achat.
types_actions = [
    "AIME",
    "VOUT",
    "ACHAT"
]

print("Générateur lancé.")
print("Un nouvel événement est créé toutes les secondes.")
print("Appuyer sur CTRL + C pour arrêter le programme.")

while True:
    evenement = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"usr_{random.randint(1, 50)}",
        "user_city": random.choice(villes),
        "product_id": f"prod_{random.randint(1, 30)}",
        "product_cat": random.choice(categories_produits),
        "seller_id": f"sel_{random.randint(1, 15)}",
        "action_type": random.choice(types_actions),
        "price": round(random.uniform(10, 2000), 2)
    }

    # On crée un nom unique pour éviter d'écraser les anciens événements.
    nom_fichier = dossier_flux / f"event_{time.time_ns()}.json"

    with open(nom_fichier, "w", encoding="utf-8") as fichier:
        json.dump(evenement, fichier)

    print("Événement généré :", evenement)

    time.sleep(1)