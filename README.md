# Projet_spark_data

Projet réalisé dans le cadre du module Spark Data.

L'objectif du projet est de simuler une plateforme de petites annonces de type LeBonCoin.
Des utilisateurs interagissent en continu avec des produits proposés par des vendeurs. Ces interactions sont ensuite traitées en temps réel avec PySpark, puis représentées sous forme de graphe dynamique.

## Auteurs

* William Church
* Axel EDOUARD
* Abderrahmane Zerargui

## Objectif du projet

Le projet permet de créer un flux continu d'événements représentant des actions réalisées sur une plateforme commerciale.

Chaque événement contient plusieurs informations :

* l'identifiant de l'utilisateur ;
* la ville de l'utilisateur ;
* l'identifiant du produit ;
* la catégorie du produit ;
* l'identifiant du vendeur ;
* le type d'action réalisée ;
* le prix du produit ;
* la date et l'heure de l'événement.

Les actions possibles sont :

* `AIME` : l'utilisateur aime un produit ;
* `VOUT` : l'utilisateur montre une intention d'achat ;
* `ACHAT` : l'utilisateur achète un produit ;
* `PROPOSE` : un vendeur propose un produit.

## Structure du projet

```text
Projet_spark_data/
│
├── data_generator.py
├── spark_streaming_graph.py
├── dashboard.py
├── README.md
└── .gitignore
```

## Description des fichiers

### `data_generator.py`

Ce fichier génère automatiquement des événements au format JSON.

Les événements sont créés en continu et enregistrés dans le dossier `stream_data`.
Ce dossier sert ensuite de source de données pour le traitement Spark.

### `spark_streaming_graph.py`

Ce fichier contient le traitement principal du projet.

Il permet de :

* lire les événements JSON en streaming ;
* appliquer un schéma strict aux données ;
* gérer le temps des événements ;
* utiliser un watermark ;
* agréger les actions par fenêtres temporelles ;
* construire les sommets du graphe ;
* construire les arêtes du graphe ;
* utiliser GraphFrames pour calculer les degrés des nœuds ;
* écrire les résultats dans le dossier `dashboard_data`.

Les sommets du graphe représentent :

* les utilisateurs ;
* les produits ;
* les vendeurs.

Les arêtes représentent les interactions entre ces entités.

### `dashboard.py`

Ce fichier lance une interface graphique avec Streamlit.

Le dashboard affiche :

* le nombre d'utilisateurs ;
* le nombre de produits ;
* le nombre de vendeurs ;
* le nombre total d'interactions ;
* un graphe dynamique représentant les relations entre utilisateurs, produits et vendeurs.

Le graphe est affiché avec NetworkX afin d'obtenir une visualisation claire dans le dashboard.

## Fonctionnement général

Le projet fonctionne en trois étapes :

```text
Générateur JSON
        ↓
PySpark Structured Streaming
        ↓
Dashboard dynamique
```

Le générateur crée des données en continu.
Spark lit ces données sous forme de flux, les transforme et construit les informations nécessaires au graphe.
Le dashboard lit ensuite les résultats produits par Spark et affiche les données sous forme visuelle.

## Lancement du projet

Le projet doit être lancé avec trois terminaux différents.

### Terminal 1 : générateur de données

```bash
python3 data_generator.py
```

Ce terminal génère les événements JSON en continu.

### Terminal 2 : traitement Spark

```bash
python3 spark_streaming_graph.py
```

Ce terminal lance le traitement PySpark Structured Streaming.

### Terminal 3 : dashboard

```bash
python3 -m streamlit run dashboard.py
```

Une fois Streamlit lancé, il faut ouvrir l'adresse suivante dans un navigateur :

```text
http://localhost:8501
```

## Nettoyer les anciennes données

Avant de relancer un test propre, il est possible de supprimer les anciens fichiers générés :

```bash
rm -rf stream_data
rm -rf dashboard_data
mkdir stream_data
```

Ensuite, il suffit de relancer les trois fichiers dans l'ordre.

## Exemple d'événement généré

```json
{
  "timestamp": "2026-06-19T15:30:00+00:00",
  "user_id": "usr_12",
  "user_city": "Paris",
  "product_id": "prod_8",
  "product_cat": "Electronique",
  "seller_id": "sel_3",
  "action_type": "AIME",
  "price": 249.99
}
```

## Remarque sur le graphe

Le flux peut générer beaucoup d'interactions.
Pour garder un affichage lisible, le dashboard affiche seulement une partie des arêtes dans le graphe.
Les compteurs, eux, prennent bien en compte toutes les données lues par le dashboard.
