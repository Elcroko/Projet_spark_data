from pathlib import Path

from graphframes import GraphFrame

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)
from pyspark.sql.functions import (
    col,
    to_timestamp,
    window,
    lit
)

# Création des dossiers utilisés par le projet.
Path("stream_data").mkdir(exist_ok=True)
Path("dashboard_data").mkdir(exist_ok=True)

# Création de la session Spark.
# La configuration spark.sql.shuffle.partitions permet d'éviter d'avoir trop
# de partitions pour un projet lancé en local.
spark = SparkSession.builder \
    .appName("ProjetStreamingLeBonCoin") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.jars.packages", "io.graphframes:graphframes-spark4_2.13:0.12.1") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

# Schéma imposé pour éviter que Spark essaye de deviner les types.
# C'est plus propre et plus efficace en streaming.
schema_evenement = StructType([
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_city", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_cat", StringType(), True),
    StructField("seller_id", StringType(), True),
    StructField("action_type", StringType(), True),
    StructField("price", DoubleType(), True)
])

# Lecture du flux JSON.
# maxFilesPerTrigger limite le nombre de fichiers lus à chaque micro-batch.
flux_json = spark.readStream \
    .schema(schema_evenement) \
    .option("maxFilesPerTrigger", 20) \
    .json("stream_data")

# Conversion du timestamp texte en timestamp Spark.
evenements = flux_json.withColumn(
    "event_time",
    to_timestamp(col("timestamp"))
)

# Watermark : utile pour gérer les événements qui arriveraient en retard.
evenements_watermark = evenements.withWatermark(
    "event_time",
    "1 minute"
)

# Statistiques par fenêtre de temps.
# Ici on compte le nombre de AIME, VOUT et ACHAT toutes les 30 secondes.
stats_actions = evenements_watermark.groupBy(
    window(col("event_time"), "30 seconds"),
    col("action_type")
).count()

# Affichage des statistiques dans le terminal.
requete_stats = stats_actions.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()


def construire_graphe(batch_df, numero_batch):
    print("\n==============================")
    print(f"MICRO-BATCH {numero_batch}")
    print("==============================")

    if batch_df.count() == 0:
        print("Aucune donnée reçue dans ce batch.")
        return

    # ----------------------------
    # 1) Création des sommets
    # ----------------------------
    # GraphFrames impose une colonne nommée 'id' pour les sommets.
    utilisateurs = batch_df.select(
        col("user_id").alias("id")
    ).distinct().withColumn(
        "type",
        lit("USER")
    )

    produits = batch_df.select(
        col("product_id").alias("id")
    ).distinct().withColumn(
        "type",
        lit("PRODUCT")
    )

    vendeurs = batch_df.select(
        col("seller_id").alias("id")
    ).distinct().withColumn(
        "type",
        lit("SELLER")
    )

    sommets = utilisateurs.union(produits).union(vendeurs)

    # ----------------------------
    # 2) Création des arêtes
    # ----------------------------
    # GraphFrames impose les colonnes 'src' et 'dst' pour les arêtes.
    aretes_utilisateur_produit = batch_df.select(
        col("user_id").alias("src"),
        col("product_id").alias("dst"),
        col("action_type").alias("relationship")
    )

    aretes_vendeur_produit = batch_df.select(
        col("seller_id").alias("src"),
        col("product_id").alias("dst")
    ).withColumn(
        "relationship",
        lit("PROPOSE")
    )

    aretes = aretes_utilisateur_produit.union(aretes_vendeur_produit)

    # ----------------------------
    # 3) Analyse avec GraphFrames
    # ----------------------------
    graphe = GraphFrame(sommets, aretes)

    print("Degrés calculés avec GraphFrames :")
    graphe.degrees.show(20, truncate=False)

    print("Sommets du graphe :")
    sommets.show(20, truncate=False)

    print("Arêtes du graphe :")
    aretes.show(20, truncate=False)

    # Deux petits calculs simples pour expliquer la centralité.
    print("Degré entrant : produits les plus reliés")
    degres_entrants = aretes.groupBy("dst").count().withColumnRenamed("count", "in_degree")
    degres_entrants.show(20, truncate=False)

    print("Degré sortant : utilisateurs/vendeurs les plus actifs")
    degres_sortants = aretes.groupBy("src").count().withColumnRenamed("count", "out_degree")
    degres_sortants.show(20, truncate=False)

    # ----------------------------
    # 4) Écriture pour le dashboard
    # ----------------------------
    # On utilise append pour garder l'historique des batchs.
    sommets.coalesce(1).write \
        .mode("append") \
        .option("header", "true") \
        .csv("dashboard_data/vertices")

    aretes.coalesce(1).write \
        .mode("append") \
        .option("header", "true") \
        .csv("dashboard_data/edges")


# foreachBatch permet de traiter chaque micro-batch avec du code Python classique.
requete_graphe = evenements_watermark.writeStream \
    .foreachBatch(construire_graphe) \
    .outputMode("append") \
    .start()

requete_graphe.awaitTermination()
requete_stats.awaitTermination()