import glob
import time

import matplotlib.pyplot as plt
import networkx as nx
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Dashboard LeBonCoin", layout="wide")

st.title("Dashboard dynamique - Graphe LeBonCoin")


def lire_csv_du_dossier(dossier):
    """
    Spark écrit plusieurs fichiers CSV dans un dossier.
    Cette fonction les lit tous puis les regroupe dans un seul DataFrame.
    """
    fichiers = glob.glob(f"{dossier}/part-*.csv")

    if not fichiers:
        return pd.DataFrame()

    dataframes = []

    for fichier in fichiers:
        try:
            dataframes.append(pd.read_csv(fichier))
        except Exception:
            # Si un fichier est en cours d'écriture par Spark, on l'ignore.
            pass

    if not dataframes:
        return pd.DataFrame()

    return pd.concat(dataframes, ignore_index=True)


zone_dashboard = st.empty()

while True:
    with zone_dashboard.container():
        sommets = lire_csv_du_dossier("dashboard_data/vertices")
        aretes = lire_csv_du_dossier("dashboard_data/edges")

        if sommets.empty or aretes.empty:
            st.warning("En attente des données Spark...")
        else:
            # Un même sommet peut être présent dans plusieurs batchs.
            sommets = sommets.drop_duplicates(subset=["id"])

            nb_utilisateurs = len(sommets[sommets["type"] == "USER"])
            nb_produits = len(sommets[sommets["type"] == "PRODUCT"])
            nb_vendeurs = len(sommets[sommets["type"] == "SELLER"])
            nb_interactions = len(aretes)

            col1, col2, col3, col4 = st.columns(4)

            col1.metric("Utilisateurs", nb_utilisateurs)
            col2.metric("Produits", nb_produits)
            col3.metric("Vendeurs", nb_vendeurs)
            col4.metric("Interactions", nb_interactions)

            st.markdown("""
            **Légende :**  
            🔵 Utilisateur  
            🟢 Produit  
            🔴 Vendeur
            """)

            st.subheader("Graphe des interactions")

            # On limite l'affichage pour éviter un graphe trop illisible.
            # Le reste des données est quand même comptabilisé dans les métriques.
            nombre_aretes_affichees = 250

            if len(aretes) > nombre_aretes_affichees:
                aretes_affichees = aretes.sample(
                    n=nombre_aretes_affichees,
                    random_state=42
                )
            else:
                aretes_affichees = aretes

            ids_sommets_utiles = set(aretes_affichees["src"]).union(set(aretes_affichees["dst"]))
            sommets_affiches = sommets[sommets["id"].isin(ids_sommets_utiles)]

            graphe = nx.DiGraph()

            for _, ligne in sommets_affiches.iterrows():
                graphe.add_node(
                    ligne["id"],
                    type=ligne["type"]
                )

            for _, ligne in aretes_affichees.iterrows():
                graphe.add_edge(
                    ligne["src"],
                    ligne["dst"],
                    label=ligne["relationship"]
                )

            fig, ax = plt.subplots(figsize=(18, 12))

            positions = nx.spring_layout(
                graphe,
                seed=42,
                k=1.2
            )

            couleurs_sommets = []

            for sommet in graphe.nodes():
                type_sommet = graphe.nodes[sommet].get("type")

                if type_sommet == "USER":
                    couleurs_sommets.append("skyblue")
                elif type_sommet == "PRODUCT":
                    couleurs_sommets.append("lightgreen")
                elif type_sommet == "SELLER":
                    couleurs_sommets.append("salmon")
                else:
                    couleurs_sommets.append("gray")

            nx.draw_networkx_nodes(
                graphe,
                positions,
                node_color=couleurs_sommets,
                node_size=700,
                ax=ax
            )

            nx.draw_networkx_edges(
                graphe,
                positions,
                arrows=True,
                alpha=0.25,
                ax=ax
            )

            nx.draw_networkx_labels(
                graphe,
                positions,
                font_size=7,
                ax=ax
            )

            etiquettes_aretes = nx.get_edge_attributes(graphe, "label")

            nx.draw_networkx_edge_labels(
                graphe,
                positions,
                edge_labels=etiquettes_aretes,
                font_size=5,
                ax=ax
            )

            ax.axis("off")

            st.pyplot(fig)

            st.subheader("Sommets affichés")
            st.dataframe(sommets_affiches)

            st.subheader("Arêtes affichées")
            st.dataframe(aretes_affichees)

    time.sleep(5)