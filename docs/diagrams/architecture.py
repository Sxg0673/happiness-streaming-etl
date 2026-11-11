"""Diagrama de arquitectura del proyecto (docs/img/architecture.png).

Requiere la biblioteca `diagrams` y Graphviz, que no forman parte del pipeline:
    pip install diagrams      # y Graphviz: brew install graphviz
    python docs/diagrams/architecture.py
"""
from pathlib import Path

from diagrams import Cluster, Diagram, Edge
from diagrams.programming.flowchart import Database, Document, MultipleDocuments
from diagrams.onprem.analytics import Tableau
from diagrams.onprem.database import Postgresql
from diagrams.onprem.queue import Kafka
from diagrams.programming.language import Python

OUT = Path(__file__).resolve().parents[1] / "img" / "architecture"
WINE, TEAL, INK, MUTED = "#8e2c48", "#2b7686", "#2a1e22", "#6a5a5f"

graph = {"fontname": "Helvetica", "fontsize": "22", "fontcolor": INK, "bgcolor": "white",
         "pad": "0.4", "nodesep": "0.6", "ranksep": "1.1", "labelloc": "t"}
node = {"fontname": "Helvetica", "fontsize": "13", "fontcolor": INK}
edge = {"fontname": "Helvetica", "fontsize": "11", "fontcolor": MUTED, "color": MUTED, "penwidth": "1.4"}


def box(color, fill):
    return {"fontname": "Helvetica-Bold", "fontsize": "15", "fontcolor": color, "bgcolor": fill,
            "pencolor": color, "penwidth": "1.5", "style": "rounded", "margin": "30"}


with Diagram("Happiness Streaming ETL · Kafka y Machine Learning", filename=str(OUT), show=False,
             direction="LR", outformat="png", graph_attr=graph, node_attr=node, edge_attr=edge):
    with Cluster("Parte A · preparación (una vez, sin Kafka)", graph_attr=box(MUTED, "#f7f3ef")):
        raw = MultipleDocuments("World Happiness\n2015-2019\n5 CSV")
        etl = Python("ETL Parte A\nmain.py\nlimpia y unifica")
        train = Python("Entrenamiento\nnotebooks/\n70 % train · 30 % test")
        model = Database("models/model.pkl\nPipeline sklearn")
        test = Document("data/streaming/\nhappiness_test.csv\n235 filas")
        raw >> etl >> Edge(label="782 filas") >> train
        train >> model
        train >> test

    with Cluster("Parte B · streaming (en tiempo real)", graph_attr=box(WINE, "#fbf6f7")):
        producer = Python("Producer\nkafka/producer.py\nun evento JSON por fila")
        kafka = Kafka("Kafka (KRaft) · Docker\ntopic happiness-events\n3 particiones · llave = país")
        consumer = Python("Consumer\nkafka/consumer.py\nvalida contrato → predice")
        db = Postgresql("PostgreSQL 16 · Docker\nraw_happiness_events\nfact_predictions\ndim_country · dim_date")
        tableau = Tableau("Tableau Desktop\nconexión en vivo\ndashboard con 4 KPI")

        producer >> Edge(label="publica") >> kafka
        kafka >> Edge(label="lee en grupo\nhappiness-ml-team") >> consumer
        consumer >> Edge(label="guarda crudo + predicción\n(inválidos: solo crudo)\ny luego confirma offset", color=WINE, fontcolor=WINE) >> db
        db >> Edge(label="consulta SQL") >> tableau

    test >> Edge(label="lee", style="dashed") >> producer
    model >> Edge(label="carga", style="dashed") >> consumer
