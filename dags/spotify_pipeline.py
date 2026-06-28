from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = Path(__file__).resolve().parent.parent
with DAG(
    dag_id="spotify_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags = ["spotify", "spark", "etl"]
) as dag:
    
    silver_tracks = BashOperator(
        task_id="silver_tracks",
        bash_command=f"""
        cd "{PROJECT_ROOT}" &&
        python src/silver/silver_tracks.py
        """
    )

    # silver_artists = BashOperator(
    #     task_id="silver_artists",
    #     bash_command="python src/silver/silver_artists.py"
    # )

    gold_decade_summary = BashOperator(
        task_id="gold_decade_summary",
        bash_command=f"""
        cd "{PROJECT_ROOT}" &&
        python src/gold/gold_decade_summary.py
        """
    )

    gold_explicit_summary = BashOperator(
        task_id="gold_explicit_summary",
        bash_command=f"""
        cd "{PROJECT_ROOT}" &&
        python src/gold/gold_explicit_summary.py
        """
    )

    gold_instrumentalness_summary = BashOperator(
        task_id="gold_instrumentalness_summary",
        bash_command=f"""
        cd "{PROJECT_ROOT}" &&
        python src/gold/gold_instrumentalness_summary.py
        """
    )

    gold_popularity_summary = BashOperator(
        task_id="gold_popularity_summary",
        bash_command=f"""
        cd "{PROJECT_ROOT}" &&
        python src/gold/gold_popularity_summary.py
        """
    )

    silver_tracks >> [
        gold_decade_summary,
        gold_explicit_summary,   
        gold_instrumentalness_summary,
        gold_popularity_summary,
    ]