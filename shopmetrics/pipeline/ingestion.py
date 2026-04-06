import snowflake.connector
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
import os
import logging

# Logging structuré
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Charger le .env
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# ── 1. Simulation d'une API externe ──────────────────────────────────────────
def fetch_new_orders():
    """Simule la récupération de nouvelles commandes depuis une API."""
    new_orders = [
        {
            "order_id": 1006,
            "customer_id": 42,
            "order_date": "2024-01-06 10:00:00",
            "status": "completed",
            "total_amount": 75.50
        },
        {
            "order_id": 1007,
            "customer_id": 17,
            "order_date": "2024-01-06 14:30:00",
            "status": "pending",
            "total_amount": 200.00
        },
        {
            "order_id": 1008,
            "customer_id": 99,
            "order_date": "2024-01-07 09:15:00",
            "status": "completed",
            "total_amount": 45.00
        },
    ]
    return pd.DataFrame(new_orders)

# ── 2. Validation des données ─────────────────────────────────────────────────
def validate_orders(df):
    """Contrôles qualité avant chargement."""
    errors = []

    # Vérifier les colonnes obligatoires
    required_cols = ["order_id", "customer_id", "order_date", "status", "total_amount"]
    for col in required_cols:
        if col not in df.columns:
            errors.append(f"Colonne manquante : {col}")

    # Vérifier les valeurs de status
    valid_statuses = ["completed", "cancelled", "pending"]
    invalid = df[~df["status"].isin(valid_statuses)]
    if not invalid.empty:
        errors.append(f"{len(invalid)} lignes avec un status invalide")

    # Vérifier les prix négatifs ou nuls
    bad_prices = df[df["total_amount"] <= 0]
    if not bad_prices.empty:
        errors.append(f"{len(bad_prices)} lignes avec un prix invalide")

    return errors

# ── 3. Connexion Snowflake ────────────────────────────────────────────────────
def get_snowflake_connection():
    """Crée une connexion à Snowflake depuis les variables d'environnement."""
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )

# ── 4. Chargement dans Snowflake ──────────────────────────────────────────────
def load_to_snowflake(df, conn):
    """Charge le DataFrame dans Snowflake avec MERGE pour éviter les doublons."""
    cursor = conn.cursor()

    # Créer la table si elle n'existe pas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW_ORDERS_STAGING (
            order_id      INTEGER,
            customer_id   INTEGER,
            order_date    TIMESTAMP,
            status        VARCHAR(50),
            total_amount  FLOAT,
            loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # MERGE : insert si nouveau, update si existant
    inserted = 0
    for _, row in df.iterrows():
        cursor.execute("""
            MERGE INTO RAW_ORDERS_STAGING AS target
            USING (
                SELECT %s AS order_id,
                       %s AS customer_id,
                       %s AS order_date,
                       %s AS status,
                       %s AS total_amount
            ) AS source
            ON target.order_id = source.order_id
            WHEN MATCHED THEN
                UPDATE SET
                    status       = source.status,
                    total_amount = source.total_amount,
                    loaded_at    = CURRENT_TIMESTAMP
            WHEN NOT MATCHED THEN
                INSERT (order_id, customer_id, order_date, status, total_amount)
                VALUES (source.order_id, source.customer_id, source.order_date,
                        source.status, source.total_amount)
        """, (
            row["order_id"],
            row["customer_id"],
            row["order_date"],
            row["status"],
            row["total_amount"]
        ))
        inserted += 1

    conn.commit()
    cursor.close()
    return inserted

# ── 5. Pipeline principal ─────────────────────────────────────────────────────
def run_pipeline():
    logger.info("Démarrage du pipeline...")

    # Extraction
    logger.info("Extraction des nouvelles commandes...")
    df = fetch_new_orders()
    logger.info(f"{len(df)} commandes récupérées")

    # Validation
    logger.info("Validation des données...")
    errors = validate_orders(df)
    if errors:
        logger.error("ERREURS DÉTECTÉES :")
        for e in errors:
            logger.error(f"  - {e}")
        return
    logger.info("Validation OK")

    # Chargement
    logger.info("Connexion à Snowflake...")
    conn = get_snowflake_connection()
    inserted = load_to_snowflake(df, conn)
    conn.close()

    logger.info(f"{inserted} commandes chargées dans RAW_ORDERS_STAGING")
    logger.info(f"[{datetime.now()}] Pipeline terminé avec succès !")

if __name__ == "__main__":
    run_pipeline()