# IMPORTS

# Librerías para argumentos, logs y manejo de errores
import argparse
import logging
import sys

# Librería de AWS para Glue y Athena
import awswrangler as wr


# CONFIGURACIÓN DE LOGGING

# Usamos logging para dejar rastro claro del ETL.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# PARSEO DE ARGUMENTOS

# Este script solo recibe el bucket.
# python etl/gold.py --bucket itam-analytics-avril
def parse_args():
    parser = argparse.ArgumentParser(
        description="Build Gold analytical table from Bronze using Athena CTAS."
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Name of the S3 bucket where Gold data and Athena results will be stored."
    )
    return parser.parse_args()


# CREAR DATABASE EN GLUE

# Creamos flights_gold si no existe.
# exist_ok=True lo vuelve idempotente.
def create_glue_database(database_name: str):
    try:
        logger.info(f"Creating Glue database if it does not exist: {database_name}")
        wr.catalog.create_database(name=database_name, exist_ok=True)
        logger.info(f"Glue database ready: {database_name}")
    except Exception:
        logger.exception(f"Error creating Glue database: {database_name}")
        sys.exit(1)


# ELIMINAR TABLA SI EXISTE

# Esto garantiza idempotencia:
# si la tabla ya existe de una corrida anterior, la borramos antes de recrearla.
def delete_table_if_exists(database_name: str, table_name: str):
    try:
        logger.info(f"Deleting table if it exists: {database_name}.{table_name}")
        wr.catalog.delete_table_if_exists(
            database=database_name,
            table=table_name
        )
        logger.info(f"Table ready to be recreated: {database_name}.{table_name}")
    except Exception:
        logger.exception(f"Error deleting table: {database_name}.{table_name}")
        sys.exit(1)


# SQL DEL CTAS

# Aquí construimos la tabla Gold desnormalizada.
# Hacemos joins desde flights_bronze.flights hacia los catálogos de aerolíneas y aeropuertos.

# Usamos una ubicación explícita en S3 para que Gold quede ordenado
# dentro del bucket de la tarea.
def build_ctas_query(bucket: str) -> str:
    gold_path = f"s3://{bucket}/flights/gold/vuelos_analitica/"

    query = f"""
    CREATE TABLE flights_gold.vuelos_analitica
    WITH (
        format = 'PARQUET',
        write_compression = 'SNAPPY',
        external_location = '{gold_path}'
    ) AS
    SELECT
        f.year,
        f.month,
        f.day,
        f.origin_airport,
        ap_orig.airport AS origin_airport_name,
        ap_orig.city AS origin_city,
        ap_orig.state AS origin_state,
        f.destination_airport,
        ap_dest.airport AS destination_airport_name,
        al.airline AS airline_name,
        f.departure_delay,
        f.arrival_delay,
        f.cancelled,
        f.cancellation_reason,
        f.distance,
        f.air_system_delay,
        f.airline_delay,
        f.weather_delay,
        f.late_aircraft_delay,
        f.security_delay
    FROM flights_bronze.flights f
    LEFT JOIN flights_bronze.airlines al
        ON f.airline = al.iata_code
    LEFT JOIN flights_bronze.airports ap_orig
        ON f.origin_airport = ap_orig.iata_code
    LEFT JOIN flights_bronze.airports ap_dest
        ON f.destination_airport = ap_dest.iata_code
    """
    return query


# EJECUTAR CTAS EN ATHENA

# El enunciado pide usar wr.athena.read_sql_query(..., ctas_approach=False).
# Aquí ejecutamos el CTAS y enviamos los resultados temporales de Athena
# a una carpeta de resultados dentro del bucket.
def run_ctas(query: str, bucket: str):
    try:
        athena_output = f"s3://{bucket}/athena-results/"
        logger.info("Running CTAS query in Athena")

        wr.athena.read_sql_query(
            sql=query,
            database="flights_gold",
            ctas_approach=False,
            s3_output=athena_output
        )

        logger.info("CTAS query completed successfully")

    except Exception:
        logger.exception("Error running CTAS query in Athena")
        sys.exit(1)


# VALIDAR TABLA GOLD

# Después del CTAS verificamos que la tabla exista y devuelva filas.
# Esto confirma que el join sí funcionó.
def validate_gold_table(bucket: str):
    try:
        athena_output = f"s3://{bucket}/athena-results/"
        logger.info("Validating Gold table with a sample query")

        df = wr.athena.read_sql_query(
            sql="""
            SELECT *
            FROM flights_gold.vuelos_analitica
            LIMIT 5
            """,
            database="flights_gold",
            ctas_approach=False,
            s3_output=athena_output
        )

        assert not df.empty, "Gold table validation returned no rows"

        logger.info(f"Gold validation succeeded with {len(df)} sample rows")

    except Exception:
        logger.exception("Error validating Gold table")
        sys.exit(1)


# MAIN

# Aquí conectamos todo:
# crear flights_gold
# borrar la tabla si existe
# construir el SQL CTAS
# ejecutar el CTAS
# validar que la tabla sí responda
def main():
    try:
        args = parse_args()
        database_name = "flights_gold"
        table_name = "vuelos_analitica"

        logger.info("Starting Gold ETL")

        create_glue_database(database_name)
        delete_table_if_exists(database_name, table_name)

        ctas_query = build_ctas_query(args.bucket)
        run_ctas(ctas_query, args.bucket)

        validate_gold_table(args.bucket)

        logger.info("Gold ETL completed successfully")

    except Exception:
        logger.exception("Unexpected error in Gold ETL")
        sys.exit(1)


# ENTRYPOINT

if __name__ == "__main__":
    main()
