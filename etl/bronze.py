# IMPORTS

# Librerías necesarias para argumentos, logs, manejo de errores y rutas
import argparse
import logging
import sys
from pathlib import Path

# Librerías de datos y AWS
import pandas as pd
import awswrangler as wr


# CONFIGURACIÓN DE LOGGING

# Configuramos logs con timestamp, nivel y mensaje
# Esto reemplaza print() y permite auditar el pipeline
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Creamos el logger del script
logger = logging.getLogger(__name__)


# PARSEO DE ARGUMENTOS

# Aquí definimos los parámetros que el usuario pasará desde terminal
# Ejemplo:
# python etl/bronze.py --bucket mi-bucket --data-dir data/
def parse_args():
    parser = argparse.ArgumentParser(
        description="Load raw flights CSV files to S3 Bronze and register them in Glue."
    )

    # Nombre del bucket S3 donde guardaremos Bronze
    parser.add_argument(
        "--bucket",
        required=True,
        help="Name of the S3 bucket where Bronze data will be stored."
    )

    # Ruta local donde están los CSVs descargados
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Local directory that contains the raw CSV files."
    )

    return parser.parse_args()


# EXTRACT (LECTURA DE CSV)

# Esta función lee un CSV desde disco local:
# Loggea qué archivo está leyendo
# Cuenta filas
# Si falla, rompe el pipeline (correcto en ETL)
def load_csv(file_path: Path) -> pd.DataFrame:
    try:
        logger.info(f"Loading file: {file_path}")

        # Leemos el CSV a un DataFrame
        df = pd.read_csv(file_path)

        # Log de control de calidad básico
        logger.info(f"Loaded {len(df)} rows from {file_path.name}")

        return df

    except Exception as e:
        # Loggea el error completo (stack trace)
        logger.exception(f"Error loading {file_path}")

        # Terminamos con error (exit code != 0)
        sys.exit(1)


# LOAD GRANDE POR CHUNKS (SOLO PARA flights)

# flights.csv es demasiado grande para leerlo completo en memoria.
# Entonces lo procesamos por partes ("chunks").

# Se lee 1 bloque de filas
# se valida
# se escribe a S3 como parte del dataset
# se repite  hasta terminar

# Primer chunk: mode="overwrite"
# Siguientes chunks: mode="append"

# Así evitamos el error "Killed" por memoria.
def write_large_csv_in_chunks(
    file_path: Path,
    bucket: str,
    database_name: str,
    table_name: str,
    chunk_size: int = 500_000
):
    try:
        s3_path = f"s3://{bucket}/flights/bronze/{table_name}/"
        total_rows = 0
        first_chunk = True

        logger.info(f"Loading large file in chunks: {file_path}")
        logger.info(f"Destination S3 path: {s3_path}")
        logger.info(f"Chunk size: {chunk_size}")

        for i, chunk in enumerate(pd.read_csv(file_path, chunksize=chunk_size), start=1):
            logger.info(f"Processing chunk {i} for table {table_name} with {len(chunk)} rows")

            validate_df(chunk, table_name)

            wr.s3.to_parquet(
                df=chunk,
                path=s3_path,
                dataset=True,
                mode="overwrite" if first_chunk else "append",
                database=database_name,
                table=table_name
            )

            total_rows += len(chunk)
            first_chunk = False

            logger.info(f"Chunk {i} written successfully for table {table_name}")

        logger.info(f"Loaded {total_rows} total rows into {table_name} at {s3_path}")

    except Exception:
        logger.exception(f"Error writing large table {table_name} in chunks")
        sys.exit(1)


# VALIDACIÓN DE DATAFRAMES

# En ETL, antes de subir cualquier tabla a S3, revisamos:
# que sí tenga filas
# que sí tenga columnas
# que las columnas esperadas existan

# Esto evita tener Bronze con archivos vacíos o mal leídos.
def validate_df(df: pd.DataFrame, table_name: str):
    try:
        logger.info(f"Validating DataFrame for table: {table_name}")

        # Regla 1: no puede venir vacío
        assert not df.empty, f"{table_name} is empty"

        # Regla 2: debe tener al menos una columna
        assert len(df.columns) > 0, f"{table_name} has no columns"

        # Regla 3: validamos columnas clave por tabla
        # No validamos todo el schema completo,
        # solo columnas mínimas para detectar errores graves.
        expected_columns = {
            "flights": ["YEAR", "MONTH", "DAY", "AIRLINE", "ORIGIN_AIRPORT", "DESTINATION_AIRPORT"],
            "airlines": ["IATA_CODE", "AIRLINE"],
            "airports": ["IATA_CODE", "AIRPORT", "CITY", "STATE"]
        }

        for col in expected_columns[table_name]:
            assert col in df.columns, f"{table_name} is missing required column: {col}"

        logger.info(f"Validation passed for table: {table_name}")

    except AssertionError as e:
        logger.exception(f"Validation failed for {table_name}: {e}")
        sys.exit(1)


# GLUE DATABASE

# Bronze necesita una base de datos en Glue llamada flights_bronze.
# Glue no guarda datos; guarda el catálogo de tablas:
# nombre, columnas, tipos y ruta en S3.

# exist_ok=True hace el script idempotente:
# si la base ya existe, no falla.
def create_glue_database(database_name: str):
    try:
        logger.info(f"Creating Glue database if it does not exist: {database_name}")
        wr.catalog.create_database(name=database_name, exist_ok=True)
        logger.info(f"Glue database ready: {database_name}")

    except Exception:
        logger.exception(f"Error creating Glue database: {database_name}")
        sys.exit(1)


# WRITE A BRONZE TABLE

# Esta función hace la parte central de Bronze:
# toma un DataFrame ya leído
# lo escribe en S3 en su propia ruta
# registra automáticamente la tabla en Glue

# Aunque el enunciado dice "sin transformaciones",
# existe una excepción ante el uso de  wr.s3.to_parquet().

# mode="overwrite" permite correr el script varias veces
# sin duplicar datos.
def write_bronze_table(
    df: pd.DataFrame,
    bucket: str,
    database_name: str,
    table_name: str
):
    try:
        s3_path = f"s3://{bucket}/flights/bronze/{table_name}/"

        logger.info(f"Writing table {table_name} to S3 path: {s3_path}")

        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            dataset=True,
            mode="overwrite",
            database=database_name,
            table=table_name
        )

        logger.info(f"Loaded {len(df)} rows into {table_name} at {s3_path}")

    except Exception:
        logger.exception(f"Error writing table {table_name} to S3/Glue")
        sys.exit(1)


# MAIN

# main() es el orquestador del script.
# Aquí conectamos todos los pasos en orden:

# leemos argumentos de terminal
# construimos rutas locales a los CSVs
# creamos la base flights_bronze en Glue

# por cada archivo:
# - se lee CSV
# - se valida DataFrame
# - se escribe a S3
# - se registra en Glue

# Si todo sale bien, el script termina con un log final de éxito.
def main():
    try:
        args = parse_args()

        # data_dir será "data/" cuando se corra:
        # python etl/bronze.py --bucket ... --data-dir data/

        # Dentro de data/ se tiene una carpeta flights/
        # y dentro están los 3 CSVs.
        data_dir = Path(args.data_dir) / "flights"

        # Nombre fijo de la base de datos Bronze en Glue
        database_name = "flights_bronze"

        # Mapa tabla -> archivo local
        files = {
            "flights": data_dir / "flights.csv",
            "airlines": data_dir / "airlines.csv",
            "airports": data_dir / "airports.csv"
        }

        logger.info("Starting Bronze ETL")

        # Primero garantizamos que la base exista
        create_glue_database(database_name)

        # Luego procesamos los 3 archivos uno por uno

        for table_name, file_path in files.items():
            logger.info(f"Processing table: {table_name}")
            
            # flights.csv es muy grande, así que lo procesamos por chunks
            if table_name == "flights":
                write_large_csv_in_chunks(
                    file_path=file_path,
                    bucket=args.bucket,
                    database_name=database_name,
                    table_name=table_name
                )
            else:
                # airlines y airports sí caben completos en memoria
                df = load_csv(file_path)
                validate_df(df, table_name)
                write_bronze_table(
                    df=df,
                    bucket=args.bucket,
                    database_name=database_name,
                    table_name=table_name
                )
        logger.info("Bronze ETL completed successfully")

    except Exception:
        logger.exception("Unexpected error in Bronze ETL")
        sys.exit(1)


# ENTRYPOINT

# Este bloque permite correr el script desde terminal.
if __name__ == "__main__":
    main()
