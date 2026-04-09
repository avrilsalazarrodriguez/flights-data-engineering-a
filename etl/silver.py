# IMPORTS

# Librerías para argumentos, logs y manejo de errores
import argparse
import logging
import sys

# Librerías de datos y AWS
import pandas as pd
import awswrangler as wr


# CONFIGURACIÓN DE LOGGING

# Usamos logging para que el ETL deje rastro claro
# de qué hizo, cuándo lo hizo y si falló.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# PARSEO DE ARGUMENTOS

# Este script solo recibe el bucket.
# python etl/silver.py --bucket itam-analytics-avril
def parse_args():
    parser = argparse.ArgumentParser(
        description="Build Silver aggregated tables from Bronze flights data."
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="Name of the S3 bucket where Silver data will be stored."
    )
    return parser.parse_args()


# CREAR DATABASE EN GLUE

# Creamos flights_silver si no existe.
# exist_ok=True lo vuelve idempotente.
def create_glue_database(database_name: str):
    try:
        logger.info(f"Creating Glue database if it does not exist: {database_name}")
        wr.catalog.create_database(name=database_name, exist_ok=True)
        logger.info(f"Glue database ready: {database_name}")
    except Exception:
        logger.exception(f"Error creating Glue database: {database_name}")
        sys.exit(1)


# LIMPIEZA DE TIPOS

# En Silver sí limpiamos porque ya vamos a agregar y calcular.
# errors = "coerce" convierte valores raros a NaN y evita sesgar 
# los promedios o comparaciones.
def clean_flights_types(df: pd.DataFrame) -> pd.DataFrame:
    try:
        numeric_columns = [
            "YEAR",
            "MONTH",
            "DAY",
            "DEPARTURE_DELAY",
            "ARRIVAL_DELAY",
            "CANCELLED",
            "WEATHER_DELAY"
        ]

        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        string_columns = [
            "AIRLINE",
            "ORIGIN_AIRPORT"
        ]

        for col in string_columns:
            df[col] = df[col].astype("string").str.strip().str.upper()
            df[col] = df[col].replace(
                {
                    "<NA>": pd.NA,
                    "NAN": pd.NA,
                    "NONE": pd.NA,
                    "NULL": pd.NA
                }
            )

        return df

    except Exception:
        logger.exception("Error cleaning flight data types")
        sys.exit(1)
        

# Verificación de nombres válidos para aeropuertos
def keep_valid_iata_airports(df: pd.DataFrame) -> pd.DataFrame:
    valid_mask = df["ORIGIN_AIRPORT"].str.fullmatch(r"[A-Z]{3}", na=False)
    return df[valid_mask].copy()


# VALIDACIÓN GENERAL

# Antes de escribir cualquier tabla Silver validamos:
# que no esté vacía
# que tenga columnas
# que tenga columnas clave esperadas
def validate_df(df: pd.DataFrame, table_name: str):
    try:
        logger.info(f"Validating DataFrame for table: {table_name}")

        assert not df.empty, f"{table_name} is empty"
        assert len(df.columns) > 0, f"{table_name} has no columns"

        expected_columns = {
            "flights_daily": [
                "YEAR", "MONTH", "DAY",
                "total_flights", "total_delayed", "total_cancelled",
                "avg_departure_delay", "avg_arrival_delay"
            ],
            "flights_monthly": [
                "MONTH", "AIRLINE",
                "total_flights", "total_delayed", "total_cancelled",
                "avg_arrival_delay", "on_time_pct"
            ],
            "flights_by_airport": [
                "ORIGIN_AIRPORT",
                "total_departures", "total_delayed", "total_cancelled",
                "avg_departure_delay", "pct_weather_delay"
            ]
        }

        for col in expected_columns[table_name]:
            assert col in df.columns, f"{table_name} is missing required column: {col}"

        logger.info(f"Validation passed for table: {table_name}")

    except AssertionError as e:
        logger.exception(f"Validation failed for {table_name}: {e}")
        sys.exit(1)


# LECTURA DE BRONZE EN CHUNKS

# Leemos Bronze por partes para no saturar la memoría.
# Además pedimos solo las columnas que de verdad necesitamos para Silver.
def read_bronze_flights_in_chunks(bucket: str, chunk_size: int = 500_000):
    try:
        bronze_path = f"s3://{bucket}/flights/bronze/flights/"
        logger.info(f"Reading Bronze flights data in chunks from: {bronze_path}")
        logger.info(f"Chunk size: {chunk_size}")

        columns = [
            "year",
            "month",
            "day",
            "airline",
            "origin_airport",
            "departure_delay",
            "arrival_delay",
            "cancelled",
            "weather_delay"
        ]

        chunk_iterator = wr.s3.read_parquet(
            path=bronze_path,
            dataset=True,
            columns=columns,
            chunked=chunk_size,
            use_threads=False
        )

        for chunk in chunk_iterator:
            chunk = chunk.rename(columns=str.upper)
            yield chunk

    except Exception:
        logger.exception("Error preparing chunked read from Bronze flights")
        sys.exit(1)

# AGREGACIÓN PARCIAL: flights_daily

# Para poder procesar por chunks, primero calculamos parciales.
# Luego al final juntamos todos los parciales y volvemos a agrupar.

# Guardamos sums y counts para después calcular promedios correctamente.
def build_daily_partial(chunk: pd.DataFrame) -> pd.DataFrame:
    not_cancelled = chunk[chunk["CANCELLED"] == 0].copy()

    base = (
        chunk.groupby(["YEAR", "MONTH", "DAY"], dropna=False)
        .agg(
            total_flights=("YEAR", "size"),
            total_delayed=("DEPARTURE_DELAY", lambda s: (s > 0).sum()),
            total_cancelled=("CANCELLED", lambda s: (s == 1).sum())
        )
        .reset_index()
    )

    delays = (
        not_cancelled.groupby(["YEAR", "MONTH", "DAY"], dropna=False)
        .agg(
            departure_delay_sum=("DEPARTURE_DELAY", "sum"),
            departure_delay_count=("DEPARTURE_DELAY", "count"),
            arrival_delay_sum=("ARRIVAL_DELAY", "sum"),
            arrival_delay_count=("ARRIVAL_DELAY", "count")
        )
        .reset_index()
    )

    return base.merge(
        delays,
        on=["YEAR", "MONTH", "DAY"],
        how="left"
    )


# AGREGACIÓN PARCIAL: flights_monthly

# Igual que arriba: guardamos sumas y conteos.
# on_time_pct se calcula al final como:
# vuelos con arrival_delay <= 15 / total_flights * 100
def build_monthly_partial(chunk: pd.DataFrame) -> pd.DataFrame:
    not_cancelled = chunk[chunk["CANCELLED"] == 0].copy()

    base = (
        chunk.groupby(["MONTH", "AIRLINE"], dropna=False)
        .agg(
            total_flights=("MONTH", "size"),
            total_delayed=("DEPARTURE_DELAY", lambda s: (s > 0).sum()),
            total_cancelled=("CANCELLED", lambda s: (s == 1).sum()),
            on_time_count=("ARRIVAL_DELAY", lambda s: (s <= 15).sum())
        )
        .reset_index()
    )

    delays = (
        not_cancelled.groupby(["MONTH", "AIRLINE"], dropna=False)
        .agg(
            arrival_delay_sum=("ARRIVAL_DELAY", "sum"),
            arrival_delay_count=("ARRIVAL_DELAY", "count")
        )
        .reset_index()
    )

    return base.merge(
        delays,
        on=["MONTH", "AIRLINE"],
        how="left"
    )


# AGREGACIÓN PARCIAL: flights_by_airport

# pct_weather_delay se calcula al final como:
# total minutos WEATHER_DELAY / total minutos de departure_delay positivo * 100
def build_airport_partial(chunk: pd.DataFrame) -> pd.DataFrame:
    chunk = keep_valid_iata_airports(chunk)
    not_cancelled = chunk[chunk["CANCELLED"] == 0].copy()

    base = (
        chunk.groupby(["ORIGIN_AIRPORT"], dropna=False)
        .agg(
            total_departures=("ORIGIN_AIRPORT", "size"),
            total_delayed=("DEPARTURE_DELAY", lambda s: (s > 0).sum()),
            total_cancelled=("CANCELLED", lambda s: (s == 1).sum())
        )
        .reset_index()
    )

    dep_delay = (
        not_cancelled.groupby(["ORIGIN_AIRPORT"], dropna=False)
        .agg(
            departure_delay_sum=("DEPARTURE_DELAY", "sum"),
            departure_delay_count=("DEPARTURE_DELAY", "count")
        )
        .reset_index()
    )

    weather_delay = (
        chunk.groupby(["ORIGIN_AIRPORT"], dropna=False)
        .agg(
            total_weather_delay=("WEATHER_DELAY", "sum"),
            total_positive_departure_delay=("DEPARTURE_DELAY", lambda s: s[s > 0].sum())
        )
        .reset_index()
    )

    result = base.merge(dep_delay, on=["ORIGIN_AIRPORT"], how="left")
    result = result.merge(weather_delay, on=["ORIGIN_AIRPORT"], how="left")

    return result

# FINALIZAR flights_daily

# Aquí juntamos todos los parciales y calculamos promedios finales.
def finalize_flights_daily(partials: list[pd.DataFrame]) -> pd.DataFrame:
    try:
        logger.info("Finalizing Silver table: flights_daily")

        df = pd.concat(partials, ignore_index=True)

        result = (
            df.groupby(["YEAR", "MONTH", "DAY"], dropna=False)
            .agg(
                total_flights=("total_flights", "sum"),
                total_delayed=("total_delayed", "sum"),
                total_cancelled=("total_cancelled", "sum"),
                departure_delay_sum=("departure_delay_sum", "sum"),
                departure_delay_count=("departure_delay_count", "sum"),
                arrival_delay_sum=("arrival_delay_sum", "sum"),
                arrival_delay_count=("arrival_delay_count", "sum")
            )
            .reset_index()
        )

        result["avg_departure_delay"] = (
            result["departure_delay_sum"] / result["departure_delay_count"]
        )
        result["avg_arrival_delay"] = (
            result["arrival_delay_sum"] / result["arrival_delay_count"]
        )

        result = result.drop(
            columns=[
                "departure_delay_sum",
                "departure_delay_count",
                "arrival_delay_sum",
                "arrival_delay_count"
            ]
        )

        logger.info(f"Built flights_daily with {len(result)} rows")
        result = result.sort_values(["YEAR", "MONTH", "DAY"]).reset_index(drop=True)
        return result

    except Exception:
        logger.exception("Error finalizing flights_daily")
        sys.exit(1)


# FINALIZAR flights_monthly

# Aquí ya calculamos avg_arrival_delay y on_time_pct final.
def finalize_flights_monthly(partials: list[pd.DataFrame]) -> pd.DataFrame:
    try:
        logger.info("Finalizing Silver table: flights_monthly")

        df = pd.concat(partials, ignore_index=True)

        result = (
            df.groupby(["MONTH", "AIRLINE"], dropna=False)
            .agg(
                total_flights=("total_flights", "sum"),
                total_delayed=("total_delayed", "sum"),
                total_cancelled=("total_cancelled", "sum"),
                on_time_count=("on_time_count", "sum"),
                arrival_delay_sum=("arrival_delay_sum", "sum"),
                arrival_delay_count=("arrival_delay_count", "sum")
            )
            .reset_index()
        )

        result["avg_arrival_delay"] = (
            result["arrival_delay_sum"] / result["arrival_delay_count"]
        )
        result["on_time_pct"] = (
            result["on_time_count"] / result["total_flights"]
        ) * 100

        result = result.drop(
            columns=[
                "on_time_count",
                "arrival_delay_sum",
                "arrival_delay_count"
            ]
        )

        logger.info(f"Built flights_monthly with {len(result)} rows")
        result = result.sort_values(["MONTH", "AIRLINE"]).reset_index(drop=True)
        return result

    except Exception:
        logger.exception("Error finalizing flights_monthly")
        sys.exit(1)


# FINALIZAR flights_by_airport

# Aquí calculamos avg_departure_delay y pct_weather_delay final.
def finalize_flights_by_airport(partials: list[pd.DataFrame]) -> pd.DataFrame:
    try:
        logger.info("Finalizing Silver table: flights_by_airport")

        df = pd.concat(partials, ignore_index=True)

        result = (
            df.groupby(["ORIGIN_AIRPORT"], dropna=False)
            .agg(
                total_departures=("total_departures", "sum"),
                total_delayed=("total_delayed", "sum"),
                total_cancelled=("total_cancelled", "sum"),
                departure_delay_sum=("departure_delay_sum", "sum"),
                departure_delay_count=("departure_delay_count", "sum"),
                total_weather_delay=("total_weather_delay", "sum"),
                total_positive_departure_delay=("total_positive_departure_delay", "sum")
            )
            .reset_index()
        )

        result["avg_departure_delay"] = (
            result["departure_delay_sum"] / result["departure_delay_count"]
        )

        result["pct_weather_delay"] = (
            result["total_weather_delay"] / result["total_positive_departure_delay"]
        ) * 100

        result["pct_weather_delay"] = result["pct_weather_delay"].fillna(0)

        result = result.drop(
            columns=[
                "departure_delay_sum",
                "departure_delay_count",
                "total_weather_delay",
                "total_positive_departure_delay"
            ]
        )

        result = result.sort_values(["ORIGIN_AIRPORT"]).reset_index(drop=True)

        logger.info(f"Built flights_by_airport with {len(result)} rows")
        return result

    except Exception:
        logger.exception("Error finalizing flights_by_airport")
        sys.exit(1)


# ESCRITURA A S3 y GLUE

# flights_daily se particiona por MONTH y usa overwrite_partitions.
# Las otras dos usan overwrite normal.
def write_silver_table(
    df: pd.DataFrame,
    bucket: str,
    database_name: str,
    table_name: str
):
    try:
        s3_path = f"s3://{bucket}/flights/silver/{table_name}/"
        logger.info(f"Writing Silver table {table_name} to: {s3_path}")

        if table_name == "flights_daily":
            wr.s3.to_parquet(
                df=df,
                path=s3_path,
                dataset=True,
                mode="overwrite_partitions",
                compression="snappy",
                partition_cols=["MONTH"],
                database=database_name,
                table=table_name
            )
        else:
            wr.s3.to_parquet(
                df=df,
                path=s3_path,
                dataset=True,
                mode="overwrite",
                compression="snappy",
                database=database_name,
                table=table_name
            )

        logger.info(f"Wrote {len(df)} rows to {s3_path}")

    except Exception:
        logger.exception(f"Error writing Silver table: {table_name}")
        sys.exit(1)


# MAIN

# Aquí conectamos todo:
# crear flights_silver
# leer Bronze en chunks
# limpiar chunk por chunk
# construir parciales chunk por chunk
# finalizar las 3 tablas
# validar
# escribir a S3 + Glue
def main():
    try:
        args = parse_args()
        database_name = "flights_silver"

        logger.info("Starting Silver ETL")

        create_glue_database(database_name)

        daily_partials = []
        monthly_partials = []
        airport_partials = []

        chunk_iterator = read_bronze_flights_in_chunks(args.bucket)

        for i, chunk in enumerate(chunk_iterator, start=1):
            logger.info(f"Processing Bronze chunk {i} with {len(chunk)} rows")

            chunk = clean_flights_types(chunk)

            daily_partials.append(build_daily_partial(chunk))
            monthly_partials.append(build_monthly_partial(chunk))
            airport_partials.append(build_airport_partial(chunk))

            logger.info(f"Finished aggregations for chunk {i}")

        flights_daily_df = finalize_flights_daily(daily_partials)
        validate_df(flights_daily_df, "flights_daily")
        write_silver_table(
            df=flights_daily_df,
            bucket=args.bucket,
            database_name=database_name,
            table_name="flights_daily"
        )

        flights_monthly_df = finalize_flights_monthly(monthly_partials)
        validate_df(flights_monthly_df, "flights_monthly")
        write_silver_table(
            df=flights_monthly_df,
            bucket=args.bucket,
            database_name=database_name,
            table_name="flights_monthly"
        )

        flights_by_airport_df = finalize_flights_by_airport(airport_partials)
        validate_df(flights_by_airport_df, "flights_by_airport")
        write_silver_table(
            df=flights_by_airport_df,
            bucket=args.bucket,
            database_name=database_name,
            table_name="flights_by_airport"
        )

        logger.info("Silver ETL completed successfully")

    except Exception:
        logger.exception("Unexpected error in Silver ETL")
        sys.exit(1)


# ENTRYPOINT

if __name__ == "__main__":
    main()
