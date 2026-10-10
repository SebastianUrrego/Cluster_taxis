"""
Taller Taxis NYC - PySpark en AWS EMR
Programa python-spark (punto 14). Ejecutar en el nodo master del cluster con:

    spark-submit taxis_nyc.py --bucket NOMBRE_DE_TU_BUCKET

Cada bloque "PUNTO N" se puede copiar tal cual a una celda de un notebook
(EMR Studio / JupyterHub). En el notebook la variable `spark` ya existe, así que
basta con definir B = "s3://NOMBRE_DE_TU_BUCKET" y pegar los bloques.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Zonas del archivo de zonas que no corresponden a un lugar real (IDs 264 y 265)
ZONAS_INVALIDAS = ["NV", "N/A", "Unknown", "Outside of NYC"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True, help="Nombre del bucket S3 (sin s3://)")
    args = parser.parse_args()
    B = f"s3://{args.bucket}"

    spark = (
        SparkSession.builder.appName("TallerTaxisNYC")
        # UTC para que year/month/unix_timestamp no se vean afectados por zonas horarias
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # ------------------------------------------------------------------
    # PUNTO 2: cargar viajes y zonas en dos dataframes
    # ------------------------------------------------------------------
    viajes = spark.read.parquet(f"{B}/taxis/viajes/").select(
        "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"
    )
    zonas = spark.read.csv(f"{B}/taxis/zonas/", header=True, inferSchema=True)

    viajes.printSchema()
    zonas.show(5, truncate=False)
    print("Total de viajes:", viajes.count())

    # ------------------------------------------------------------------
    # PUNTO 3: join para obtener el lugar de recogida (alias PU_ZONE)
    # ------------------------------------------------------------------
    zonas_pu = zonas.select(
        F.col("LocationID").alias("PU_ID"), F.col("Zone").alias("PU_ZONE")
    )
    consulta1 = viajes.join(
        F.broadcast(zonas_pu), viajes.PULocationID == zonas_pu.PU_ID
    ).select(
        "PU_ZONE",
        F.col("tpep_pickup_datetime").cast("timestamp").alias("hora_recogida"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("hora_dejada"),
        "DOLocationID",
    )
    consulta1.show(10, truncate=False)

    # ------------------------------------------------------------------
    # PUNTO 4: segundo join para obtener la zona de dejada (DO_ZONE)
    # ------------------------------------------------------------------
    zonas_do = zonas.select(
        F.col("LocationID").alias("DO_ID"), F.col("Zone").alias("DO_ZONE")
    )
    consulta2 = consulta1.join(
        F.broadcast(zonas_do), consulta1.DOLocationID == zonas_do.DO_ID
    ).select(
        "PU_ZONE",
        "DO_ZONE",
        F.col("hora_recogida").alias("PU_datetime"),
        F.col("hora_dejada").alias("DO_datetime"),
    )
    consulta2.show(10, truncate=False)

    # ------------------------------------------------------------------
    # PUNTO 5: trip_duration en minutos usando withColumn + unix_timestamp
    # ------------------------------------------------------------------
    df = consulta2.withColumn(
        "trip_duration",
        F.round(
            (F.unix_timestamp("DO_datetime") - F.unix_timestamp("PU_datetime")) / 60, 2
        ),
    )
    df.cache()
    df.show(10, truncate=False)

    # Datos "limpios": se usan donde el taller pide excluir datos erróneos.
    # - viajes que empiezan en 2018 (el dataset trae fechas como 2001, 2009, 2088)
    # - duración positiva y de máximo 24 horas
    # - zonas conocidas (sin nulos, "NV", "Unknown", etc.)
    limpio = df.filter(
        (F.year("PU_datetime") == 2018)
        & (F.col("trip_duration") > 0)
        & (F.col("trip_duration") <= 24 * 60)
        & F.col("PU_ZONE").isNotNull()
        & F.col("DO_ZONE").isNotNull()
        & ~F.col("PU_ZONE").isin(ZONAS_INVALIDAS)
        & ~F.col("DO_ZONE").isin(ZONAS_INVALIDAS)
    )

    # ------------------------------------------------------------------
    # PUNTO 6: el viaje que más ha durado
    # ------------------------------------------------------------------
    print("Viaje más largo (datos crudos, suele ser un error de registro):")
    df.orderBy(F.desc("trip_duration")).show(1, truncate=False)

    print("Viaje más largo (datos limpios):")
    limpio.orderBy(F.desc("trip_duration")).show(1, truncate=False)

    # ------------------------------------------------------------------
    # PUNTO 7: destino más frecuente (excluyendo datos erróneos)
    # ------------------------------------------------------------------
    destinos = limpio.groupBy("DO_ZONE").count().orderBy(F.desc("count"))
    print("Destino más frecuente:")
    destinos.show(5, truncate=False)

    # ------------------------------------------------------------------
    # PUNTO 8: viaje más frecuente (origen-destino), promedio y desviación estándar
    # ------------------------------------------------------------------
    rutas = (
        limpio.groupBy("PU_ZONE", "DO_ZONE")
        .agg(
            F.count("*").alias("num_viajes"),
            F.round(F.avg("trip_duration"), 2).alias("duracion_promedio_min"),
            F.round(F.stddev("trip_duration"), 2).alias("desviacion_std_min"),
        )
        .orderBy(F.desc("num_viajes"))
    )
    print("Viaje (origen-destino) más frecuente:")
    rutas.show(5, truncate=False)

    # Opcional: excluyendo viajes dentro de la misma zona
    print("Más frecuente con origen distinto al destino:")
    rutas.filter(F.col("PU_ZONE") != F.col("DO_ZONE")).show(5, truncate=False)

    # ------------------------------------------------------------------
    # PUNTOS 9 y 10: columnas enteras year y month del inicio del viaje
    # ------------------------------------------------------------------
    df = df.withColumn("year", F.year("PU_datetime").cast("int")).withColumn(
        "month", F.month("PU_datetime").cast("int")
    )
    df.printSchema()

    # ------------------------------------------------------------------
    # PUNTO 11: meses con número de viajes, de mayor a menor
    # ------------------------------------------------------------------
    (
        df.filter(F.col("year") == 2018)
        .groupBy("year", "month")
        .count()
        .withColumnRenamed("count", "num_viajes")
        .orderBy(F.desc("num_viajes"))
        .show(12)
    )

    # ------------------------------------------------------------------
    # PUNTO 12: guardar en parquet particionado por year y month
    # Resultado: s3://bucket/viajes/year=2018/month=1 ... month=12
    # Se guardan solo viajes de 2018 para no crear particiones basura (year=2088, etc.)
    # ------------------------------------------------------------------
    (
        df.filter(F.col("year") == 2018)
        .select("PU_ZONE", "DO_ZONE", "PU_datetime", "DO_datetime",
                "trip_duration", "year", "month")
        .repartition("year", "month")  # un archivo por partición
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(f"{B}/viajes/")
    )
    print(f"Datos guardados en {B}/viajes/")

    spark.stop()


if __name__ == "__main__":
    main()
