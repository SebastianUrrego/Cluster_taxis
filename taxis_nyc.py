"""
Taller Taxis NYC - PySpark en AWS EMR
Punto 14: versión ejecutable del notebook.

Ejecutar en el nodo principal del clúster:

    spark-submit taxis_nyc.py --bucket taxis-gotys

Reproduce los puntos 2 al 12 del taller en el mismo orden que el notebook.
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
        # UTC: las marcas de tiempo son timestamp_ntz, se fija la zona para que
        # unix_timestamp no aplique conversiones
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    # ------------------------------------------------------------------
    # PUNTO 2: cargar viajes y zonas en dos dataframes
    # ------------------------------------------------------------------
    viajes = spark.read.parquet(f"{B}/taxis/viajes/")
    zonas = spark.read.csv(f"{B}/taxis/zonas/", header=True, inferSchema=True)

    viajes.printSchema()
    zonas.show(5, truncate=False)
    print("Viajes:", viajes.count())
    print("Zonas:", zonas.count())

    # ------------------------------------------------------------------
    # PUNTO 3: join para obtener el lugar de recogida (alias PU_ZONE)
    # broadcast: la tabla de zonas (265 filas) se envia a cada nodo en lugar
    # de redistribuir los 102 millones de viajes por la red
    # ------------------------------------------------------------------
    zonas_pu = zonas.select(
        F.col("LocationID").alias("PU_ID"), F.col("Zone").alias("PU_ZONE")
    )
    consulta1 = viajes.join(
        F.broadcast(zonas_pu), viajes.PULocationID == zonas_pu.PU_ID
    ).select(
        "PU_ZONE",
        F.col("tpep_pickup_datetime").alias("hora_recogida"),
        F.col("tpep_dropoff_datetime").alias("hora_dejada"),
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

    # ------------------------------------------------------------------
    # PUNTO 6: el viaje que mas ha durado
    # ------------------------------------------------------------------
    print("Viaje mas largo (datos crudos):")
    df.orderBy(F.desc("trip_duration")).show(1, truncate=False)

    # Datos limpios: se excluyen fechas fuera de 2018 (el dataset trae 2001,
    # 2009, 2088), duraciones negativas o mayores a 24 horas, y zonas sin
    # identificar. Se usa en los puntos 6, 7 y 8.
    limpio = df.filter(
        (F.year("PU_datetime") == 2018)
        & (F.col("trip_duration") > 0)
        & (F.col("trip_duration") <= 24 * 60)
        & F.col("PU_ZONE").isNotNull()
        & F.col("DO_ZONE").isNotNull()
        & ~F.col("PU_ZONE").isin(ZONAS_INVALIDAS)
        & ~F.col("DO_ZONE").isin(ZONAS_INVALIDAS)
    )
    limpio.cache()

    print("Viaje mas largo (datos limpios):")
    limpio.orderBy(F.desc("trip_duration")).show(1, truncate=False)

    # ------------------------------------------------------------------
    # PUNTO 7: destino mas frecuente (excluyendo datos erroneos)
    # ------------------------------------------------------------------
    print("Destinos mas frecuentes:")
    limpio.groupBy("DO_ZONE").count().orderBy(F.desc("count")).show(10, truncate=False)

    # ------------------------------------------------------------------
    # PUNTO 8: viaje mas frecuente (origen-destino), promedio y desviacion
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
    print("Rutas mas frecuentes:")
    rutas.show(10, truncate=False)

    print("Ruta mas frecuente con origen distinto al destino:")
    rutas.filter(F.col("PU_ZONE") != F.col("DO_ZONE")).show(10, truncate=False)

    # Percentiles de la ruta mas frecuente: la desviacion estandar (49.55 min)
    # es seis veces el promedio (8.03 min). Comparar mediana y p99 contra el
    # maximo muestra que la dispersion viene de registros con el taximetro mal
    # cerrado, no de variabilidad real del trafico.
    ruta_top = limpio.filter(
        (F.col("PU_ZONE") == "Upper East Side South")
        & (F.col("DO_ZONE") == "Upper East Side North")
    )
    print("Distribucion de la ruta mas frecuente:")
    ruta_top.select(
        F.count("*").alias("n"),
        F.round(F.avg("trip_duration"), 2).alias("promedio"),
        F.round(F.stddev("trip_duration"), 2).alias("desviacion"),
        F.round(F.expr("percentile_approx(trip_duration, 0.5)"), 2).alias("mediana"),
        F.round(F.expr("percentile_approx(trip_duration, 0.99)"), 2).alias("p99"),
        F.round(F.max("trip_duration"), 2).alias("maximo"),
    ).show()

    # ------------------------------------------------------------------
    # PUNTOS 9 y 10: columnas enteras year y month del inicio del viaje
    # ------------------------------------------------------------------
    df = df.withColumn("year", F.year("PU_datetime").cast("int")).withColumn(
        "month", F.month("PU_datetime").cast("int")
    )
    df.printSchema()
    df.select("PU_datetime", "year", "month").show(5)

    # ------------------------------------------------------------------
    # PUNTO 11: meses con numero de viajes, de mayor a menor
    # ------------------------------------------------------------------
    print("Meses por numero de viajes:")
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
    # Se filtra a 2018 para no crear particiones basura (year=2088, etc.)
    # ------------------------------------------------------------------
    (
        df.filter(F.col("year") == 2018)
        .select("PU_ZONE", "DO_ZONE", "PU_datetime", "DO_datetime",
                "trip_duration", "year", "month")
        .repartition("year", "month")
        .write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(f"{B}/viajes/")
    )
    print("Guardado en", f"{B}/viajes/")

    spark.stop()


if __name__ == "__main__":
    main()