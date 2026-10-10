#!/bin/bash
# Taller Taxis NYC - comandos de AWS (puntos 1, 13 y 14)
# Ejecutar en el nodo master del EMR o en CloudShell. Cambia el nombre del bucket.
set -e
BUCKET=mi-bucket-taxis
ROL_GLUE=LabRole   # en AWS Academy es LabRole; en una cuenta propia, un rol con AWSGlueServiceRole + acceso al bucket

# ----------------------------------------------------------------------
# PUNTO 1: descargar taxis amarillos 2018 y zonas, y subirlos a S3
# (los archivos se envían directo a S3 sin ocupar disco local)
# ----------------------------------------------------------------------
aws s3 mb s3://$BUCKET 2>/dev/null || true

for MES in 01 02 03 04 05 06 07 08 09 10 11 12; do
  F=yellow_tripdata_2018-$MES.parquet
  echo "Subiendo $F ..."
  curl -sSf https://d37ci6vzurychx.cloudfront.net/trip-data/$F \
    | aws s3 cp - s3://$BUCKET/taxis/viajes/$F
done

curl -sSf https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv \
  | aws s3 cp - s3://$BUCKET/taxis/zonas/zones.csv

aws s3 ls s3://$BUCKET/taxis/ --recursive --human-readable

# ----------------------------------------------------------------------
# PUNTO 14: ejecutar el programa en el cluster (desde el nodo master)
# ----------------------------------------------------------------------
spark-submit taxis_nyc.py --bucket $BUCKET

# ----------------------------------------------------------------------
# PUNTO 13: crawler de Glue -> BD "taxis", tabla "viajes"
# El crawler nombra la tabla según la carpeta: s3://bucket/viajes/ -> "viajes"
# ----------------------------------------------------------------------
aws glue create-database --database-input '{"Name":"taxis"}' || true

aws glue create-crawler \
  --name crawler-viajes \
  --role $ROL_GLUE \
  --database-name taxis \
  --targets "{\"S3Targets\":[{\"Path\":\"s3://$BUCKET/viajes/\"}]}" || true

aws glue start-crawler --name crawler-viajes
echo "Esperando a que termine el crawler..."
while [ "$(aws glue get-crawler --name crawler-viajes --query 'Crawler.State' --output text)" != "READY" ]; do
  sleep 15
done
aws glue get-tables --database-name taxis --query 'TableList[].Name'

# Verificación en Athena (también se puede pegar la consulta en la consola de Athena)
aws athena start-query-execution \
  --query-string "SELECT year, month, COUNT(*) AS viajes FROM taxis.viajes GROUP BY year, month ORDER BY viajes DESC" \
  --result-configuration "OutputLocation=s3://$BUCKET/athena-results/"
