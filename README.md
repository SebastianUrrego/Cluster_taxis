# Taller Taxis NYC — PySpark en AWS EMR

Análisis de los viajes de taxis amarillos de Nueva York (año 2018, ~102.8 millones de
registros) usando PySpark sobre un clúster de Amazon EMR, con los resultados
consultables desde AWS Athena.

Universidad Sergio Arboleda — 9º semestre.

## Contenido del repositorio

| Archivo | Descripción |
|---|---|
| `taxis.ipynb` | Notebook con el desarrollo de los puntos 2 al 12, con las salidas de cada consulta |
| `taxis_nyc.py` | El mismo análisis como programa `spark-submit` (punto 14) |
| `aws_setup.sh` | Comandos de AWS: carga a S3 (punto 1), crawler de Glue y verificación en Athena (punto 13) |
| `Taller Taxis.pdf` | Enunciado del taller |

Los archivos de datos no se versionan (ver `.gitignore`): son ~1.4 GB y se descargan
directamente de la fuente oficial.

## Datos

Los datos provienen del portal de la NYC Taxi & Limousine Commission:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

- **Viajes**: 12 archivos `yellow_tripdata_2018-MM.parquet` → `s3://BUCKET/taxis/viajes/`
- **Zonas**: `taxi_zone_lookup.csv` (265 zonas) → `s3://BUCKET/taxis/zonas/`

## Cómo ejecutarlo

### 1. Cargar los datos en S3

```bash
BUCKET=mi-bucket-taxis
aws s3 mb s3://$BUCKET --region us-east-1

for MES in 01 02 03 04 05 06 07 08 09 10 11 12; do
  curl -sSf https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-$MES.parquet \
    | aws s3 cp - s3://$BUCKET/taxis/viajes/yellow_tripdata_2018-$MES.parquet
done

curl -sSf https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv \
  | aws s3 cp - s3://$BUCKET/taxis/zonas/zones.csv
```

### 2. Crear el clúster EMR

```bash
aws emr create-cluster \
  --name "taxis-taller" \
  --release-label emr-7.9.0 \
  --applications Name=Spark Name=Hadoop Name=Livy Name=JupyterEnterpriseGateway \
  --instance-groups \
    InstanceGroupType=MASTER,InstanceCount=1,InstanceType=m4.xlarge \
    InstanceGroupType=CORE,InstanceCount=1,InstanceType=m4.xlarge \
  --service-role EMR_DefaultRole \
  --ec2-attributes InstanceProfile=LabInstanceProfile,KeyName=vockey,SubnetId=subnet-XXXX \
  --auto-termination-policy IdleTimeout=3600 \
  --region us-east-1
```

La subred debe pertenecer a la misma VPC configurada en el EMR Studio; de lo contrario
el Workspace no podrá conectarse al clúster.

### 3. Ejecutar el análisis

Desde un notebook en EMR Studio (kernel PySpark), conectando el Workspace al clúster, o
directamente en el nodo principal:

```bash
spark-submit taxis_nyc.py --bucket mi-bucket-taxis
```

### 4. Catalogar en Glue y consultar en Athena

Ver `aws_setup.sh`. El crawler crea la tabla `viajes` en la base de datos `taxis` a
partir del parquet particionado.

## Desarrollo

| Punto | Descripción |
|---|---|
| 1 | Carga del dataset en S3 |
| 2 | Lectura de viajes y zonas en dataframes |
| 3 | Join para obtener la zona de recogida (`PU_ZONE`) |
| 4 | Segundo join para la zona de destino (`DO_ZONE`) |
| 5 | Columna `trip_duration` en minutos vía `unix_timestamp` |
| 6 | Viaje de mayor duración |
| 7 | Destino más frecuente |
| 8 | Ruta más frecuente, con promedio y desviación estándar |
| 9-10 | Columnas enteras `year` y `month` |
| 11 | Meses ordenados por número de viajes |
| 12 | Escritura en parquet particionado por `year` y `month` |
| 13 | Crawler de Glue y consulta en Athena |
| 14 | Conversión del notebook a programa `spark-submit` |

### Tratamiento de datos erróneos

El dataset contiene registros inconsistentes que se excluyen en los puntos que lo
requieren: fechas fuera de 2018 (aparecen años como 2001, 2009 y 2088), duraciones
negativas o superiores a 24 horas, y zonas sin identificar (`NV`, `Unknown`,
`Outside of NYC`).

### Hallazgos

- **Destino más frecuente**: Upper East Side North, con 3.970.775 viajes.
- **Ruta más frecuente**: Upper East Side South → Upper East Side North, 610.448 viajes,
  con una duración promedio de 8.03 minutos y desviación estándar de 49.55 minutos.
- **Mes con más viajes**: marzo (9.430.359); el mes más bajo es julio (7.850.980).

La desviación estándar de la ruta más frecuente (49.55 min) es seis veces su promedio.
Al revisar los percentiles de esa ruta, la mediana es de 5.73 minutos y el percentil 99
de 16.68, frente a un máximo de 1439.72: la dispersión no refleja variabilidad real del
tráfico sino un conjunto reducido de registros con el taxímetro mal cerrado. Para este
caso la mediana describe mejor el comportamiento típico que el promedio.

## Optimizaciones aplicadas

- **Broadcast join**: la tabla de zonas (265 filas) se envía a cada nodo en lugar de
  redistribuir los 102 millones de viajes por la red.
- **Caché**: el dataframe con `trip_duration` se mantiene en memoria, ya que los puntos
  6 a 11 parten todos de él.
- **Particionado**: la escritura por `year` y `month` permite que Athena lea únicamente
  las particiones necesarias en cada consulta.
- **Sesión en UTC**: las marcas de tiempo son `timestamp_ntz`, por lo que se fija la
  zona horaria de la sesión para que `unix_timestamp` no aplique conversiones.
