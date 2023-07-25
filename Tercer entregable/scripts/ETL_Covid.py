# Este script está pensado para correr en Spark y hacer el proceso de ETL de la tabla covid_colombia

import requests
from datetime import datetime, timedelta
from os import environ as env
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, lit, col, expr, to_date

from commons import ETL_Spark

class ETL_Covid(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = "2023-07-23"  # datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def extract(self):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API...")

        url='https://www.datos.gov.co/resource/er5n-e6tz.json'
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            print(data)
        else:
            print("Error al extraer datos de la API")
            data = []
            raise Exception("Error al extraer datos de la API")

        df = self.spark.read.json(
            self.spark.sparkContext.parallelize(data), multiLine=True
        )
        df.printSchema()
        df.show()

        return df

    def transform(self, df):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")

        dfCov_t = df.toPandas()

        # Eliminacion de duplicados
        dfCov_t.drop_duplicates()

        # Eliminacion de columnas sin valor.
        col_del=['nom_grupo_', 'departamento_nom', 'ciudad_municipio_nom']
        dfCov_t=dfCov_t.drop(labels=col_del, axis=1)

        # Se ajustan las columnas con variables numéricas
        dfCov_t['id_de_caso'] = dfCov_t['id_de_caso'].astype('int')
        dfCov_t['departamento'] = dfCov_t['departamento'].astype('int')
        dfCov_t['edad'] = dfCov_t['edad'].astype('int')
        dfCov_t['per_etn_'] = dfCov_t['per_etn_'].astype('int')
        dfCov_t['ciudad_municipio'] = dfCov_t['ciudad_municipio'].astype('int')
        
        # Se ajustan las columnas con variables temporales
        dfCov_t['fecha_reporte_web'] = dfCov_t['fecha_reporte_web'].astype('datetime64[ns]')
        dfCov_t['fecha_de_notificaci_n'] = dfCov_t['fecha_de_notificaci_n'].astype('datetime64[ns]')
        dfCov_t['fecha_inicio_sintomas'] = dfCov_t['fecha_inicio_sintomas'].astype('datetime64[ns]')
        dfCov_t['fecha_diagnostico'] = dfCov_t['fecha_diagnostico'].astype('datetime64[ns]')

        # Create the DataFrame with the specified column names
        df_final = self.spark.createDataFrame(dfCov_t, ["ciudad_municipio", "departamento", "edad",
            "estado", "fecha_de_notificaci_n", "fecha_diagnostico", "fecha_inicio_sintomas",
            "fecha_muerte", "fecha_recuperado", "fecha_reporte_web", "fuente_tipo_contagio", 
            "id_de_caso", "per_etn_", "recuperado", "sexo",
            "tipo_recuperacion", "ubicacion", "unidad_medida"])

        df_final.printSchema()
        df_final.show()

        return df_final

    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")

        # add process_date column
        df_final = df_final.withColumn("process_date", lit(self.process_date))

        df_final.write \
            .format("jdbc") \
            .option("url", env['REDSHIFT_URL']) \
            .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.covid_colombia") \
            .option("user", env['REDSHIFT_USER']) \
            .option("password", env['REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        print(">>> [L] Datos cargados exitosamente")


if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_Covid()
    etl.run()
