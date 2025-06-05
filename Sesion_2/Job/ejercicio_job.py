#!/usr/bin/env python
from pyspark.sql import SparkSession
from pyspark.sql.functions import ???  # <-- Completa
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(????, ????, help="Ruta del volumen")
    parser.add_argument(????, ????, help="Nombre del archivo JSON")
    parser.add_argument(????, ????, help="Tabla Delta destino")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ejercicio_job").getOrCreate()

    try:
        # Leer el archivo JSON
        df = spark.read.???(args.input_volume + args.input_file)  # <-- Completa

        # Validar esquema (order_id, customer_id, total_amount, order_date)
        required_columns = ???  # <-- Completar
        if not all(col in df.columns for col in required_columns):
            raise ValueError("Faltan columnas obligatorias")

        # Añadir filtrado
        # 0. Eliminar duplicados
        # 1. Eliminar filas con total_amount <= 0
        df = ????

        # Añadir metadatos
        # 1. Añadir una nueva columna con la fecha de procesamiento
        # 2. Añadir una nueva columna con el nombre del archivo fuente
        # 3. Añadir una nueva columna con vuestro nombre
        df = ????



        # Escribir en Delta (append o overwrite según exista la tabla)
        if spark.catalog.tableExists(args.output_table):
            print("Añadiendo datos a la tabla existente...")
            ???  # <-- Completa
        else:
            ???  # <-- Completa (crear la tabla)

    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()