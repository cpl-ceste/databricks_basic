#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Job de Databricks para carga incremental de archivos desde un Volumen a una Delta Table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
import argparse



def main():
    # Configuración de argumentos
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_volume", required=True, help="Ruta del volumen con los archivos")
    parser.add_argument("--input_file", required=True, help="Patrón del nombre de archivo")
    parser.add_argument("--output_table", required=True, help="Nombre de la Delta Table destino")
    args = parser.parse_args()
    
    # Inicializar sesión Spark
    spark = SparkSession.builder \
        .appName("CargaIncrementalDelta") \
        .getOrCreate()
    
    try:
        # Paso 1: Leer datos del volumen
        input_path = f"{args.input_volume}{args.input_file}"
        print(f"\nLeyendo archivos de: {input_path}")
        
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("sep", ",") \
            .csv(input_path)
        
        # Añadir metadatos
        df = df.withColumn("fecha_carga", current_timestamp())
        
        num_nuevos_registros = df.count()
        print(f"Registros a cargar: {num_nuevos_registros}")
        
        if num_nuevos_registros == 0:
            print("No hay registros nuevos. Finalizando ejecución.")
            return
        
        # Paso 2: Escribir en Delta Table
        table_exists = spark.catalog.tableExists(args.output_table)
        
        if table_exists:
            print(f"\nLa tabla {args.output_table} existe. Añadiendo datos...")
            df.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(args.output_table)
        else:
            print(f"\nLa tabla {args.output_table} no existe. Creándola...")
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .saveAsTable(args.output_table)

        print("Tabla carga con exito")
        
        
    except Exception as e:
        print(f"\nERROR durante la ejecución: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()