import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_file", required=True, help="Nombre de archivo")
    parser.add_argument("--output_table", required=True, help="Nombre de la Delta Table destino")
    args = parser.parse_args()
    spark = SparkSession.builder.getOrCreate()
    
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(args.input_file)
        .withColumn("ingest_time", current_timestamp())
    )

    # Comprobar si el archivo no tiene filas
    if df.count() == 0:
        raise Exception("El archivo no contiene filas para procesar.")

    df.write.format("delta").mode("append").saveAsTable(args.output_table)
    print("Ingesta completada.")

if __name__ == "__main__":
    main()
