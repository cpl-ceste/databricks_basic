import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import sum
from pyspark.sql import Row
from datetime import datetime

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", required=True, help="Ruta de la Delta tabla de entrada")
    parser.add_argument("--output_table", required=True, help="Nombre de la Delta Table destino")
    parser.add_argument("--umbral_ventas", type=float, default=100000, help="Umbral de ventas para alerta")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    df = spark.table(args.input_table)
    total = df.agg(sum("ventas_total")).collect()[0][0]

    # Define el esquema explícitamente
    schema = StructType([
        StructField("timestamp", TimestampType(), True),
        StructField("ventas_total", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("mensaje", StringType(), True)
    ])

    log_entry = {
        "timestamp": datetime.now(),
        "ventas_total": total,
        "status": "OK" if total >= args.umbral_ventas else "ALERTA",
        "mensaje": "Ventas dentro del rango" if total >= args.umbral_ventas else "Ventas anormalmente bajas"
    }
    log_df = spark.createDataFrame([log_entry], schema=schema)
    log_df.write.format("delta").mode("append").saveAsTable(args.output_table)

    print(f"Ventas OK: {total}")

if __name__ == "__main__":
    main()
