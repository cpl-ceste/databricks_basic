
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, avg

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", required=True, help="Ruta de la Delta tabla de entrada")
    parser.add_argument("--output_table", required=True, help="Nombre de la Delta Table destino")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    df = spark.table(args.input_table)

    metricas = (
        df.groupBy("category", "country")
          .agg(
              sum("amount").alias("ventas_total"),
              count("order_id").alias("num_ordenes"),
              avg("amount").alias("promedio_orden")
          )
    )

    metricas.write.format("delta").mode("overwrite").saveAsTable(args.output_table)
    print("Métricas cargadas.")

if __name__ == "__main__":
    main()
