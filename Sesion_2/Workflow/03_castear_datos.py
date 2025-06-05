
import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, upper

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", required=True, help="Ruta de la Delta tabla de entrada")
    parser.add_argument("--output_table", required=True, help="Nombre de la Delta Table destino")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    df = spark.table(args.input_table)

    df_clean = (
        df.withColumn("amount", col("amount").cast("double"))
          .withColumn("order_date", to_date("order_date"))
          .withColumn("category", upper(col("category")))
          .withColumn("country", upper(col("country")))
    )

    df_clean.write.format("delta").mode("overwrite").saveAsTable(args.output_table)
    print("Transformación completada.")

if __name__ == "__main__":
    main()
