
import sys
import argparse
from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_table", required=True, help="Ruta de la Delta tabla de entrada")
    parser.add_argument("--output_table", required=True, help="Nombre de la Delta Table destino")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    df = spark.table(args.input_table)

    expected_columns = ['order_id', 'customer_id', 'amount', 'category', 'country', 'order_date', 'ingest_time']
    if set(df.columns) != set(expected_columns):
        raise Exception("Error: Esquema incorrecto.")

    df = df.withColumn("amount", df["amount"].cast("double"))
    outliers = df.filter("amount > 100000 OR amount < 0").count()
    
    if outliers > 0:
        print(f"Se detectaron {outliers} outliers en 'amount'.")

    df_clean = df.dropDuplicates(["order_id"]) \
        .filter("amount IS NOT NULL AND order_date IS NOT NULL") \
        .filter("amount >= 0 AND amount < 100000")
    
    df_clean.write.format("delta").mode("overwrite").saveAsTable(args.output_table)

    print("Validación completada.")

if __name__ == "__main__":
    main()
