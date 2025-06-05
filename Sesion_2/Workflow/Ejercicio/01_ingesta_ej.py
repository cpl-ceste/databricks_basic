from pyspark.sql import SparkSession
import argparse

def main():
    parser = argparse.ArgumentParser()
    ???
    ???
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Ingesta").getOrCreate()
    
    df = ???
    df.???

if __name__ == "__main__":
    main()