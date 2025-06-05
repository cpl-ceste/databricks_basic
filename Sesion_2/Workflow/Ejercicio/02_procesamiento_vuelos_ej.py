import argparse
from pyspark.sql.functions import col, unix_timestamp

def main():
    parser = argparse.ArgumentParser()
    ???
    ???
    args = parser.parse_args()

    vuelos = ????

    formato = "yyyy-MM-dd HH:mm"
    
    vuelos = vuelos.???

    vuelos.???

if __name__ == "__main__":
    main()