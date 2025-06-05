from pyspark.sql import SparkSession
import argparse


def main():
    parser = argparse.ArgumentParser()
    ???
    ???
    ???
    args = parser.parse_args()

    spark = SparkSession.builder.appName("JoinVuelosAeropuertos").getOrCreate()

    vuelos = ???
    aeropuertos = ???

    # Join por origen y destino
    vuelos_enriquecidos = vuelos.???

    vuelos_enriquecidos.write.???

    print(f"Guardada tabla {args.tabla_resultado} con {vuelos_enriquecidos.count()} registros")


if __name__ == "__main__":
    main()