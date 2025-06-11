import sys
import argparse
import json
from pyspark.sql.functions import col, count, isnan, when, to_date, min as spark_min, max as spark_max


def main():
    parser = argparse.ArgumentParser()
    # Poner los argumentos que se necesitan
    ???
    ???
    ???
    ???
    args = parser.parse_args()

    # Cargar tabla
    df = ??? 
    
    # Cargar el fichero de configuracion
    with open() ???

    # Trabajar con el fichero de configuracion
    claves_primarias = ???
    columnas_categoricas = ???
    columnas_numericas = ???
    columna_fecha = ???
    columnas_completas = ???

    print(f"Validando DataFrame: {args.input_table}")
    # 0. Seleccionamos las columnas que queremos
    df = ???

    # 1. Nulos en claves primarias
    if claves_primarias:
        ???
    
    # 2. Duplicados por claves primarias
    if claves_primarias:
        ???
    
    # 3. Rango de columnas numéricas
    for col_name, (min_val, max_val) in columnas_numericas.items():
        ???
    
    # 4. Validación de columna de fecha
    if columna_fecha:
        ???

    # Escribir el df
    df.???

    print("Validación completada.\n")


if __name__ == "__main__":
    main()