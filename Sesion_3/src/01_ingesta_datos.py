import sys
import argparse
from pyspark.sql.functions import input_file_name, current_timestamp

def main():
    parser = argparse.ArgumentParser()
    # Poner los argumentos que se necesitan
    ???
    ???
    ???
    ???

    args = parser.parse_args()
    
    df = ???

    # Comprobar si el archivo no tiene filas
    if ???:
        raise Exception("El archivo no contiene filas para procesar.")

    # Asegurarse que hay columnas de particion
    if ??? != '':
        # Utilizar la función .split para separar las columnas de particion
        partition_columns = ???
        # Escribir con particion
        df.???
    else:
        # Escribir sin particion
        df.???
        
    print("Ingesta completada.")

if __name__ == "__main__":
    main()