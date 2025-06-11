import argparse
from pyspark.sql.functions import col, avg, count, year, month, when


# ----------------------------
# Vista 1: KPI de puntualidad por mes y modelo de aeronave
# ----------------------------
def main():
    parser = argparse.ArgumentParser()
    # Poner los argumentos que se necesitan
    ???
    ???
    ???
    ???
    args = parser.parse_args()

    # Cargar tablas
    df_vuelos = ???
    df_dim_aeronaves = ???
    df_dim_aeropuertos = ???

    # Enriquecer vuelos con la tabla dim_aeronaves y dim_aeropuertos
    df_vuelos_enriquecido = df_vuelos \
        ???

    # Agrupar por modelo, pais, año y mes y calcular el % de vuelos a tiempo
    df_kpi_puntualidad = df_vuelos_enriquecido \
        ???

    # Cargar la tabla
    df_kpi_puntualidad.???


if __name__ == "__main__":
    main()