import argparse
from pyspark.sql.functions import col, avg, count, year, month

# ----------------------------
# Vista 2: Costo promedio de mantenimiento por tipo y modelo de aeronave
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
    df_dim_aeronaves = ???
    df_mantenimientos = ???

    # Enriquecer mantenmientos con aeronaves
    df_mantenimiento_enriquecido = df_mantenimientos \
        ???

    # Agrupar por aeronave_id, modelo, año, mes y calcular KPIs de mantenimientos
    df_kpi_mantenimiento = df_mantenimiento_enriquecido \
        ???

    df_kpi_mantenimiento.???


if __name__ == "__main__":
    main()