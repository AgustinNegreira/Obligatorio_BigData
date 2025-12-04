# scripts/kpis_imbd_to_excel.py
import polars as pl
from pathlib import Path
import pandas as pd

# Rutas absolutas y seguras basadas en la ubicación del script
BASE_DIR = Path(__file__).resolve().parent.parent   # → carpeta Obligatorio
DATA_DIR = BASE_DIR / "datalake" / "curated" / "imdb"

OUT_DIR = BASE_DIR / "kpis"
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_XLSX = OUT_DIR / "kpis_imdb.xlsx"



def load_data():
    name_basics = pl.read_parquet(DATA_DIR / "name_basics_curated.parquet")
    title_basics = pl.read_parquet(DATA_DIR / "title_basics_curated.parquet")
    ratings = pl.read_parquet(DATA_DIR / "title_ratings_curated.parquet")
    principals = pl.read_parquet(DATA_DIR / "title_principals_curated.parquet")
    return name_basics, title_basics, principals, ratings


def normalize_genres(df):
    # convierte "Action,Drama" -> ["Action","Drama"], maneja \N y nulos
    return (
        df.lazy()
        .with_columns([
            pl.col("genres")
            .fill_null("")                        # evita None
            .str.replace(r"\\N", "")              # quita \N literal
            .str.replace(r"^\s+|\s+$", "")        # trim
            .str.split(",")                       # lista de géneros
            .alias("genres")
        ])
    )


# -------------------------------------------------------------
# KPI 1: Popularidad ponderada por género
# -------------------------------------------------------------
def kpi_popularidad_generos(title_basics, ratings):
    tb = (
        normalize_genres(title_basics)
        .select(["tconst", "genres"])
        .explode("genres")
        .filter(pl.col("genres").is_not_null() & (pl.col("genres") != ""))
    )

    rt = ratings.lazy().select(["tconst", "averageRating", "numVotes"])

    result = (
        tb.join(rt, on="tconst", how="inner")
        .group_by("genres")
        .agg([
            (pl.col("averageRating") * pl.col("numVotes")).sum().alias("popularidad_ponderada"),
            pl.col("numVotes").sum().alias("total_votos")
        ])
        .with_columns([
            (pl.col("popularidad_ponderada") / pl.col("total_votos"))
            .alias("rating_promedio_ponderado")
        ])
        .sort("rating_promedio_ponderado", descending=True)
        .collect()
    )

    return result


# -------------------------------------------------------------
# KPI 2: Evolución del rating por género (últimos 20 años)
# -------------------------------------------------------------
def kpi_evolucion_generos(title_basics, ratings, desde_anyo=2000):
    tb = (
        normalize_genres(title_basics)
        .select(["tconst", "startYear", "genres"])
        .with_columns([pl.col("startYear").cast(pl.Int32).alias("startYear")])
        .explode("genres")
        .filter(
            pl.col("genres").is_not_null() & (pl.col("genres") != "") &
            pl.col("startYear").is_not_null() & (pl.col("startYear") >= desde_anyo)
        )
    )

    rt = ratings.lazy().select(["tconst", "averageRating"])

    result = (
        tb.join(rt, on="tconst", how="inner")
        .group_by(["startYear", "genres"])
        .agg([pl.col("averageRating").mean().alias("rating_promedio")])
        .sort(["genres", "startYear"])
        .collect()
    )

    return result


# -------------------------------------------------------------
# KPI 3: Actores con mejor desempeño promedio
# -------------------------------------------------------------
def kpi_actores_exitosos(name_basics, principals, ratings, min_titles=3, top_n=20):
    actores_roles = ["actor", "actress"]

    df = (
        principals.lazy()
        .filter(pl.col("category").is_in(actores_roles))
        .join(ratings.lazy(), on="tconst", how="inner")
        .join(name_basics.lazy(), on="nconst", how="inner")
        .group_by("primaryName")
        .agg([
            pl.count().alias("cantidad_peliculas"),
            pl.mean("averageRating").alias("rating_promedio")
        ])
        .filter(pl.col("cantidad_peliculas") >= min_titles)
        .sort("rating_promedio", descending=True)
        .limit(top_n)
        .collect()
    )
    return df


# -------------------------------------------------------------
# KPI 4: Directores con mejor performance
# -------------------------------------------------------------
def kpi_directores_exitosos(name_basics, principals, ratings, min_titles=3, top_n=20):
    df = (
        principals.lazy()
        .filter(pl.col("category") == "director")
        .join(ratings.lazy(), on="tconst", how="inner")
        .join(name_basics.lazy(), on="nconst", how="inner")
        .group_by("primaryName")
        .agg([
            pl.count().alias("cantidad_peliculas"),
            pl.mean("averageRating").alias("rating_promedio")
        ])
        .filter(pl.col("cantidad_peliculas") >= min_titles)
        .sort("rating_promedio", descending=True)
        .limit(top_n)
        .collect()
    )
    return df


# -------------------------------------------------------------
# KPI 5: Duración promedio por género
# -------------------------------------------------------------
def kpi_duracion_por_genero(title_basics):
    tb = (
        normalize_genres(title_basics)
        .select(["tconst", "runtimeMinutes", "genres"])
        .with_columns([pl.col("runtimeMinutes").cast(pl.Float64)])
        .filter(pl.col("runtimeMinutes").is_not_null() & (pl.col("runtimeMinutes") > 0))
        .explode("genres")
        .filter(pl.col("genres").is_not_null() & (pl.col("genres") != ""))
    )

    result = (
        tb.group_by("genres")
        .agg([pl.col("runtimeMinutes").mean().alias("duracion_promedio")])
        .sort("duracion_promedio", descending=True)
        .collect()
    )

    return result


# -------------------------------------------------------------
# Export a una planilla Excel con múltiples hojas
# -------------------------------------------------------------
def export_to_excel(kpi_dict, out_path: Path):
    # kpi_dict: {"sheet_name": polars_df, ...}
    # usamos pandas ExcelWriter por compatibilidad
    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        for sheet_name, pl_df in kpi_dict.items():
            # normalizar nombre de hoja (máx 31 chars, no caracteres inválidos)
            safe_name = sheet_name[:31].replace("/", "_").replace("\\", "_")
            # convertir a pandas y escribir
            df_pd = pl_df.to_pandas()
            df_pd.to_excel(writer, sheet_name=safe_name, index=False)
        


# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
def main():
    print("Cargando datos...")
    name_basics, title_basics, principals, ratings = load_data()

    print("Calculando KPIs...")
    k1 = kpi_popularidad_generos(title_basics, ratings)
    k2 = kpi_evolucion_generos(title_basics, ratings, desde_anyo=2000)
    k3 = kpi_actores_exitosos(name_basics, principals, ratings)
    k4 = kpi_directores_exitosos(name_basics, principals, ratings)
    k5 = kpi_duracion_por_genero(title_basics)

    # Guardar cada KPI como hoja en un mismo Excel
    kpis = {
        "popularidad_por_genero": k1,
        "evolucion_rating_genero": k2,
        "actores_exitosos": k3,
        "directores_exitosos": k4,
        "duracion_promedio_genero": k5,
    }

    print(f"Exportando planilla a: {OUT_XLSX}")
    export_to_excel(kpis, OUT_XLSX)
    print("Export complete. Archivo listo para el Analista Funcional.")


if __name__ == "__main__":
    main()
