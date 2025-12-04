import polars as pl
import os


def curate_name_basics(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .with_columns([
            pl.col("birthYear").cast(pl.Int64, strict=False),
            pl.col("deathYear").cast(pl.Int64, strict=False)
        ])
    )


def curate_title_basics(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .with_columns([
            pl.col("isAdult").cast(pl.Int8, strict=False),
            pl.col("startYear").cast(pl.Int64, strict=False),
            pl.col("endYear").cast(pl.Int64, strict=False),
            pl.col("runtimeMinutes").cast(pl.Int64, strict=False),
        ])
    )


def curate_title_principals(df: pl.DataFrame) -> pl.DataFrame:
    return df


def curate_title_ratings(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .with_columns([
            pl.col("averageRating").cast(pl.Float64, strict=False),
            pl.col("numVotes").cast(pl.Int64, strict=False)
        ])
    )


def main():
    base_dir = os.path.dirname(os.path.dirname(__file__))

    raw_imdb = os.path.join(base_dir, "datalake", "raw", "imdb")
    curated_imdb = os.path.join(base_dir, "datalake", "curated", "imdb")

    archivos = {
        "name_basics.parquet": ("name_basics_curated.parquet", curate_name_basics),
        "title_basics.parquet": ("title_basics_curated.parquet", curate_title_basics),
        "title_principals.parquet": ("title_principals_curated.parquet", curate_title_principals),
        "title_ratings.parquet": ("title_ratings_curated.parquet", curate_title_ratings),
    }

    for archivo_raw, (archivo_curado, funcion) in archivos.items():
        input_path = os.path.join(raw_imdb, archivo_raw)
        output_path = os.path.join(curated_imdb, archivo_curado)

        print(f"Curando: {input_path}")

        df = pl.read_parquet(input_path)
        df_curado = funcion(df)

        os.makedirs(curated_imdb, exist_ok=True)
        df_curado.write_parquet(output_path)

        print(f" → Guardado en: {output_path}")

    print("\nCurado completo ✔")


if __name__ == "__main__":
    main()
