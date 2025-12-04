import polars as pl
import os

def process_file(input_path: str, output_path: str):
    print(f"Procesando: {input_path}")

    df = pl.read_csv(
        input_path,
        separator="\t",
        null_values="\\N",
        infer_schema_length=500000,
        quote_char=None,          # IGNORA comillas rotas del dataset IMDB
        ignore_errors=False
    )

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.write_parquet(output_path)

    print(f"  → Guardado en: {output_path}")


def main():
    base_dir = os.path.dirname(os.path.dirname(__file__))

    landing_imdb = os.path.join(base_dir, "datalake", "landing", "imdb")
    raw_imdb = os.path.join(base_dir, "datalake", "raw", "imdb")

    archivos = {
        "name.basics.tsv.gz": "name_basics.parquet",
        "title.basics.tsv.gz": "title_basics.parquet",
        "title.principals.tsv.gz": "title_principals.parquet",
        "title.ratings.tsv.gz": "title_ratings.parquet",
    }

    for entrada, salida in archivos.items():
        input_path = os.path.join(landing_imdb, entrada)
        output_path = os.path.join(raw_imdb, salida)

        process_file(input_path, output_path)

    print("\nIngesta completada sin errores.")


if __name__ == "__main__":
    main()
