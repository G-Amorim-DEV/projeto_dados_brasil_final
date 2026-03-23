"""
Módulo de validação de dados: detecta e quantifica valores nulos em tabelas BigQuery.
Gera relatório com contagens e percentuais de valores ausentes por coluna.
"""

import argparse
import os

from google.cloud import bigquery


def montar_query_nulls(table_fqn, schema):
    """
    Monta query BigQuery que conta valores NULL por coluna.
    
    Args:
        table_fqn: Fully qualified name (project.dataset.table)
        schema: Schema da tabela (lista de Field objects)
    
    Returns:
        SQL string com UNION ALL de COUNTIF para cada coluna
    """
    selects = []

    for field in schema:
        col = field.name
        selects.append(
            f"""
SELECT
  '{col}' AS coluna,
  COUNTIF(`{col}` IS NULL) AS qtd_nulls,
  COUNT(*) AS total_linhas,
  SAFE_MULTIPLY(SAFE_DIVIDE(COUNTIF(`{col}` IS NULL), COUNT(*)), 100.0) AS percentual_nulls
FROM `{table_fqn}`
""".strip()
        )

    return "\nUNION ALL\n".join(selects)


def verificar_nulls_bigquery(project_id, dataset, table, top_n):
    client = bigquery.Client(project=project_id)
    table_fqn = f"{project_id}.{dataset}.{table}"
    table_obj = client.get_table(table_fqn)

    query_base = montar_query_nulls(table_fqn, table_obj.schema)
    query = (
        f"SELECT * FROM ({query_base}) "
        "WHERE qtd_nulls > 0 "
        "ORDER BY percentual_nulls DESC, qtd_nulls DESC "
        f"LIMIT {int(top_n)}"
    )

    df = client.query(query).result().to_dataframe()

    if df.empty:
        print("Nenhuma coluna com valores nulos encontrada.")
        return

    df["percentual_nulls"] = df["percentual_nulls"].round(2)
    print("\n--- Relatorio de Nulls (BigQuery) ---")
    print(df.to_string(index=False))


def parse_args():
    parser = argparse.ArgumentParser(description="Conta nulls por coluna no BigQuery.")
    parser.add_argument("--project-id", default=os.getenv("GCP_PROJECT_ID", "projetofinalia-488312"), required=False)
    parser.add_argument("--dataset", default=os.getenv("BQ_ANALYTICS_DATASET", "analytics"))
    parser.add_argument("--table", default=os.getenv("BQ_FEATURE_STORE_TABLE", "feature_store_beneficios"))
    parser.add_argument("--top-n", type=int, default=30)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    verificar_nulls_bigquery(args.project_id, args.dataset, args.table, args.top_n)
