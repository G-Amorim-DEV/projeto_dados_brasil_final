"""
Módulo de validação de dados: calcula percentual combinado de NULL + vazios.
Para STRING: conta IS NULL OU TRIM = ''
Para outros tipos: conta IS NULL
Gera relatório consolidado de dados ausentes por coluna.
"""

import argparse
import os

from google.cloud import bigquery


def montar_query_percentual(table_fqn, schema):
    """
    Monta query BigQuery que calcula % de ausentes (NULL + vazios).
    
    Args:
        table_fqn: Fully qualified name (project.dataset.table)
        schema: Schema da tabela (lista de Field objects)
    
    Returns:
        SQL string com percentual de ausentes por coluna
    """
    selects = []

    for field in schema:
        col = field.name
        if field.field_type.upper() == "STRING":
            ausente_expr = f"COUNTIF(`{col}` IS NULL OR TRIM(`{col}`) = '')"
        else:
            ausente_expr = f"COUNTIF(`{col}` IS NULL)"

        selects.append(
            f"""
SELECT
  '{col}' AS coluna,
  {ausente_expr} AS qtd_ausentes,
  COUNT(*) AS total_linhas,
  SAFE_MULTIPLY(SAFE_DIVIDE({ausente_expr}, COUNT(*)), 100.0) AS percentual_ausentes
FROM `{table_fqn}`
""".strip()
        )

    return "\nUNION ALL\n".join(selects)


def calcular_percentual_ausentes(project_id, dataset, table, top_n):
    client = bigquery.Client(project=project_id)
    table_fqn = f"{project_id}.{dataset}.{table}"
    schema = client.get_table(table_fqn).schema

    query = (
        f"SELECT * FROM ({montar_query_percentual(table_fqn, schema)}) "
        "ORDER BY percentual_ausentes DESC, qtd_ausentes DESC "
        f"LIMIT {int(top_n)}"
    )

    df = client.query(query).result().to_dataframe()
    df["percentual_ausentes"] = df["percentual_ausentes"].round(2)

    print("\n--- Percentual de Null + Vazio por Coluna ---")
    print(df.to_string(index=False))


def parse_args():
    parser = argparse.ArgumentParser(description="Calcula percentual de ausentes por coluna.")
    parser.add_argument("--project-id", default=os.getenv("GCP_PROJECT_ID", "projetofinalia-488312"), required=False)
    parser.add_argument("--dataset", default=os.getenv("BQ_ANALYTICS_DATASET", "analytics"))
    parser.add_argument("--table", default=os.getenv("BQ_FEATURE_STORE_TABLE", "feature_store_beneficios"))
    parser.add_argument("--top-n", type=int, default=30)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    calcular_percentual_ausentes(args.project_id, args.dataset, args.table, args.top_n)
