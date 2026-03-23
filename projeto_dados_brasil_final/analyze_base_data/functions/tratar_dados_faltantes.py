"""
Módulo de limpeza de dados: trata valores faltantes (NULL) em colunas numéricas.
Estratégia: Imputação por MEDIANA calculada a partir do dataset original.
Cria nova tabela com dados tratados, mantendo estrutura particionada/clusterizada.
"""

import argparse
import os

from google.cloud import bigquery


NUMERIC_COLS = [
    "quantidade_beneficiarios",
    "valor_pago",
    "saldo_emprego",
    "media_salario_mensal",
    "pib_per_capita",
    "indice_gini",
    "populacao",
    "beneficiarios_por_1000_habitantes",
    "proficiencia_media",
    "taxa_alfabetizacao",
]


def montar_query_limpeza(project_id, dataset, origem_table, destino_table):
    """
    Monta query BigQuery para imputar nulos com MEDIANA.
    
    Estratégia:
    - Calcula APPROX_QUANTILE mediana de cada coluna numérica
    - COALESCE: usa mediana para substituir NULL
    - Cria tabela clusterizada por id_municipio, particionada por data_referencia
    
    Args:
        project_id: ID do projeto GCP
        dataset: Dataset de origem e destino
        origem_table: Tabela com dados brutos (com NULLs)
        destino_table: Tabela limpa (com dados imputados)
    
    Returns:
        SQL string com CREATE OR REPLACE TABLE + CTEs de medianas
    """
    origem = f"`{project_id}.{dataset}.{origem_table}`"
    destino = f"`{project_id}.{dataset}.{destino_table}`"

    median_ctes = []
    fills = []
    for col in NUMERIC_COLS:
        median_ctes.append(
            f"med_{col} AS (SELECT APPROX_QUANTILES({col}, 100)[OFFSET(50)] AS mediana FROM {origem})"
        )
        fills.append(f"COALESCE(src.{col}, med_{col}.mediana) AS {col}")

    ctes = ",\n".join(median_ctes)
    fill_select = ",\n    ".join(fills)

    return f"""
CREATE OR REPLACE TABLE {destino}
PARTITION BY data_referencia
CLUSTER BY id_municipio AS
WITH
{ctes}
SELECT
    src.id_municipio,
    src.ano,
    src.mes,
    src.data_referencia,
    {fill_select},
    src.atualizado_em
FROM {origem} AS src
CROSS JOIN med_quantidade_beneficiarios
CROSS JOIN med_valor_pago
CROSS JOIN med_saldo_emprego
CROSS JOIN med_media_salario_mensal
CROSS JOIN med_pib_per_capita
CROSS JOIN med_indice_gini
CROSS JOIN med_populacao
CROSS JOIN med_beneficiarios_por_1000_habitantes
CROSS JOIN med_proficiencia_media
CROSS JOIN med_taxa_alfabetizacao
WHERE src.id_municipio IS NOT NULL
  AND src.ano IS NOT NULL
  AND src.mes BETWEEN 1 AND 12;
"""


def tratar_dados_faltantes(project_id, dataset, origem_table, destino_table):
    client = bigquery.Client(project=project_id)
    query = montar_query_limpeza(project_id, dataset, origem_table, destino_table)
    client.query(query).result()

    print(
        "Tabela tratada criada com sucesso em "
        f"{project_id}.{dataset}.{destino_table}"
    )


def parse_args():
    parser = argparse.ArgumentParser(description="Cria versao tratada da feature store no BigQuery.")
    parser.add_argument("--project-id", default=os.getenv("GCP_PROJECT_ID", "projetofinalia-488312"), required=False)
    parser.add_argument("--dataset", default=os.getenv("BQ_ANALYTICS_DATASET", "analytics"))
    parser.add_argument("--source-table", default=os.getenv("BQ_FEATURE_STORE_TABLE", "feature_store_beneficios"))
    parser.add_argument("--target-table", default=os.getenv("BQ_FEATURE_STORE_CLEAN_TABLE", "feature_store_beneficios_clean"))
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    tratar_dados_faltantes(args.project_id, args.dataset, args.source_table, args.target_table)
