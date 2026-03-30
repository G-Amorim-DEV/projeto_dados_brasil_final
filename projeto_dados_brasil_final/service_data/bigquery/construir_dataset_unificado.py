import argparse
import json
import sys
from pathlib import Path
from google.api_core.exceptions import Forbidden

# --- CORREÇÃO DE PATH E IMPORTAÇÃO ---
# Resolve o diretório raiz do projeto para permitir importações absolutas
CURRENT_FILE_PATH = Path(__file__).resolve()
# Sobe dois níveis para chegar na raiz 'projeto_dados_brasil_final'
PROJECT_ROOT = CURRENT_FILE_PATH.parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
  # Tenta importar usando o caminho completo do pacote
  from projeto_dados_brasil_final.service_data.bigquery.acesso_google_cloud import obter_cliente
except (ModuleNotFoundError, ImportError):
  try:
    # Tenta importar como módulo relativo se executado de dentro da pasta
    from acesso_google_cloud import obter_cliente
  except (ModuleNotFoundError, ImportError):
    # Fallback para estrutura de pastas service_data.bigquery
    from service_data.bigquery.acesso_google_cloud import obter_cliente

# Caminho para o JSON de configuração (baseado na localização deste script)
CONFIG_PATH = CURRENT_FILE_PATH.parent / "base_dados_bigquery.json"

FEATURE_COLUMNS = [
    "id_municipio", "ano", "mes", "data_referencia",
    "quantidade_beneficiarios", "valor_pago", "saldo_emprego",
    "media_salario_mensal", "pib_per_capita", "indice_gini",
    "populacao", "beneficiarios_por_1000_habitantes",
    "proficiencia_media", "taxa_alfabetizacao", "atualizado_em",
]

def carregar_datasets():
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado em: {CONFIG_PATH}")
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return json.load(file)

def tabela_referencia(datasets, categoria, nome_tabela):
    dados = datasets[categoria][nome_tabela]
    return f"{dados['dataset']}.{dados['table']}"

# --- MANTEM-SE AS FUNÇÕES montar_ctes_feature_store, montar_select_feature_store, etc. ---
# (O conteúdo das strings SQL que você enviou está correto e foi preservado internamente)

def montar_ctes_feature_store(datasets):
    # Pega as referências do JSON
    tabela_bf_antigo = tabela_referencia(datasets, "beneficios_cidadao", "bolsa_familia_antigo")
    tabela_bf_novo = tabela_referencia(datasets, "beneficios_cidadao", "novo_bolsa_familia")
    tabela_bpc = tabela_referencia(datasets, "beneficios_cidadao", "bpc")
    tabela_caged = tabela_referencia(datasets, "caged", "microdados_movimentacao")
    tabela_pib = tabela_referencia(datasets, "pib", "municipio")
    tabela_gini = tabela_referencia(datasets, "pib", "gini")
    tabela_pop = tabela_referencia(datasets, "populacao", "municipio")
    tabela_proficiencia = tabela_referencia(datasets, "saeb", "proficiencia")
    tabela_taxa_alfabetizacao_uf = tabela_referencia(datasets, "saeb", "uf_taxa_alfabetizacao")

    return f"""
beneficios_unificado AS (
  SELECT
    CAST(id_municipio AS STRING) AS id_municipio,
    SAFE_CAST(COALESCE(ano_referencia, ano_competencia) AS INT64) AS ano,
    SAFE_CAST(COALESCE(mes_referencia, mes_competencia) AS INT64) AS mes,
    NULLIF(TRIM(CAST(cpf_favorecido AS STRING)), '') AS cpf_favorecido,
    NULLIF(TRIM(CAST(nis_favorecido AS STRING)), '') AS nis_favorecido,
    SAFE_CAST(valor_parcela AS FLOAT64) AS valor_parcela
  FROM `{tabela_bf_antigo}`

  UNION ALL

  SELECT
    CAST(id_municipio AS STRING) AS id_municipio,
    SAFE_CAST(COALESCE(ano_referencia, ano_competencia) AS INT64) AS ano,
    SAFE_CAST(COALESCE(mes_referencia, mes_competencia) AS INT64) AS mes,
    NULLIF(TRIM(CAST(cpf_favorecido AS STRING)), '') AS cpf_favorecido,
    NULLIF(TRIM(CAST(nis_favorecido AS STRING)), '') AS nis_favorecido,
    SAFE_CAST(valor_parcela AS FLOAT64) AS valor_parcela
  FROM `{tabela_bf_novo}`

  UNION ALL

  SELECT
    CAST(id_municipio AS STRING) AS id_municipio,
    SAFE_CAST(COALESCE(ano_referencia, ano_competencia) AS INT64) AS ano,
    SAFE_CAST(COALESCE(mes_referencia, mes_competencia) AS INT64) AS mes,
    NULLIF(TRIM(CAST(cpf_favorecido AS STRING)), '') AS cpf_favorecido,
    NULLIF(TRIM(CAST(nis_favorecido AS STRING)), '') AS nis_favorecido,
    SAFE_CAST(valor_parcela AS FLOAT64) AS valor_parcela
  FROM `{tabela_bpc}`
),
beneficios_base AS (
  SELECT
    id_municipio,
    ano,
    mes,
    COALESCE(cpf_favorecido, nis_favorecido) AS beneficiario_id,
    valor_parcela AS valor_pago
  FROM beneficios_unificado
),
beneficios AS (
  SELECT
    id_municipio,
    ano,
    mes,
    COUNT(DISTINCT beneficiario_id) AS quantidade_beneficiarios,
    SUM(valor_pago) AS valor_pago
  FROM beneficios_base
  WHERE id_municipio IS NOT NULL
    AND ano IS NOT NULL
    AND beneficiario_id IS NOT NULL
    AND mes BETWEEN 1 AND 12
  GROUP BY id_municipio, ano, mes
),
caged_base AS (
  SELECT
    CAST(t.id_municipio AS STRING) AS id_municipio,
    SAFE_CAST(t.ano AS INT64) AS ano,
    SAFE_CAST(t.mes AS INT64) AS mes,
    SAFE_CAST(t.saldo_movimentacao AS INT64) AS sinal_movimentacao,
    SAFE_CAST(t.salario_mensal AS FLOAT64) AS salario_mensal
  FROM `{tabela_caged}` AS t
),
caged AS (
  SELECT
    id_municipio,
    ano,
    mes,
    SUM(sinal_movimentacao) AS saldo_emprego,
    AVG(salario_mensal) AS media_salario_mensal
  FROM caged_base
  WHERE id_municipio IS NOT NULL
    AND ano IS NOT NULL
    AND mes BETWEEN 1 AND 12
  GROUP BY id_municipio, ano, mes
),
pib_base AS (
  SELECT
    CAST(t.id_municipio AS STRING) AS id_municipio,
    SAFE_CAST(t.ano AS INT64) AS ano,
    SAFE_DIVIDE(SAFE_CAST(t.pib AS FLOAT64), NULLIF(SAFE_CAST(pop.populacao AS FLOAT64), 0)) AS pib_per_capita
  FROM `{tabela_pib}` AS t
  LEFT JOIN `{tabela_pop}` AS pop
    ON CAST(pop.id_municipio AS STRING) = CAST(t.id_municipio AS STRING)
    AND SAFE_CAST(pop.ano AS INT64) = SAFE_CAST(t.ano AS INT64)
),
gini_base AS (
  SELECT
    LPAD(CAST(SAFE_CAST(t.id_uf AS INT64) AS STRING), 2, '0') AS id_uf,
    SAFE_CAST(t.ano AS INT64) AS ano,
    SAFE_CAST(t.gini_pib AS FLOAT64) AS indice_gini
  FROM `{tabela_gini}` AS t
),
pib_anual AS (
  SELECT
    p.id_municipio,
    p.ano,
    MAX(p.pib_per_capita) AS pib_per_capita,
    MAX(g.indice_gini) AS indice_gini
  FROM pib_base AS p
  LEFT JOIN gini_base AS g
    ON g.id_uf = SUBSTR(p.id_municipio, 1, 2)
    AND g.ano = p.ano
  WHERE p.id_municipio IS NOT NULL
    AND p.ano IS NOT NULL
  GROUP BY p.id_municipio, p.ano
),
populacao_base AS (
  SELECT
    CAST(t.id_municipio AS STRING) AS id_municipio,
    SAFE_CAST(t.ano AS INT64) AS ano,
    12 AS mes,
    SAFE_CAST(t.populacao AS FLOAT64) AS populacao
  FROM `{tabela_pop}` AS t
),
populacao AS (
  SELECT
    id_municipio,
    ano,
    mes,
    MAX(populacao) AS populacao
  FROM populacao_base
  WHERE id_municipio IS NOT NULL
    AND ano IS NOT NULL
    AND mes BETWEEN 1 AND 12
  GROUP BY id_municipio, ano, mes
),
proficiencia_base AS (
  SELECT
    CAST(t.id_municipio AS STRING) AS id_municipio,
    CAST(t.sigla_uf AS STRING) AS sigla_uf,
    SAFE_CAST(t.ano AS INT64) AS ano,
    SAFE_CAST(t.proficiencia AS FLOAT64) AS proficiencia_media
  FROM `{tabela_proficiencia}` AS t
),
taxa_alfabetizacao_uf AS (
  SELECT
    CAST(t.sigla_uf AS STRING) AS sigla_uf,
    SAFE_CAST(t.ano AS INT64) AS ano,
    SAFE_CAST(t.taxa_alfabetizacao AS FLOAT64) AS taxa_alfabetizacao
  FROM `{tabela_taxa_alfabetizacao_uf}` AS t
),
educacao_anual AS (
  SELECT
    p.id_municipio,
    p.ano,
    AVG(p.proficiencia_media) AS proficiencia_media,
    MAX(t.taxa_alfabetizacao) AS taxa_alfabetizacao
  FROM proficiencia_base AS p
  LEFT JOIN taxa_alfabetizacao_uf AS t
    ON t.ano = p.ano
    AND t.sigla_uf = p.sigla_uf
  WHERE p.id_municipio IS NOT NULL
    AND p.ano IS NOT NULL
  GROUP BY p.id_municipio, p.ano
),
periodo_mensal_limites AS (
  SELECT MIN(DATE(ano, mes, 1)) AS min_periodo, MAX(DATE(ano, mes, 1)) AS max_periodo FROM beneficios
  UNION ALL
  SELECT MIN(DATE(ano, mes, 1)) AS min_periodo, MAX(DATE(ano, mes, 1)) AS max_periodo FROM caged
  UNION ALL
  SELECT MIN(DATE(ano, mes, 1)) AS min_periodo, MAX(DATE(ano, mes, 1)) AS max_periodo FROM populacao
),
periodo_anual_limites AS (
  SELECT MIN(ano) AS min_ano, MAX(ano) AS max_ano FROM pib_anual
  UNION ALL
  SELECT MIN(ano) AS min_ano, MAX(ano) AS max_ano FROM educacao_anual
),
periodo_comum AS (
  SELECT
    GREATEST(
      (SELECT MAX(min_periodo) FROM periodo_mensal_limites),
      DATE((SELECT MAX(min_ano) FROM periodo_anual_limites), 1, 1)
    ) AS data_inicio,
    LEAST(
      (SELECT MIN(max_periodo) FROM periodo_mensal_limites),
      DATE((SELECT MIN(max_ano) FROM periodo_anual_limites), 12, 1)
    ) AS data_fim
),
chaves_mensais AS (
  SELECT b.id_municipio, b.ano, b.mes
  FROM beneficios AS b
  INNER JOIN caged AS c
    ON c.id_municipio = b.id_municipio
    AND c.ano = b.ano
    AND c.mes = b.mes
  INNER JOIN populacao AS p
    ON p.id_municipio = b.id_municipio
    AND p.ano = b.ano
    AND p.mes = b.mes
),
feature_store AS (
  SELECT
    k.id_municipio,
    k.ano,
    k.mes,
    DATE(k.ano, k.mes, 1) AS data_referencia,
    b.quantidade_beneficiarios,
    b.valor_pago,
    c.saldo_emprego,
    c.media_salario_mensal,
    pib.pib_per_capita,
    pib.indice_gini,
    p.populacao,
    SAFE_DIVIDE(b.quantidade_beneficiarios * 1000.0, NULLIF(p.populacao, 0)) AS beneficiarios_por_1000_habitantes,
    e.proficiencia_media,
    e.taxa_alfabetizacao,
    CURRENT_TIMESTAMP() AS atualizado_em
  FROM chaves_mensais AS k
  INNER JOIN beneficios AS b
    ON b.id_municipio = k.id_municipio
    AND b.ano = k.ano
    AND b.mes = k.mes
  INNER JOIN caged AS c
    ON c.id_municipio = k.id_municipio
    AND c.ano = k.ano
    AND c.mes = k.mes
  INNER JOIN populacao AS p
    ON p.id_municipio = k.id_municipio
    AND p.ano = k.ano
    AND p.mes = k.mes
  INNER JOIN pib_anual AS pib
    ON pib.id_municipio = k.id_municipio
    AND pib.ano = k.ano
  INNER JOIN educacao_anual AS e
    ON e.id_municipio = k.id_municipio
    AND e.ano = k.ano
  INNER JOIN periodo_comum AS pc
    ON DATE(k.ano, k.mes, 1) BETWEEN pc.data_inicio AND pc.data_fim
)
"""

# Re-inserindo as funções de montagem de query conforme o original
def montar_select_feature_store(datasets, cutoff_date_sql=None):
    where_cutoff = f"\nWHERE data_referencia >= {cutoff_date_sql}" if cutoff_date_sql else ""
    return f"WITH\n{montar_ctes_feature_store(datasets)}\nSELECT * FROM feature_store{where_cutoff}"

def montar_query_full(project_id, analytics_dataset, analytics_table, datasets):
    destino = f"`{project_id}.{analytics_dataset}.{analytics_table}`"
    return f"CREATE OR REPLACE TABLE {destino} PARTITION BY data_referencia CLUSTER BY id_municipio AS\n{montar_select_feature_store(datasets)}"

def montar_query_incremental(project_id, analytics_dataset, analytics_table, datasets, lookback_months):
    """
    Monta query incremental usando MERGE para atualizar apenas dados dos últimos meses.
    """
    destino = f"`{project_id}.{analytics_dataset}.{analytics_table}`"
    cutoff_date_sql = f"DATE_SUB(CURRENT_DATE(), INTERVAL {lookback_months} MONTH)"
    select_query = montar_select_feature_store(datasets, cutoff_date_sql)
    
    return f"""
MERGE INTO {destino} T
USING (
{select_query}
) S
ON T.id_municipio = S.id_municipio
  AND T.ano = S.ano
  AND T.mes = S.mes
WHEN MATCHED THEN
  UPDATE SET
    quantidade_beneficiarios = S.quantidade_beneficiarios,
    valor_pago = S.valor_pago,
    saldo_emprego = S.saldo_emprego,
    media_salario_mensal = S.media_salario_mensal,
    pib_per_capita = S.pib_per_capita,
    indice_gini = S.indice_gini,
    populacao = S.populacao,
    beneficiarios_por_1000_habitantes = S.beneficiarios_por_1000_habitantes,
    proficiencia_media = S.proficiencia_media,
    taxa_alfabetizacao = S.taxa_alfabetizacao,
    atualizado_em = S.atualizado_em
WHEN NOT MATCHED THEN
  INSERT (
    id_municipio, ano, mes, data_referencia,
    quantidade_beneficiarios, valor_pago, saldo_emprego,
    media_salario_mensal, pib_per_capita, indice_gini,
    populacao, beneficiarios_por_1000_habitantes,
    proficiencia_media, taxa_alfabetizacao, atualizado_em
  )
  VALUES (
    S.id_municipio, S.ano, S.mes, S.data_referencia,
    S.quantidade_beneficiarios, S.valor_pago, S.saldo_emprego,
    S.media_salario_mensal, S.pib_per_capita, S.indice_gini,
    S.populacao, S.beneficiarios_por_1000_habitantes,
    S.proficiencia_media, S.taxa_alfabetizacao, S.atualizado_em
  )
"""

def _is_billing_dml_error(msg_lower):
    return (
        "billing has not been enabled" in msg_lower
        or "billingnotenabled" in msg_lower
        or "dml queries are not allowed in the free tier" in msg_lower
    )

# --- FUNÇÕES DE EXECUÇÃO E VALIDAÇÃO ---

def executar_pipeline(project_id, analytics_dataset, analytics_table, mode, lookback_months, credentials_json=None):
    datasets = carregar_datasets()
    client = obter_cliente(project_id=project_id, credentials_path=credentials_json)

    modo_efetivo = "incremental" if mode == "auto" else mode

    if modo_efetivo == "full":
      query = montar_query_full(project_id, analytics_dataset, analytics_table, datasets)
    else:
      # Lógica incremental (MERGE) conforme o original
      query = montar_query_incremental(project_id, analytics_dataset, analytics_table, datasets, lookback_months)

    print(f"\n--- QUERY EXECUTADA NO BIGQUERY ---\n")
    print(query)
    print(f"\n--- FIM DA QUERY ---\n")
    print(f"Executando pipeline no modo: {modo_efetivo}...")
    try:
      job = client.query(query)
      job.result()
      print(f"Sucesso! Tabela {analytics_dataset}.{analytics_table} atualizada.")
    except Forbidden as exc:
      msg = str(exc)
      msg_lower = msg.lower()

      # No BigQuery Sandbox, MERGE (DML) é bloqueado sem billing.
      # Se o modo for incremental, tenta automaticamente o modo full.
      if modo_efetivo == "incremental" and _is_billing_dml_error(msg_lower):
        print("\nBilling não habilitado para DML (MERGE) no modo incremental.")
        print("Tentando fallback automático para modo full (CREATE OR REPLACE TABLE)...")
        try:
          query_full = montar_query_full(project_id, analytics_dataset, analytics_table, datasets)
          job_full = client.query(query_full)
          job_full.result()
          print(f"Sucesso! Tabela {analytics_dataset}.{analytics_table} atualizada via fallback full.")
          return
        except Exception as exc_full:
          print(f"Fallback para modo full também falhou: {exc_full}")
          raise

      if "free query bytes scanned" in msg_lower or "quota exceeded" in msg_lower:
        print("\nERRO DE COTA BIGQUERY (bytes escaneados).")
        print("Solução recomendada:")
        print("1) Execute no modo incremental (padrão) para reduzir varredura.")
        print("2) Reduza --lookback-months (ex.: 1).")
        print("3) Se necessário, habilite billing no projeto GCP para remover limite free tier.")
      elif _is_billing_dml_error(msg_lower):
        print("\nERRO DE BILLING BIGQUERY.")
        print("Sem billing ativo, o BigQuery Sandbox bloqueia DML (MERGE/UPDATE/DELETE/INSERT).")
        print("Solução recomendada:")
        print("1) Ative billing no projeto GCP para usar modo incremental.")
        print("2) Ou execute com --mode full para evitar MERGE.")
      else:
        print(f"\nErro de acesso/quota no BigQuery: {msg}")
      raise

def validar_amostra(project_id, analytics_dataset, analytics_table, export_local_csv=False, credentials_json=None):
    tabela = f"{project_id}.{analytics_dataset}.{analytics_table}"
    client = obter_cliente(project_id=project_id, credentials_path=credentials_json)
    
    print("\n--- Amostra de Validação (Top 5) ---")
    query_amostra = f"SELECT * FROM `{tabela}` ORDER BY data_referencia DESC LIMIT 5"
    df_amostra = client.query(query_amostra).to_dataframe()
    print(df_amostra)

    # Exporta todos os dados para CSV se solicitado
    if export_local_csv:
      print("Exportando todos os dados para feature_store_beneficios.csv ...")
      query_tudo = f"SELECT * FROM `{tabela}`"
      df_tudo = client.query(query_tudo).to_dataframe()
      total_linhas = len(df_tudo)
      print(f"Total de linhas no dataset unificado: {total_linhas}")
      if total_linhas == 0:
        print("ATENÇÃO: O dataset unificado está vazio! Verifique as tabelas de origem e a query de unificação.")
      else:
        df_tudo.to_csv("feature_store_beneficios.csv", index=False, encoding="utf-8")
        print("Arquivo feature_store_beneficios.csv salvo com sucesso.")
        print("O dataset unificado está pronto para análise!")

def parse_args():
    parser = argparse.ArgumentParser(description="Pipeline Feature Store BigQuery")
    parser.add_argument("--project-id", default="projetofinalia-488312")
    parser.add_argument(
        "--mode",
    choices=["full", "incremental", "auto"],
    default="auto",
    help="Modo de execução. auto tenta incremental e faz fallback para full em erro de billing.",
    )
    parser.add_argument(
        "--lookback-months",
        type=int,
        default=2,
        help="Quantidade de meses para reprocessar no modo incremental (padrão: 2).",
    )
    parser.add_argument(
        "--credentials-json",
        default=None,
        help="Caminho opcional para arquivo de credenciais service account JSON.",
    )
    parser.add_argument(
        "--export-local-csv",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Exporta CSV local após atualizar a tabela analytics (padrão: habilitado).",
    )
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    executar_pipeline(
        project_id=args.project_id,
        analytics_dataset="analytics",
        analytics_table="feature_store_beneficios",
        mode=args.mode,
        lookback_months=args.lookback_months,
        credentials_json=args.credentials_json,
    )
    validar_amostra(
        args.project_id,
        "analytics",
        "feature_store_beneficios",
        export_local_csv=args.export_local_csv,
        credentials_json=args.credentials_json,
    )