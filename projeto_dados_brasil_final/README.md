# Projeto Dados Brasil Final

Projeto orientado a BigQuery para criar um Dataset Unico (Feature Store) no grao `municipio + ano + mes`, sem depender de download de CSVs massivos.

## Objetivo principal

Gerar uma unica tabela para analise e IA, com:
- beneficios sociais (target)
- emprego (CAGED)
- economia (PIB + GINI)
- demografia (populacao)
- educacao (proficiencia + alfabetizacao)

## Estrutura utilizada

- `service_data/bigquery/construir_dataset_unificado.py`: cria/atualiza o dataset unico no BigQuery.
- `service_data/bigquery/base_dados_bigquery.json`: mapeamento das fontes.
- `service_data/bigquery/converter_bigquery_para_csv.py`: entrada legada, redireciona para o pipeline principal.
- `analyze_base_data/functions/quantidade_nulos.py`: auditoria de nulos.
- `analyze_base_data/functions/quantidade_vazios.py`: auditoria de vazios em STRING.
- `analyze_base_data/functions/percentual_nulo_vazio.py`: percentual combinado de ausentes.
- `analyze_base_data/functions/tratar_dados_faltantes.py`: limpeza com imputacao por mediana.

## Como criar o Dataset Unico

### Carga completa (rebuild)

```bash
python service_data/bigquery/construir_dataset_unificado.py \
  --project-id projetofinalia-488312 \
  --analytics-dataset analytics \
  --analytics-table feature_store_beneficios \
  --mode full
```

### Carga incremental (mais economica)

```bash
python service_data/bigquery/construir_dataset_unificado.py \
  --project-id projetofinalia-488312 \
  --analytics-dataset analytics \
  --analytics-table feature_store_beneficios \
  --mode incremental \
  --lookback-months 2
```

### Carga + exportacao local enxuta (CSV)

```bash
python service_data/bigquery/construir_dataset_unificado.py \
  --project-id projetofinalia-488312 \
  --analytics-dataset analytics \
  --analytics-table feature_store_beneficios \
  --mode incremental \
  --lookback-months 2 \
  --export-local-csv \
  --local-csv-path base/dataset_unificado.csv \
  --local-max-rows 300000
```

Observacao:
- o CSV local exporta somente as colunas da feature store (dados necessarios para analise)
- `--local-max-rows` controla volume local para evitar estouro de memoria
- use `--local-max-rows 0` para exportar tudo (se tiver capacidade local)

## Validacao e limpeza (BigQuery)

### 1) Nulos

```bash
python analyze_base_data/functions/quantidade_nulos.py \
  --project-id projetofinalia-488312 \
  --dataset analytics \
  --table feature_store_beneficios \
  --top-n 30
```

### 2) Vazios em colunas STRING

```bash
python analyze_base_data/functions/quantidade_vazios.py \
  --project-id projetofinalia-488312 \
  --dataset analytics \
  --table feature_store_beneficios \
  --top-n 30
```

### 3) Percentual de ausentes (NULL + vazio)

```bash
python analyze_base_data/functions/percentual_nulo_vazio.py \
  --project-id projetofinalia-488312 \
  --dataset analytics \
  --table feature_store_beneficios \
  --top-n 30
```

### 4) Limpeza com imputacao

```bash
python analyze_base_data/functions/tratar_dados_faltantes.py \
  --project-id projetofinalia-488312 \
  --dataset analytics \
  --source-table feature_store_beneficios \
  --target-table feature_store_beneficios_clean
```

## Variaveis de ambiente opcionais

```bash
export GCP_PROJECT_ID="projetofinalia-488312"
export BQ_ANALYTICS_DATASET="analytics"
export BQ_FEATURE_STORE_TABLE="feature_store_beneficios"
export BQ_BUILD_MODE="incremental"
export BQ_INCREMENTAL_LOOKBACK_MONTHS="2"
export BQ_LOCAL_CSV_PATH="base/dataset_unificado.csv"
export BQ_LOCAL_MAX_ROWS="300000"
```

## Observacao importante

A pasta `base/` concentra apenas o CSV unificado local. O pipeline trabalha direto no BigQuery e so exporta este CSV final enxuto para analise local.
