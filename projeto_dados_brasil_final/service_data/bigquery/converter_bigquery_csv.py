import os
import json
import subprocess

# =========================
# CAMINHOS
# =========================

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

PROJECT_ROOT = os.path.abspath(
    os.path.join(CURRENT_DIR, "../../")
)

JSON_PATH = os.path.join(
    CURRENT_DIR,
    "data_base_bigquery.json"
)

BASE_DATA_PATH = os.path.join(
    PROJECT_ROOT,
    "base_data"
)

# =========================
# CRIAR PASTA
# =========================

def criar_pasta(path):

    if not os.path.exists(path):
        os.makedirs(path)

# =========================
# CARREGAR JSON
# =========================

def carregar_datasets():

    with open(JSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# =========================
# EXPORTAR TABELA
# =========================

def exportar_tabela(dataset, table, pasta, output):

    criar_pasta(os.path.join(BASE_DATA_PATH, pasta))

    output_path = os.path.join(BASE_DATA_PATH, pasta, output)

    tabela = f"{dataset}.{table}"

    query = f"SELECT * FROM `{tabela}`"

    comando = [
        "bq",
        "query",
        "--use_legacy_sql=false",
        "--format=csv",
        "--max_rows=1000000000",
        query
    ]

    print(f"Exportando {tabela}")

    with open(output_path, "w", encoding="utf-8") as f:
        subprocess.run(comando, stdout=f)

    print(f"Salvo em: {output_path}")

# =========================
# PIPELINE
# =========================

def executar_pipeline():

    datasets = carregar_datasets()

    for categoria, tabelas in datasets.items():

        print(f"\nCategoria: {categoria}\n")

        for nome, dados in tabelas.items():

            dataset = dados["dataset"]
            table = dados["table"]
            output = dados["output"]
            folder = dados["folder"]

            try:

                exportar_tabela(dataset, table, folder, output)

            except Exception as e:

                print(f"Erro ao baixar {nome}: {e}")

# =========================
# EXECUÇÃO
# =========================

if __name__ == "__main__":

    executar_pipeline()