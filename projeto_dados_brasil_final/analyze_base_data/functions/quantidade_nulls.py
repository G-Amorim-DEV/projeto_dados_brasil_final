import pandas as pd
import os
import json

# Caminhos baseados na estrutura do seu projeto
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "../../"))
BASE_DATA_PATH = os.path.join(PROJECT_ROOT, "base_data")
JSON_PATH = os.path.join(PROJECT_ROOT, "service_data", "bigquery", "data_base_bigquery.json")

def verificar_nulos_no_repositorio():
    # Carrega as definições do seu banco de dados
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        datasets = json.load(f)

    relatorio_nulos = []

    for categoria, tabelas in datasets.items():
        for nome, dados in tabelas.items():
            # Reconstrói o caminho onde o conversor salvou o CSV
            file_path = os.path.join(BASE_DATA_PATH, dados["folder"], dados["output"])
            
            if os.path.exists(file_path):
                print(f"Analisando: {dados['output']}...")
                try:
                    df = pd.read_csv(file_path, keep_default_na=False, na_values=None)
                except pd.errors.EmptyDataError:
                    print(f"Arquivo vazio ou sem colunas: {dados['output']}. Pulando...")
                    continue
                # Calcula nulos por coluna
                nulos = df.isnull().sum()
                total_linhas = len(df)
                for coluna, qtd in nulos.items():
                    if qtd > 0:
                        percentual = (qtd / total_linhas) * 100
                        relatorio_nulos.append({
                            "Arquivo": dados["output"],
                            "Coluna": coluna,
                            "Nulos": qtd,
                            "Percentual": f"{percentual:.2f}%"
                        })
            else:
                print(f"Aviso: Arquivo {dados['output']} não encontrado.")

    # Exibe o resultado final
    if relatorio_nulos:
        df_relatorio = pd.DataFrame(relatorio_nulos)
        print("\n--- Relatório de Valores Ausentes ---")
        print(df_relatorio.to_string(index=False))
    else:
        print("\nNenhum valor nulo encontrado nos arquivos processados.")

if __name__ == "__main__":
    verificar_nulos_no_repositorio()