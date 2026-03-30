import pandas as pd
from projeto_dados_brasil_final.service_data.bigquery.construir_dataset_unificado import (
    obter_cliente, 
    carregar_datasets, 
    montar_query_full
)

def gerar_dataframe_analise():
    # 1. Configurações básicas
    project_id = "projetofinalia-488312"
    dataset_destno = "analytics"
    tabela_destino = "feature_store_beneficios"
    
    print("Iniciando conexão com BigQuery...")
    client = obter_cliente(project_id=project_id)
    datasets = carregar_datasets()

    # 2. Monta a Query (Modo FULL para funcionar no Sandbox/Free Tier)
    # A agregação de 60GB acontece nos servidores do Google, retornando apenas o resumo por município.
    query = montar_query_full(project_id, dataset_destno, tabela_destino, datasets)
    
    print("Executando agregação massiva na nuvem (isso economiza sua memória local)...")
    try:
        # Executa a query e já converte o resultado direto para DataFrame
        # O método .to_dataframe() é eficiente para baixar os dados agregados
        df = client.query(query).to_dataframe()
        
        if df.empty:
            print("Atenção: O retorno do BigQuery está vazio.")
            return None

        # 3. Exportação para CSV Local
        nome_arquivo = "dataset_municipios_final.csv"
        df.to_csv(nome_arquivo, index=False, encoding="utf-8")
        
        print(f"Sucesso! DataFrame gerado com {len(df)} linhas.")
        print(f"Arquivo salvo em: {nome_arquivo}")
        
        return df

    except Exception as e:
        print(f"Erro ao processar dados: {e}")
        print("Dica: Verifique se o billing está desativado. No Sandbox, use apenas o modo FULL.")
        return None

if __name__ == "__main__":
    df_final = gerar_dataframe_analise()
    if df_final is not None:
        print(df_final.head())