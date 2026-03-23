import os
from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

# Configurações Padrão
PROJECT_ID = "projetofinalia-488312"
# O script busca o arquivo na mesma pasta onde ele está localizado
CREDENTIALS_PATH = Path(__file__).parent / "google_credentials.json"

def obter_cliente():
    if not CREDENTIALS_PATH.exists():
        raise FileNotFoundError(f"Arquivo de credenciais não encontrado em: {CREDENTIALS_PATH}")
    
    credentials = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS_PATH),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(project=PROJECT_ID, credentials=credentials)

if __name__ == "__main__":
    try:
        client = obter_cliente()
        print(f"Conexão estabelecida com sucesso ao projeto: {client.project}")
    except Exception as e:
        print(f"Erro ao conectar: {e}")