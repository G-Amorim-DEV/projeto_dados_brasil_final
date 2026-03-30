import os
from pathlib import Path
import google.auth
from google.cloud import bigquery
from google.auth.exceptions import DefaultCredentialsError
from google.auth.transport.requests import Request
from google.oauth2 import service_account

# Configurações Padrão
PROJECT_ID = "projetofinalia-488312"
# O script busca o arquivo na mesma pasta onde ele está localizado
CREDENTIALS_PATH = Path(__file__).parent / "google_credentials.json"

def obter_cliente(project_id=None, credentials_path=None):
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    mensagens_erro = []
    projeto_alvo = project_id or PROJECT_ID

    caminhos_credenciais = []
    env_credentials = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if env_credentials:
        caminhos_credenciais.append(Path(env_credentials).expanduser())
    if credentials_path:
        caminhos_credenciais.append(Path(credentials_path).expanduser())
    caminhos_credenciais.append(CREDENTIALS_PATH)

    for caminho in caminhos_credenciais:
        if not caminho.exists():
            continue
        try:
            credentials = service_account.Credentials.from_service_account_file(
                str(caminho),
                scopes=scopes,
            )
            # Valida o JWT antes de retornar o cliente para falhar cedo em chave inválida.
            credentials.refresh(Request())
            return bigquery.Client(project=projeto_alvo, credentials=credentials)
        except Exception as erro:
            mensagens_erro.append(f"{caminho}: {erro}")

    try:
        credentials_adc, projeto_adc = google.auth.default(scopes=scopes)
        projeto_final = projeto_alvo or projeto_adc
        if not projeto_final:
            raise RuntimeError("Projeto GCP não definido para usar ADC.")
        return bigquery.Client(project=projeto_final, credentials=credentials_adc)
    except DefaultCredentialsError as erro_adc:
        detalhes = " | ".join(mensagens_erro) if mensagens_erro else "nenhum arquivo de chave utilizável encontrado"
        raise RuntimeError(
            "Falha ao autenticar no BigQuery. "
            f"Detalhes: {detalhes}. "
            "Soluções: 1) gere uma nova chave JSON válida para a service account; "
            "ou 2) rode 'gcloud auth application-default login' e use ADC. "
            f"Erro ADC: {erro_adc}"
        )

if __name__ == "__main__":
    try:
        client = obter_cliente()
        print(f"Conexão estabelecida com sucesso ao projeto: {client.project}")
    except Exception as e:
        print(f"Erro ao conectar: {e}")