"""Acesso direto ao Google Cloud (BigQuery) para este projeto.

Suporta dois modos de autenticacao:
1) ADC (Application Default Credentials): `gcloud auth application-default login`
2) Service Account JSON: argumento `--credentials-json` ou env `GOOGLE_APPLICATION_CREDENTIALS`
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import google.auth
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import bigquery
from google.oauth2 import service_account


DEFAULT_PROJECT_ID = "projetofinalia-488312"
DEFAULT_LOCAL_CREDENTIALS = Path(__file__).with_name("google_credentials.json")


def resolver_arquivo_credenciais(credentials_json: str | None) -> str | None:
    """Resolve caminho de credenciais por argumento/env ou arquivo local padrao."""
    if credentials_json:
        return credentials_json

    if DEFAULT_LOCAL_CREDENTIALS.exists():
        return str(DEFAULT_LOCAL_CREDENTIALS)

    return None


def criar_cliente_bigquery(project_id: str, credentials_json: str | None = None) -> bigquery.Client:
    """Cria cliente BigQuery usando Service Account JSON ou ADC."""
    credentials_json = resolver_arquivo_credenciais(credentials_json)

    if credentials_json:
        if not os.path.exists(credentials_json):
            raise FileNotFoundError(
                f"Arquivo de credenciais nao encontrado: {credentials_json}"
            )

        credentials = service_account.Credentials.from_service_account_file(
            credentials_json,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=project_id, credentials=credentials)

    try:
        credentials, discovered_project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
    except DefaultCredentialsError as exc:
        raise RuntimeError(
            "Credenciais nao encontradas. Use --credentials-json <arquivo.json> "
            "ou rode: gcloud auth application-default login. "
            "Opcional: salve em service_data/bigquery/google_credentials.json"
        ) from exc

    effective_project = project_id or discovered_project
    if not effective_project:
        raise RuntimeError(
            "Nao foi possivel determinar o project_id automaticamente. "
            "Informe --project-id."
        )

    return bigquery.Client(project=effective_project, credentials=credentials)


def testar_acesso(client: bigquery.Client) -> None:
    """Executa uma consulta simples para validar acesso."""
    query = "SELECT CURRENT_TIMESTAMP() AS ts"
    resultado = list(client.query(query).result())

    datasets = list(client.list_datasets(max_results=5))
    print("Conexao com Google Cloud estabelecida com sucesso.")
    print(f"Projeto conectado: {client.project}")
    print(f"Teste SQL OK (timestamp): {resultado[0]['ts']}")
    if datasets:
        nomes = ", ".join(ds.dataset_id for ds in datasets)
        print(f"Datasets visiveis (ate 5): {nomes}")
    else:
        print("Nenhum dataset visivel para esta conta/projeto.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Acesso direto ao Google Cloud BigQuery para o projeto.",
    )
    parser.add_argument(
        "--project-id",
        default=os.getenv("GCP_PROJECT_ID", DEFAULT_PROJECT_ID),
        help="ID do projeto GCP (padrao: projetofinalia-488312).",
    )
    parser.add_argument(
        "--credentials-json",
        default=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        help="Caminho para arquivo JSON da Service Account (opcional).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    cliente = criar_cliente_bigquery(
        project_id=args.project_id,
        credentials_json=args.credentials_json,
    )
    testar_acesso(cliente)