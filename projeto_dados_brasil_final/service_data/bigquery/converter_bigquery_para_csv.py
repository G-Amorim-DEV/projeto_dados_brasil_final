"""Compat wrapper: encaminha para o novo pipeline cloud-first."""

import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from service_data.bigquery.construir_dataset_unificado import (
    executar_pipeline,
    exportar_csv_local,
    parse_args,
    validar_amostra,
)


if __name__ == "__main__":
    args = parse_args()
    executar_pipeline(
        project_id=args.project_id,
        analytics_dataset=args.analytics_dataset,
        analytics_table=args.analytics_table,
        mode=args.mode,
        lookback_months=args.lookback_months,
    )
    validar_amostra(
        project_id=args.project_id,
        analytics_dataset=args.analytics_dataset,
        analytics_table=args.analytics_table,
    )
    if args.export_local_csv:
        exportar_csv_local(
            project_id=args.project_id,
            analytics_dataset=args.analytics_dataset,
            source_table=args.export_source_table or args.analytics_table,
            local_csv_path=args.local_csv_path,
            max_rows=args.local_max_rows,
        )
