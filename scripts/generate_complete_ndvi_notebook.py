from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent

from notebook_bootstrap import build_runtime_bootstrap_source


def markdown_cell(source: str) -> dict:
    return {"cell_type": "markdown", "metadata": {}, "source": _lines(source)}


def code_cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": _lines(source),
    }


def _lines(source: str) -> list[str]:
    text = dedent(source).strip("\n")
    if not text:
        return []
    return [f"{line}\n" for line in text.splitlines()]


def build_notebook() -> dict:
    cells = [
        markdown_cell(
            """
            # Projeto Completo NDVI - CRISP-DM

            Notebook principal do projeto, organizado em CRISP-DM e alinhado ao objetivo descrito em `info.md`.

            Estrutura adotada:
            - Business Understanding
            - Data Understanding
            - Data Preparation
            - Statistical Analysis
            - Modeling
            - Evaluation
            - Export

            Ambientes suportados:
            - localmente;
            - no Jupyter tradicional;
            - no Google Colab com pasta no Drive.
            """
        ),
        markdown_cell(
            """
            ## Uso no Colab

            1. Coloque o repositorio no Drive, se a execucao for feita a partir dele.
            2. Para nao versionar caminhos locais, use um arquivo `.monolithfarm.paths.json`.
            3. Selecione o perfil com `MONOLITHFARM_PROFILE`.
            4. Para definir o modo explicitamente, use `MONOLITHFARM_NOTEBOOK_MODE=colab`.
            5. Rode a primeira celula para montar o Drive e localizar o projeto.
            6. Variaveis opcionais:
               - `MONOLITHFARM_PROJECT_DIR`
               - `MONOLITHFARM_DATA_DIR`
               - `MONOLITHFARM_OUTPUT_DIR`
            """
        ),
        code_cell(
            build_runtime_bootstrap_source(
                output_subdir="notebook_outputs/complete_ndvi",
                required_packages=["duckdb", "pandas", "plotly", "pyproj", "shapely", "scipy"],
            )
        ),
        code_cell(
            """
            import hashlib
            import base64
            import inspect
            import math
            import tempfile
            from pathlib import Path

            import pandas as pd
            import plotly.express as px
            import plotly.graph_objects as go
            from IPython.display import HTML, Markdown, display

            from farmlab.complete_analysis import (
                build_complete_ndvi_workspace,
                build_ndvi_outliers,
                build_pair_classic_tests,
                build_pair_weekly_gaps,
                build_weekly_correlations,
                save_complete_ndvi_outputs,
            )
            from farmlab.io import discover_dataset_paths, load_ndvi_metadata
            from farmlab.ndvi_crispdm import build_ndvi_crispdm_workspace
            from farmlab.ndvi_crispdm import (
                build_data_audit,
                build_decision_summary,
                build_event_driver_lift,
                build_final_hypothesis_register,
                build_pair_effect_tests,
                build_transition_model_frame,
            )
            from farmlab.ndvi_deepdive import build_ndvi_deep_dive_workspace
            from farmlab.ndvi_deepdive import (
                build_ndvi_events,
                build_ndvi_outlook,
                build_ndvi_pair_diagnostics,
                build_ndvi_phase_timeline,
                build_ops_support_weekly,
            )
            from farmlab.pairwise import build_phase1_workspace
            from farmlab.pairwise import (
                build_area_inventory,
                build_ndvi_clean,
                build_ndvi_weekly,
                build_pairwise_weekly_features,
            )


            paths = discover_dataset_paths(DATA_DIR)
            ndvi_raw = load_ndvi_metadata(paths)
            workspace = build_complete_ndvi_workspace(DATA_DIR)
            dataset_overview = workspace["dataset_overview"]
            numeric_profiles = workspace["numeric_profiles"]
            ndvi_stats_by_area = workspace["ndvi_stats_by_area"]
            ndvi_outliers = workspace["ndvi_outliers"]
            pair_weekly_gaps = workspace["pair_weekly_gaps"]
            pair_classic_tests = workspace["pair_classic_tests"]
            ndvi_trend_tests = workspace["ndvi_trend_tests"]
            weekly_correlations = workspace["weekly_correlations"]

            data_audit = workspace["data_audit"]
            pair_effect_tests = workspace["pair_effect_tests"]
            event_driver_lift = workspace["event_driver_lift"]
            transition_model_frame = workspace["transition_model_frame"]
            transition_model_summary = workspace["transition_model_summary"]
            transition_model_coefficients = workspace["transition_model_coefficients"]
            transition_model_predictions = workspace["transition_model_predictions"]
            final_hypothesis_register = workspace["final_hypothesis_register"]
            decision_summary = workspace["decision_summary"]
            ndvi_phase_timeline = workspace["ndvi_phase_timeline"]
            ndvi_events = workspace["ndvi_events"]
            ndvi_outlook = workspace["ndvi_outlook"]
            ndvi_pair_diagnostics = workspace["ndvi_pair_diagnostics"]
            ndvi_clean = workspace["ndvi_clean"]
            area_inventory = workspace["area_inventory"]
            area_metadata = workspace["area_metadata"]
            weather_daily = workspace["weather_daily"]
            ops_area_daily = workspace["ops_area_daily"]
            miip_daily = workspace["miip_daily"]
            soil_context = workspace["soil_context"]
            deep_dive_gaps = workspace["deep_dive_gaps"]

            source_inventory = pd.DataFrame(
                [
                    {
                        "source_key": key,
                        "path": str(value),
                        "exists": Path(value).exists() if value is not None else False,
                        "kind": "arquivo" if value is not None and Path(value).is_file() else "diretorio" if value is not None else "opcional_ausente",
                    }
                    for key, value in vars(paths).items()
                ]
            )

            pipeline_flow = pd.DataFrame(
                [
                    {
                        "layer": "Notebook",
                        "entrypoint": "build_complete_ndvi_workspace(DATA_DIR)",
                        "defined_in": "farmlab.complete_analysis",
                        "role": "orquestra toda a cadeia e devolve o workspace final",
                        "produces": "dataset_overview, pair_classic_tests, weekly_correlations e demais tabelas finais",
                    },
                    {
                        "layer": "Complete analysis",
                        "entrypoint": "build_complete_ndvi_workspace",
                        "defined_in": "farmlab.complete_analysis",
                        "role": "consome a camada CRISP-DM e acrescenta estatistica final",
                        "produces": "dataset_overview, numeric_profiles, ndvi_stats_by_area, ndvi_outliers, pair_weekly_gaps, pair_classic_tests, ndvi_trend_tests, weekly_correlations",
                    },
                    {
                        "layer": "CRISP-DM",
                        "entrypoint": "build_ndvi_crispdm_workspace",
                        "defined_in": "farmlab.ndvi_crispdm",
                        "role": "gera auditoria, testes, modelagem e hipoteses",
                        "produces": "data_audit, pair_effect_tests, event_driver_lift, transition_model_*, final_hypothesis_register, decision_summary",
                    },
                    {
                        "layer": "Deep dive",
                        "entrypoint": "build_ndvi_deep_dive_workspace",
                        "defined_in": "farmlab.ndvi_deepdive",
                        "role": "monta timeline semanal, eventos, diagnosticos e outlook",
                        "produces": "ops_support_*, ndvi_phase_timeline, ndvi_events, ndvi_pair_diagnostics, ndvi_outlook",
                    },
                    {
                        "layer": "Phase 1",
                        "entrypoint": "build_phase1_workspace",
                        "defined_in": "farmlab.pairwise",
                        "role": "integra os dados brutos e cria a base comparavel por area e semana",
                        "produces": "area_inventory, ndvi_clean, ops_area_daily, miip_daily, pairwise_weekly_features, hypothesis_matrix",
                    },
                    {
                        "layer": "IO",
                        "entrypoint": "discover_dataset_paths + load_*",
                        "defined_in": "farmlab.io",
                        "role": "descobre e carrega os CSVs brutos do projeto",
                        "produces": "ndvi raw, clima, operacao, pragas, solo",
                    },
                ]
            )

            storytelling_steps = pd.DataFrame(
                [
                    {
                        "ordem": 1,
                        "etapa": "Leitura bruta",
                        "o_que_acontece": "o projeto localiza e le os CSVs do OneSoil, EKOS, Metos e Cropman",
                        "regra_chave": "discover_dataset_paths + load_*",
                        "saida_principal": "ndvi_raw, clima bruto, operacao bruta, pragas brutas, solo bruto",
                    },
                    {
                        "ordem": 2,
                        "etapa": "Filtro NDVI",
                        "o_que_acontece": "linhas com b1_valid_pixels <= 0 sao removidas da base analitica",
                        "regra_chave": "somente b1_valid_pixels > 0 entra em ndvi_clean",
                        "saida_principal": "ndvi_clean",
                    },
                    {
                        "ordem": 3,
                        "etapa": "Renomeacao e enriquecimento",
                        "o_que_acontece": "b1_mean vira ndvi_mean; b1_pct_solo vira soil_pct; data vira week_start",
                        "regra_chave": "renomeacao e criacao de ndvi_delta / ndvi_auc",
                        "saida_principal": "ndvi_clean enriquecido",
                    },
                    {
                        "ordem": 4,
                        "etapa": "Integracao semanal",
                        "o_que_acontece": "NDVI, clima, operacao e MIIP sao consolidados por area e semana",
                        "regra_chave": "granularidade semanal para comparacao entre pares",
                        "saida_principal": "pairwise_weekly_features",
                    },
                    {
                        "ordem": 5,
                        "etapa": "Deep dive",
                        "o_que_acontece": "o pipeline cria flags de queda, recuperacao, baixo vigor e drivers",
                        "regra_chave": "limiares em ndvi_delta_week e ndvi_norm_week",
                        "saida_principal": "ndvi_phase_timeline",
                    },
                    {
                        "ordem": 6,
                        "etapa": "Testes e hipoteses",
                        "o_que_acontece": "o projeto calcula gaps, testes pareados, drivers, hipoteses e decisoes",
                        "regra_chave": "H1-H4 sao decididas a partir das tabelas finais",
                        "saida_principal": "pair_effect_tests, pair_classic_tests, final_hypothesis_register, decision_summary",
                    },
                    {
                        "ordem": 7,
                        "etapa": "Export e verificacao",
                        "o_que_acontece": "os CSVs sao gravados e comparados por hash com uma regeneracao temporaria",
                        "regra_chave": "save_complete_ndvi_outputs + SHA-256",
                        "saida_principal": "notebook_outputs/complete_ndvi",
                    },
                ]
            )

            decision_register = pd.DataFrame(
                [
                    {
                        "decision": "usar apenas a banda b1 na analise principal",
                        "why": "somente a b1 foi promovida a ndvi_mean, ndvi_std, soil_pct e dense_veg_pct",
                        "impact": "b2 e b3 ficam apenas como metadado bruto e nao entram nos testes",
                    },
                    {
                        "decision": "descartar cenas com b1_valid_pixels <= 0",
                        "why": "sem pixel valido na banda principal nao existe base quantitativa confiavel para o NDVI",
                        "impact": "essas linhas nao entram em ndvi_clean, mas contam na auditoria de cobertura",
                    },
                    {
                        "decision": "agregar a analise em base semanal",
                        "why": "equilibra ruido de cena, cobertura irregular de satelite e integracao com clima/operacao/MIIP",
                        "impact": "os testes principais comparam 4.0 vs convencional por week_start",
                    },
                    {
                        "decision": "usar mapeamento manual oficial de season_id para area/treatment",
                        "why": "o dado bruto nao traz sozinho o significado do talhao em linguagem de negocio",
                        "impact": "o projeto depende do OFFICIAL_AREA_MAPPING para rotular grao/silagem e 4.0/convencional",
                    },
                    {
                        "decision": "usar JPG apenas como apoio visual",
                        "why": "o repositório local nao possui TIFF numerico processado no pipeline",
                        "impact": "a prova quantitativa vem do ndvi_metadata.csv e a galeria usa os JPGs correspondentes",
                    },
                    {
                        "decision": "tratar drivers como fatores associados, nao como causalidade fechada",
                        "why": "correlacao e sobre-representacao nao provam mecanismo causal sozinhas",
                        "impact": "H3 identifica explicadores observaveis sem vender causalidade indevida",
                    },
                ]
            )

            analysis_question_map = pd.DataFrame(
                [
                    {
                        "question": "Quais arquivos brutos entram na analise?",
                        "main_dataset_or_rule": "source_inventory + ndvi_raw",
                        "main_chart_or_table": "source_inventory + amostra das colunas brutas",
                        "final_csv_or_output": "dataset_overview.csv",
                        "decision_supported": "confianca sobre o ponto de partida do pipeline",
                    },
                    {
                        "question": "Quais linhas NDVI entram ou saem da analise?",
                        "main_dataset_or_rule": "valid_pixels_policy + dropped_ndvi_rows",
                        "main_chart_or_table": "valid_pixels_summary + valid_pixels_matrix",
                        "final_csv_or_output": "data_audit.csv",
                        "decision_supported": "cobertura e confiabilidade do NDVI",
                    },
                    {
                        "question": "Quais campos foram renomeados ou criados?",
                        "main_dataset_or_rule": "transform_catalog + derived_columns_catalog",
                        "main_chart_or_table": "catalogos de transformacao e colunas derivadas",
                        "final_csv_or_output": "ndvi_clean / pairwise_weekly_features / ndvi_phase_timeline",
                        "decision_supported": "rastreabilidade do preparo de dados",
                    },
                    {
                        "question": "Quem sustentou maior nivel temporal de NDVI?",
                        "main_dataset_or_rule": "pair_effect_tests + pair_classic_tests",
                        "main_chart_or_table": "NDVI medio semanal por area e Gap semanal 4.0 - convencional",
                        "final_csv_or_output": "pair_effect_tests.csv + pair_classic_tests.csv",
                        "decision_supported": "H1",
                    },
                    {
                        "question": "O 4.0 reduziu semanas problema?",
                        "main_dataset_or_rule": "pair_effect_tests",
                        "main_chart_or_table": "pair_effect_tests + leitura das flags na timeline",
                        "final_csv_or_output": "pair_effect_tests.csv",
                        "decision_supported": "H2",
                    },
                    {
                        "question": "Quais drivers aparecem nas semanas problema?",
                        "main_dataset_or_rule": "event_driver_lift + ndvi_phase_timeline",
                        "main_chart_or_table": "Drivers sobre-representados nas semanas problema do NDVI",
                        "final_csv_or_output": "event_driver_lift.csv",
                        "decision_supported": "H3",
                    },
                    {
                        "question": "Como o projeto decide o outlook final do par?",
                        "main_dataset_or_rule": "final_hypothesis_register + decision_summary",
                        "main_chart_or_table": "final_hypothesis_register + decision_summary",
                        "final_csv_or_output": "final_hypothesis_register.csv + decision_summary.csv",
                        "decision_supported": "H4 e mensagem final do projeto",
                    },
                ]
            )

            band_catalog = pd.DataFrame(
                [
                    {
                        "band": "b1",
                        "status_no_projeto": "usada",
                        "uso_analitico": "base do NDVI, solo exposto e vegetacao densa",
                        "colunas_centrais": "b1_valid_pixels, b1_mean, b1_std, b1_pct_solo, b1_pct_veg_densa",
                        "motivo": "e a unica banda promovida a metricas centrais do pipeline",
                    },
                    {
                        "band": "b2",
                        "status_no_projeto": "nao usada na analise principal",
                        "uso_analitico": "apenas metadado bruto",
                        "colunas_centrais": "b2_valid_pixels e estatisticas auxiliares",
                        "motivo": "nao existe papel semantico definido para b2 no modelo atual",
                    },
                    {
                        "band": "b3",
                        "status_no_projeto": "nao usada na analise principal",
                        "uso_analitico": "apenas metadado bruto",
                        "colunas_centrais": "b3_valid_pixels e estatisticas auxiliares",
                        "motivo": "nao existe papel semantico definido para b3 no modelo atual",
                    },
                ]
            )

            ndvi_raw_primary_cols = {
                "filename",
                "season_id",
                "date",
                "image_path",
                "b1_valid_pixels",
                "b1_mean",
                "b1_std",
                "b1_pct_solo",
                "b1_pct_veg_densa",
                "bounds_left",
                "bounds_bottom",
                "bounds_right",
                "bounds_top",
            }
            ndvi_raw_geometry_cols = {"bounds_left", "bounds_bottom", "bounds_right", "bounds_top", "res_x", "res_y", "width", "height", "crs"}
            ndvi_raw_identity_cols = {"filename", "season_id", "driver", "dtype", "nodata", "count", "date", "image_path"}
            ndvi_raw_rows = []
            for column in ndvi_raw.columns:
                if column in {"b1_valid_pixels", "b1_mean", "b1_std", "b1_pct_solo", "b1_pct_veg_densa"}:
                    status = "usada diretamente"
                    used_in = "filtro e metricas centrais do NDVI"
                    note = "entra no ndvi_clean e influencia testes, graficos e hipoteses"
                elif column in {"bounds_left", "bounds_bottom", "bounds_right", "bounds_top"}:
                    status = "usada diretamente"
                    used_in = "geometria e inventario espacial"
                    note = "forma o bounding box do talhao e bbox_area_ha"
                elif column in {"date", "image_path"}:
                    status = "criada na carga e usada"
                    used_in = "ordenacao temporal e galeria visual"
                    note = "nao vem pronta do CSV; e criada em load_ndvi_metadata"
                elif column.startswith("b2_") or column.startswith("b3_"):
                    status = "nao usada na analise principal"
                    used_in = "auditoria do bruto apenas"
                    note = "fica disponivel no metadado, mas nao vira metrica central nem criterio de filtro"
                elif column.startswith("b1_"):
                    status = "metadado bruto auxiliar"
                    used_in = "sem papel central na decisao"
                    note = "permanece no bruto/ndvi_clean para rastreabilidade, mas nao entra diretamente nos testes principais"
                elif column in ndvi_raw_identity_cols:
                    status = "metadado de identificacao"
                    used_in = "carga, ordenacao ou auditoria"
                    note = "ajuda a identificar a cena e o contexto do raster"
                elif column in ndvi_raw_geometry_cols:
                    status = "metadado espacial auxiliar"
                    used_in = "auditoria espacial"
                    note = "descreve resolucao, dimensao ou referencia espacial da cena"
                else:
                    status = "auxiliar"
                    used_in = "sem papel central no pipeline atual"
                    note = "preservada para auditoria do bruto"
                ndvi_raw_rows.append(
                    {
                        "column": column,
                        "status_no_projeto": status,
                        "used_in": used_in,
                        "note": note,
                    }
                )
            ndvi_raw_column_catalog = pd.DataFrame(ndvi_raw_rows)

            valid_pixels_policy = pd.DataFrame(
                [
                    {
                        "field": "b1_valid_pixels",
                        "status": "usado no filtro",
                        "where": "build_ndvi_clean",
                        "rule": "somente linhas com b1_valid_pixels > 0 entram em ndvi_clean",
                        "reason": "b1 e a banda usada como base do NDVI no projeto",
                    },
                    {
                        "field": "b2_valid_pixels",
                        "status": "nao usado no filtro principal",
                        "where": "ndvi_metadata.csv bruto",
                        "rule": "permanece apenas como metadado do export do OneSoil",
                        "reason": "b2 nao foi promovida a metrica analitica central no pipeline atual",
                    },
                    {
                        "field": "b3_valid_pixels",
                        "status": "nao usado no filtro principal",
                        "where": "ndvi_metadata.csv bruto",
                        "rule": "permanece apenas como metadado do export do OneSoil",
                        "reason": "b3 nao foi promovida a metrica analitica central no pipeline atual",
                    },
                ]
            )

            valid_pixels_summary = pd.DataFrame(
                [
                    {
                        "total_rows_raw": int(len(ndvi_raw)),
                        "rows_b1_valid": int(ndvi_raw["b1_valid_pixels"].fillna(0).gt(0).sum()),
                        "rows_b1_invalid": int(ndvi_raw["b1_valid_pixels"].fillna(0).le(0).sum()),
                        "rows_b2_valid": int(ndvi_raw["b2_valid_pixels"].fillna(0).gt(0).sum()) if "b2_valid_pixels" in ndvi_raw.columns else pd.NA,
                        "rows_b2_invalid": int(ndvi_raw["b2_valid_pixels"].fillna(0).le(0).sum()) if "b2_valid_pixels" in ndvi_raw.columns else pd.NA,
                        "rows_b3_valid": int(ndvi_raw["b3_valid_pixels"].fillna(0).gt(0).sum()) if "b3_valid_pixels" in ndvi_raw.columns else pd.NA,
                        "rows_b3_invalid": int(ndvi_raw["b3_valid_pixels"].fillna(0).le(0).sum()) if "b3_valid_pixels" in ndvi_raw.columns else pd.NA,
                        "rows_after_ndvi_clean_filter": int(len(workspace["ndvi_clean"])),
                    }
                ]
            )

            valid_pixels_matrix = (
                ndvi_raw.assign(
                    b1_valid=ndvi_raw["b1_valid_pixels"].fillna(0).gt(0),
                    b2_valid=ndvi_raw["b2_valid_pixels"].fillna(0).gt(0) if "b2_valid_pixels" in ndvi_raw.columns else False,
                    b3_valid=ndvi_raw["b3_valid_pixels"].fillna(0).gt(0) if "b3_valid_pixels" in ndvi_raw.columns else False,
                )
                .groupby(["b1_valid", "b2_valid", "b3_valid"], as_index=False)
                .size()
                .rename(columns={"size": "rows"})
                .sort_values(["b1_valid", "b2_valid", "b3_valid"])
                .reset_index(drop=True)
            )

            dropped_ndvi_rows = (
                ndvi_raw[ndvi_raw["b1_valid_pixels"].fillna(0).le(0)]
                .sort_values(["season_id", "date"])
                .reset_index(drop=True)
            )

            transform_catalog = pd.DataFrame(
                [
                    {
                        "stage": "NDVI clean",
                        "input_column": "b1_mean",
                        "output_column": "ndvi_mean",
                        "rule": "renomeacao direta",
                        "meaning": "media do NDVI por cena valida",
                    },
                    {
                        "stage": "NDVI clean",
                        "input_column": "b1_std",
                        "output_column": "ndvi_std",
                        "rule": "renomeacao direta",
                        "meaning": "dispersao do NDVI dentro da cena",
                    },
                    {
                        "stage": "NDVI clean",
                        "input_column": "b1_pct_solo",
                        "output_column": "soil_pct",
                        "rule": "renomeacao direta",
                        "meaning": "percentual de solo exposto por cena",
                    },
                    {
                        "stage": "NDVI clean",
                        "input_column": "b1_pct_veg_densa",
                        "output_column": "dense_veg_pct",
                        "rule": "renomeacao direta",
                        "meaning": "percentual de vegetacao densa por cena",
                    },
                    {
                        "stage": "NDVI clean",
                        "input_column": "date",
                        "output_column": "week_start",
                        "rule": "normalizacao para inicio da semana",
                        "meaning": "chave temporal de integracao semanal",
                    },
                    {
                        "stage": "NDVI clean",
                        "input_column": "ndvi_mean",
                        "output_column": "ndvi_delta",
                        "rule": "diff por season_id",
                        "meaning": "variacao entre cenas consecutivas",
                    },
                    {
                        "stage": "NDVI clean",
                        "input_column": "ndvi_mean + date",
                        "output_column": "ndvi_auc",
                        "rule": "acumulo por area sob a curva",
                        "meaning": "vigor acumulado ao longo do ciclo",
                    },
                    {
                        "stage": "Weekly features",
                        "input_column": "ndvi_mean",
                        "output_column": "ndvi_mean_week",
                        "rule": "media semanal",
                        "meaning": "metrica central de comparacao temporal",
                    },
                    {
                        "stage": "Weekly features",
                        "input_column": "soil_pct",
                        "output_column": "soil_pct_week",
                        "rule": "media semanal",
                        "meaning": "solo exposto medio da semana",
                    },
                    {
                        "stage": "Weekly features",
                        "input_column": "dense_veg_pct",
                        "output_column": "dense_veg_pct_week",
                        "rule": "media semanal",
                        "meaning": "vegetacao densa media da semana",
                    },
                    {
                        "stage": "Deep dive",
                        "input_column": "ndvi_mean_week",
                        "output_column": "ndvi_norm_week",
                        "rule": "normalizacao min-max por season_id",
                        "meaning": "posicao relativa da semana dentro do ciclo da propria area",
                    },
                    {
                        "stage": "Deep dive",
                        "input_column": "ndvi_delta_week",
                        "output_column": "major_drop_flag",
                        "rule": "limiar <= -0.08",
                        "meaning": "marca semana com queda relevante de vigor",
                    },
                    {
                        "stage": "Deep dive",
                        "input_column": "ndvi_norm_week",
                        "output_column": "low_vigor_flag",
                        "rule": "limiar <= 0.35",
                        "meaning": "marca semana de baixo vigor relativo",
                    },
                ]
            )

            derived_columns_catalog = pd.DataFrame(
                [
                    {"dataset": "ndvi_clean", "column": "week_start", "built_from": "date", "rule": "normalizacao para inicio da semana", "why_it_exists": "viabiliza a integracao semanal"},
                    {"dataset": "ndvi_clean", "column": "ndvi_mean", "built_from": "b1_mean", "rule": "renomeacao direta", "why_it_exists": "vigor medio da cena"},
                    {"dataset": "ndvi_clean", "column": "ndvi_std", "built_from": "b1_std", "rule": "renomeacao direta", "why_it_exists": "heterogeneidade interna da cena"},
                    {"dataset": "ndvi_clean", "column": "soil_pct", "built_from": "b1_pct_solo", "rule": "renomeacao direta", "why_it_exists": "solo exposto por cena"},
                    {"dataset": "ndvi_clean", "column": "dense_veg_pct", "built_from": "b1_pct_veg_densa", "rule": "renomeacao direta", "why_it_exists": "vegetacao densa por cena"},
                    {"dataset": "ndvi_clean", "column": "ndvi_delta", "built_from": "ndvi_mean", "rule": "diff por season_id", "why_it_exists": "mudanca entre cenas consecutivas"},
                    {"dataset": "ndvi_clean", "column": "ndvi_auc", "built_from": "ndvi_mean + date", "rule": "acumulo por area sob a curva", "why_it_exists": "vigor acumulado no ciclo"},
                    {"dataset": "ndvi_clean", "column": "has_weather_coverage", "built_from": "date + weather_daily", "rule": "marcacao booleana", "why_it_exists": "audita se a cena tem clima local associado"},
                    {"dataset": "pairwise_weekly_features", "column": "ndvi_mean_week", "built_from": "ndvi_mean", "rule": "media semanal", "why_it_exists": "metrica principal de comparacao temporal"},
                    {"dataset": "pairwise_weekly_features", "column": "ndvi_peak_week", "built_from": "ndvi_mean", "rule": "maximo semanal", "why_it_exists": "captura o melhor momento da semana"},
                    {"dataset": "pairwise_weekly_features", "column": "ndvi_std_week", "built_from": "ndvi_std", "rule": "media semanal", "why_it_exists": "resume heterogeneidade semanal"},
                    {"dataset": "pairwise_weekly_features", "column": "ndvi_auc_week", "built_from": "ndvi_auc", "rule": "maximo acumulado da semana", "why_it_exists": "resume vigor acumulado ate a semana"},
                    {"dataset": "pairwise_weekly_features", "column": "soil_pct_week", "built_from": "soil_pct", "rule": "media semanal", "why_it_exists": "solo exposto semanal"},
                    {"dataset": "pairwise_weekly_features", "column": "dense_veg_pct_week", "built_from": "dense_veg_pct", "rule": "media semanal", "why_it_exists": "vegetacao densa semanal"},
                    {"dataset": "pairwise_weekly_features", "column": "ndvi_delta_week", "built_from": "ndvi_mean_week", "rule": "diff semanal por area", "why_it_exists": "identifica queda e recuperacao"},
                    {"dataset": "pairwise_weekly_features", "column": "pair_ndvi_gap_4_0_minus_conv", "built_from": "ndvi_mean_week por par", "rule": "gap semanal", "why_it_exists": "mede vantagem semanal do 4.0"},
                    {"dataset": "ndvi_phase_timeline", "column": "ndvi_norm_week", "built_from": "ndvi_mean_week", "rule": "normalizacao min-max por season_id", "why_it_exists": "mede vigor relativo ao proprio ciclo"},
                    {"dataset": "ndvi_phase_timeline", "column": "major_drop_flag", "built_from": "ndvi_delta_week", "rule": "limiar <= -0.08", "why_it_exists": "marca queda relevante"},
                    {"dataset": "ndvi_phase_timeline", "column": "severe_drop_flag", "built_from": "ndvi_delta_week", "rule": "limiar <= -0.15", "why_it_exists": "marca queda muito forte"},
                    {"dataset": "ndvi_phase_timeline", "column": "recovery_flag", "built_from": "ndvi_delta_week", "rule": "limiar >= 0.08", "why_it_exists": "marca recuperacao relevante"},
                    {"dataset": "ndvi_phase_timeline", "column": "low_vigor_flag", "built_from": "ndvi_norm_week", "rule": "limiar <= 0.35", "why_it_exists": "marca baixo vigor relativo"},
                    {"dataset": "ndvi_phase_timeline", "column": "ops_risk_flag", "built_from": "flags operacionais", "rule": "combinacao booleana", "why_it_exists": "resume risco operacional semanal"},
                    {"dataset": "ndvi_phase_timeline", "column": "risk_flag_count", "built_from": "flags de risco", "rule": "soma de flags", "why_it_exists": "conta quantos riscos coexistem na semana"},
                    {"dataset": "ndvi_phase_timeline", "column": "primary_driver", "built_from": "flags de risco", "rule": "priorizacao de driver", "why_it_exists": "aponta explicador dominante da semana problema"},
                    {"dataset": "ndvi_phase_timeline", "column": "drivers_summary", "built_from": "primary_driver + secondary_driver", "rule": "texto resumido", "why_it_exists": "apoia leitura humana dos eventos"},
                    {"dataset": "ndvi_phase_timeline", "column": "story_sentence", "built_from": "fase + evento + drivers", "rule": "geracao de frase", "why_it_exists": "apoia apresentacao e interpretacao"},
                    {"dataset": "transition_model_frame", "column": "target_next_ndvi_delta", "built_from": "ndvi_delta_week deslocado", "rule": "lead de uma semana", "why_it_exists": "alvo do modelo interpretavel"},
                ]
            )

            metric_catalog = pd.DataFrame(
                [
                    {"metric": "ndvi_mean", "level": "cena", "meaning": "vigor medio da cena", "used_in": "ndvi_stats_by_area, outliers, series temporais"},
                    {"metric": "ndvi_delta", "level": "cena", "meaning": "mudanca do NDVI entre cenas consecutivas", "used_in": "diagnostico temporal"},
                    {"metric": "ndvi_auc", "level": "cena acumulada", "meaning": "vigor acumulado do ciclo", "used_in": "series acumuladas"},
                    {"metric": "ndvi_mean_week", "level": "semanal", "meaning": "vigor medio semanal", "used_in": "H1, pair_effect_tests, pair_classic_tests"},
                    {"metric": "ndvi_auc_week", "level": "semanal", "meaning": "area sob a curva acumulada ate a semana", "used_in": "comparacao de ciclo"},
                    {"metric": "soil_pct_week", "level": "semanal", "meaning": "solo exposto medio semanal", "used_in": "drivers, correlacoes"},
                    {"metric": "dense_veg_pct_week", "level": "semanal", "meaning": "vegetacao densa media semanal", "used_in": "contexto de vigor"},
                    {"metric": "ndvi_norm_week", "level": "semanal", "meaning": "vigor relativo ao proprio ciclo", "used_in": "flags de baixo vigor"},
                    {"metric": "risk_flag_count", "level": "semanal", "meaning": "quantidade de riscos observados na semana", "used_in": "correlacoes e modelagem"},
                    {"metric": "target_next_ndvi_delta", "level": "semanal", "meaning": "delta do NDVI da proxima semana", "used_in": "modelo interpretavel"},
                ]
            )

            stat_term_catalog = pd.DataFrame(
                [
                    {"term": "p-value", "where_it_appears": "pair_effect_tests / pair_classic_tests", "meaning": "mede quao compativel o resultado e com a hipotese de ausencia de diferenca", "how_to_read": "quanto menor, maior a evidencia contra acaso puro"},
                    {"term": "IC95%", "where_it_appears": "pair_effect_tests", "meaning": "intervalo de confianca do efeito", "how_to_read": "se todo o intervalo ficar acima de 0 favorece 4.0; abaixo de 0 favorece convencional"},
                    {"term": "vantagem_4_0", "where_it_appears": "pair_effect_tests", "meaning": "diferenca 4.0 - convencional para a metrica analisada", "how_to_read": "positivo favorece 4.0; negativo favorece convencional"},
                    {"term": "delta", "where_it_appears": "ndvi_delta / ndvi_delta_week", "meaning": "variacao entre um momento e o anterior", "how_to_read": "positivo indica subida; negativo indica queda"},
                    {"term": "delta_pp", "where_it_appears": "event_driver_lift", "meaning": "diferenca em pontos percentuais de frequencia de um driver entre semanas problema e baseline", "how_to_read": "quanto maior, mais sobre-representado o driver esta nas semanas ruins"},
                    {"term": "lift", "where_it_appears": "event_driver_lift", "meaning": "razao entre frequencia no evento e frequencia no baseline", "how_to_read": "1 = neutro; acima de 1 = driver aparece mais nas semanas problema"},
                    {"term": "score", "where_it_appears": "ndvi_outlook / final_hypothesis_register", "meaning": "indice composto de outlook pre-colheita", "how_to_read": "quanto maior, mais favoravel a leitura final da trajetoria"},
                    {"term": "evidence_level", "where_it_appears": "event_driver_lift / final_hypothesis_register", "meaning": "classificacao interpretativa da robustez do resultado", "how_to_read": "alta > media > baixa"},
                    {"term": "paired_effect_size_dz", "where_it_appears": "pair_classic_tests", "meaning": "tamanho de efeito padronizado em desenho pareado", "how_to_read": "maior magnitude absoluta indica diferenca mais forte"},
                    {"term": "target_next_ndvi_delta", "where_it_appears": "transition_model_frame", "meaning": "variacao do NDVI na semana seguinte", "how_to_read": "alvo usado para modelagem interpretavel"},
                ]
            )

            output_lineage = pd.DataFrame(
                [
                    {"csv": "data_audit.csv", "generated_by": "build_data_audit", "depends_on": "area_inventory, ndvi_clean, weather_daily, miip_daily, ops_area_daily, ndvi_phase_timeline", "decision_role": "confianca e cobertura da base"},
                    {"csv": "dataset_overview.csv", "generated_by": "build_dataset_overview", "depends_on": "workspace completo", "decision_role": "inventario tecnico das bases"},
                    {"csv": "decision_summary.csv", "generated_by": "build_decision_summary", "depends_on": "pair_effect_tests, event_driver_lift, final_hypothesis_register, ndvi_outlook, ndvi_pair_diagnostics", "decision_role": "mensagem final por par"},
                    {"csv": "event_driver_lift.csv", "generated_by": "build_event_driver_lift", "depends_on": "ndvi_phase_timeline", "decision_role": "explica drivers das semanas problema"},
                    {"csv": "final_hypothesis_register.csv", "generated_by": "build_final_hypothesis_register", "depends_on": "pair_effect_tests, event_driver_lift, ndvi_outlook, ndvi_pair_diagnostics, deep_dive_gaps", "decision_role": "fecha H1-H4"},
                    {"csv": "ndvi_outliers.csv", "generated_by": "build_ndvi_outliers", "depends_on": "ndvi_clean", "decision_role": "identifica cenas fora do padrao"},
                    {"csv": "ndvi_stats_by_area.csv", "generated_by": "build_ndvi_stats_by_area", "depends_on": "ndvi_clean", "decision_role": "resumo descritivo por talhao"},
                    {"csv": "ndvi_trend_tests.csv", "generated_by": "build_ndvi_trend_tests", "depends_on": "ndvi_clean", "decision_role": "tendencia linear de NDVI"},
                    {"csv": "numeric_profiles.csv", "generated_by": "build_numeric_profiles", "depends_on": "workspace completo", "decision_role": "perfil numerico das variaveis"},
                    {"csv": "pair_classic_tests.csv", "generated_by": "build_pair_classic_tests", "depends_on": "pair_weekly_gaps", "decision_role": "teste classico por metrica"},
                    {"csv": "pair_effect_tests.csv", "generated_by": "build_pair_effect_tests", "depends_on": "ndvi_phase_timeline", "decision_role": "teste pareado principal do projeto"},
                    {"csv": "pair_weekly_gaps.csv", "generated_by": "build_pair_weekly_gaps", "depends_on": "ndvi_phase_timeline", "decision_role": "mostra gap semanal 4.0 - convencional"},
                    {"csv": "transition_model_frame.csv", "generated_by": "build_transition_model_frame", "depends_on": "ndvi_phase_timeline", "decision_role": "base final de modelagem"},
                    {"csv": "transition_model_summary.csv", "generated_by": "fit_transition_model", "depends_on": "transition_model_frame", "decision_role": "qualidade da modelagem"},
                    {"csv": "transition_model_coefficients.csv", "generated_by": "fit_transition_model", "depends_on": "transition_model_frame", "decision_role": "interpretacao do modelo"},
                    {"csv": "transition_model_predictions.csv", "generated_by": "fit_transition_model", "depends_on": "transition_model_frame", "decision_role": "real x previsto"},
                    {"csv": "weekly_correlations.csv", "generated_by": "build_weekly_correlations", "depends_on": "transition_model_frame", "decision_role": "priorizacao de variaveis associadas"},
                ]
            )

            csv_reading_guide = pd.DataFrame(
                [
                    {"csv": "dataset_overview.csv", "grain": "dataset", "open_when": "quiser saber o inventario tecnico das bases", "read_first_columns": "dataset, rows, columns, start_date, end_date", "why_it_matters": "prova o que existe e a cobertura temporal"},
                    {"csv": "ndvi_stats_by_area.csv", "grain": "area", "open_when": "quiser ver medias, picos e solo/vegetacao por talhao", "read_first_columns": "area_label, mean, max, soil_pct_mean, dense_veg_pct_mean", "why_it_matters": "resume o comportamento descritivo do NDVI"},
                    {"csv": "ndvi_outliers.csv", "grain": "cena", "open_when": "quiser investigar imagens fora do padrao", "read_first_columns": "date, area_label, ndvi_mean, ndvi_zscore, outlier_flag", "why_it_matters": "explica cenas anormais que podem distorcer a leitura"},
                    {"csv": "pair_weekly_gaps.csv", "grain": "semana por par", "open_when": "quiser ver quando o 4.0 esteve acima ou abaixo", "read_first_columns": "week_start, comparison_pair, gap_ndvi_mean_week_4_0_minus_convencional", "why_it_matters": "e a serie mais direta da vantagem semanal do 4.0"},
                    {"csv": "pair_effect_tests.csv", "grain": "metrica por par", "open_when": "quiser decidir H1 e H2", "read_first_columns": "comparison_pair, metric_label, vantagem_4_0, p_value, ci_low, ci_high", "why_it_matters": "e a principal tabela de teste pareado do projeto"},
                    {"csv": "pair_classic_tests.csv", "grain": "metrica por par", "open_when": "quiser validacao estatistica classica adicional", "read_first_columns": "comparison_pair, metric_label, recommended_test, recommended_p_value, paired_effect_size_dz", "why_it_matters": "reforca ou qualifica a leitura dos gaps semanais"},
                    {"csv": "event_driver_lift.csv", "grain": "driver por par", "open_when": "quiser explicar semanas problema", "read_first_columns": "comparison_pair, driver, delta_pp, lift, evidence_level", "why_it_matters": "e a principal base de H3"},
                    {"csv": "final_hypothesis_register.csv", "grain": "hipotese por par", "open_when": "quiser ver o fechamento executivo H1-H4", "read_first_columns": "comparison_pair, hypothesis_id, status, proof_basis, known_limits", "why_it_matters": "fecha o status formal das hipoteses"},
                    {"csv": "decision_summary.csv", "grain": "par", "open_when": "quiser a mensagem final do projeto", "read_first_columns": "comparison_pair, temporal_winner, decision_message", "why_it_matters": "condensa a leitura tecnica para apresentacao"},
                    {"csv": "weekly_correlations.csv", "grain": "feature x target", "open_when": "quiser priorizar variaveis associadas ao NDVI", "read_first_columns": "analysis_target, feature, strongest_abs_correlation, direction", "why_it_matters": "apoia interpretacao, nao causalidade"},
                    {"csv": "transition_model_summary.csv", "grain": "modelo", "open_when": "quiser saber se o modelo e forte ou fraco", "read_first_columns": "mae_in_sample, mae_loo, r2_loo", "why_it_matters": "evita vender a modelagem como prova preditiva robusta"},
                    {"csv": "data_audit.csv", "grain": "area", "open_when": "quiser auditar qualidade e cobertura dos dados", "read_first_columns": "area_label, ndvi_valid_ratio, weather_coverage_ratio, miip_coverage_ratio, audit_status", "why_it_matters": "mostra o quanto confiar em cada area"},
                ]
            )

            csv_family_map = pd.DataFrame(
                [
                    {
                        "family": "inventario e auditoria",
                        "csvs": "dataset_overview.csv, data_audit.csv, numeric_profiles.csv",
                        "why_it_exists": "prova o que entrou no pipeline e quao confiavel e a cobertura dos dados",
                    },
                    {
                        "family": "descritivo do NDVI",
                        "csvs": "ndvi_stats_by_area.csv, ndvi_trend_tests.csv, ndvi_outliers.csv",
                        "why_it_exists": "resume o comportamento do NDVI, sua tendencia e cenas fora do padrao",
                    },
                    {
                        "family": "comparacao entre pares",
                        "csvs": "pair_weekly_gaps.csv, pair_effect_tests.csv, pair_classic_tests.csv",
                        "why_it_exists": "mede a diferenca 4.0 vs convencional e testa se ela e robusta",
                    },
                    {
                        "family": "explicacao de drivers",
                        "csvs": "weekly_correlations.csv, event_driver_lift.csv",
                        "why_it_exists": "prioriza fatores associados e drivers das semanas problema",
                    },
                    {
                        "family": "modelagem interpretavel",
                        "csvs": "transition_model_frame.csv, transition_model_summary.csv, transition_model_coefficients.csv, transition_model_predictions.csv",
                        "why_it_exists": "mostra como o projeto modela a transicao do NDVI semanal e qual a qualidade dessa modelagem",
                    },
                    {
                        "family": "fechamento executivo",
                        "csvs": "final_hypothesis_register.csv, decision_summary.csv",
                        "why_it_exists": "condensa o que o pipeline concluiu em H1-H4 e na mensagem final por par",
                    },
                ]
            )

            pair_scope = pd.DataFrame(
                [
                    {
                        "comparison_pair": "grao",
                        "area_4_0": "Grao 4.0",
                        "area_convencional": "Grao Convencional",
                        "o_que_esta_sendo_comparado": "tecnologia_4_0 vs convencional no par de grao",
                    },
                    {
                        "comparison_pair": "silagem",
                        "area_4_0": "Silagem 4.0",
                        "area_convencional": "Silagem Convencional",
                        "o_que_esta_sendo_comparado": "tecnologia_4_0 vs convencional no par de silagem",
                    },
                ]
            )

            hypothesis_catalog = pd.DataFrame(
                [
                    {"hypothesis_id": "H1", "question": "4.0 sustenta maior nivel temporal de NDVI?", "main_evidence": "pair_effect_tests + pair_classic_tests", "primary_metric": "ndvi_mean_week"},
                    {"hypothesis_id": "H2", "question": "4.0 reduz semanas problema?", "main_evidence": "pair_effect_tests", "primary_metric": "low_vigor_flag + major_drop_flag"},
                    {"hypothesis_id": "H3", "question": "As semanas problema tem drivers identificaveis?", "main_evidence": "event_driver_lift", "primary_metric": "delta_pp e lift por driver"},
                    {"hypothesis_id": "H4", "question": "O outlook pre-colheita favorece 4.0?", "main_evidence": "ndvi_outlook + ndvi_pair_diagnostics", "primary_metric": "outlook_score e trajectory_winner"},
                ]
            )

            trust_notes = pd.DataFrame(
                [
                    {"topic": "Reprodutibilidade", "status": "alta", "note": "os CSVs finais podem ser regenerados pelo pipeline a partir da pasta data"},
                    {"topic": "NDVI", "status": "alta", "note": "a analise quantitativa usa ndvi_metadata.csv; os JPGs sao apoio visual"},
                    {"topic": "Mapeamento dos talhoes", "status": "media", "note": "season_id -> area/treatment vem de mapeamento manual oficial no codigo"},
                    {"topic": "Atribuicao espacial de operacao e MIIP", "status": "media", "note": "usa bbox/centroide, nao limite vetorial oficial do talhao"},
                    {"topic": "Causalidade", "status": "baixa", "note": "correlacoes e drivers explicam associacao, nao prova causal fechada"},
                    {"topic": "Economia final", "status": "baixa", "note": "o projeto atual nao fecha ROI ou kg/R$ de forma conclusiva"},
                ]
            )

            dataset_contracts = pd.DataFrame(
                [
                    {
                        "dataset": "ndvi_raw",
                        "grain": "cena bruta",
                        "keys": "season_id + filename + date",
                        "role": "metadado bruto do OneSoil",
                        "main_columns": "b1_valid_pixels, b1_mean, b1_std, b1_pct_solo, b1_pct_veg_densa",
                    },
                    {
                        "dataset": "ndvi_clean",
                        "grain": "cena valida",
                        "keys": "season_id + date",
                        "role": "base principal do NDVI apos filtro e renomeacoes",
                        "main_columns": "ndvi_mean, ndvi_std, soil_pct, dense_veg_pct, ndvi_delta, ndvi_auc",
                    },
                    {
                        "dataset": "ops_area_daily",
                        "grain": "dia por area",
                        "keys": "season_id + date",
                        "role": "resumo operacional diario vinculado ao talhao",
                        "main_columns": "harvest_yield_mean_kg_ha, fert_dose_gap_abs_mean_kg_ha, overlap_area_pct_bbox, stop_duration_h_per_bbox_ha",
                    },
                    {
                        "dataset": "miip_daily",
                        "grain": "dia por area",
                        "keys": "season_id + date",
                        "role": "resumo de armadilhas/pragas por area",
                        "main_columns": "avg_pest_count, total_pest_count, alert_hits, control_hits, damage_hits",
                    },
                    {
                        "dataset": "pairwise_weekly_features",
                        "grain": "semana por area",
                        "keys": "season_id + week_start",
                        "role": "feature store semanal integrado",
                        "main_columns": "ndvi_mean_week, soil_pct_week, dense_veg_pct_week, clima_week, operacao_week, miip_week",
                    },
                    {
                        "dataset": "ndvi_phase_timeline",
                        "grain": "semana por area",
                        "keys": "season_id + week_start",
                        "role": "tabela central para flags, eventos, drivers e testes",
                        "main_columns": "ndvi_mean_week, ndvi_delta_week, ndvi_norm_week, low_vigor_flag, major_drop_flag, primary_driver",
                    },
                    {
                        "dataset": "transition_model_frame",
                        "grain": "semana por area",
                        "keys": "season_id + week_start",
                        "role": "base final de modelagem interpretavel",
                        "main_columns": "target_next_ndvi_delta, risk_flag_count, operacao_week, clima_week, miip_week",
                    },
                ]
            )

            auxiliary_source_usage_catalog = pd.DataFrame(
                [
                    {
                        "source_dataset": "weather_hourly -> weather_daily/weather_weekly",
                        "main_fields_used": "precipitation_mm, evapotranspiration_mm, water_balance_mm, solar_radiation_w_m2, temp_avg_c, temp_max_c, temp_min_c, humidity_avg_pct, wind_avg_kmh",
                        "why_it_matters": "explica estresse ou alivio climatico associado ao NDVI",
                    },
                    {
                        "source_dataset": "layer_map_planting -> ops_area_daily",
                        "main_fields_used": "Population - ha, Area - ha, Date Time, geometry",
                        "why_it_matters": "resume estabelecimento inicial e densidade de plantio",
                    },
                    {
                        "source_dataset": "layer_map_grain_harvesting -> ops_area_daily",
                        "main_fields_used": "Yield - kg/ha, Weight - kg, Humidity - %, Area - ha, Date Time, geometry",
                        "why_it_matters": "entrega produtividade e contexto operacional de colheita",
                    },
                    {
                        "source_dataset": "camadas EKOS adicionais -> ops_area_daily/ndvi_phase_timeline",
                        "main_fields_used": "fertilizacao, sobreposicao, velocidade, estado, parada, alarmes, telemetria, motor, combustivel",
                        "why_it_matters": "alimenta flags de risco operacional e drivers",
                    },
                    {
                        "source_dataset": "traps_data / traps_events / pest_list / pest_details -> miip_daily",
                        "main_fields_used": "pestCount, alert, control, damage, image events, ping events, battery events, actionRay",
                        "why_it_matters": "resume pressao de pragas e sinais do MIIP por area e semana",
                    },
                    {
                        "source_dataset": "soil_analysis -> soil_context",
                        "main_fields_used": "atributos quimicos do solo por amostra",
                        "why_it_matters": "entra como contexto interpretativo, nao como chave espacial fina do talhao",
                    },
                ]
            )

            function_map = pd.DataFrame(
                [
                    {"function": "build_ndvi_clean", "module": "farmlab.pairwise", "purpose": "filtra cenas NDVI invalidas, renomeia colunas e cria metricas base"},
                    {"function": "build_ndvi_weekly", "module": "farmlab.pairwise", "purpose": "agrega o NDVI por semana"},
                    {"function": "build_pairwise_weekly_features", "module": "farmlab.pairwise", "purpose": "integra NDVI, clima, operacao e pragas por area e semana"},
                    {"function": "build_area_inventory", "module": "farmlab.pairwise", "purpose": "cria o inventario final de cobertura e estatisticas por area"},
                    {"function": "build_ndvi_phase_timeline", "module": "farmlab.ndvi_deepdive", "purpose": "cria flags, eventos e drivers por semana"},
                    {"function": "build_ndvi_events", "module": "farmlab.ndvi_deepdive", "purpose": "separa semanas relevantes para leitura visual"},
                    {"function": "build_pair_effect_tests", "module": "farmlab.ndvi_crispdm", "purpose": "executa os testes pareados principais do projeto"},
                    {"function": "build_event_driver_lift", "module": "farmlab.ndvi_crispdm", "purpose": "mede drivers sobre-representados nas semanas problema"},
                    {"function": "build_final_hypothesis_register", "module": "farmlab.ndvi_crispdm", "purpose": "fecha as hipoteses H1-H4"},
                    {"function": "build_decision_summary", "module": "farmlab.ndvi_crispdm", "purpose": "gera a mensagem de decisao operacional por par"},
                    {"function": "build_pair_classic_tests", "module": "farmlab.complete_analysis", "purpose": "aplica testes estatisticos classicos sobre os gaps semanais"},
                    {"function": "build_ndvi_outliers", "module": "farmlab.complete_analysis", "purpose": "marca outliers por z-score e robust z-score"},
                ]
            )

            module_map = pd.DataFrame(
                [
                    {"module": "farmlab.io", "responsibility": "descobrir caminhos e carregar os CSVs brutos", "when_to_open": "quando quiser ver a leitura do dado de origem"},
                    {"module": "farmlab.pairwise", "responsibility": "filtro inicial, renomeacoes, uniao por area e agregacao semanal", "when_to_open": "quando quiser entender o tratamento primario dos dados"},
                    {"module": "farmlab.ndvi_deepdive", "responsibility": "flags, eventos, drivers e outlook", "when_to_open": "quando quiser entender a narrativa temporal e os drivers"},
                    {"module": "farmlab.ndvi_crispdm", "responsibility": "auditoria, testes, hipoteses e decisao", "when_to_open": "quando quiser entender H1-H4 e a decisao final"},
                    {"module": "farmlab.complete_analysis", "responsibility": "estatistica final, correlacoes, testes classicos e export", "when_to_open": "quando quiser entender os CSVs finais"},
                ]
            )

            print("Workspace carregado.")
            """
        ),
        code_cell(
            """
            pd.set_option("display.max_colwidth", None)
            pd.set_option("display.max_columns", None)
            pd.set_option("display.width", None)


            def code_block(text: str, language: str = "python") -> Markdown:
                return Markdown(f"```{language}\\n{text}\\n```")


            def show_function_source(function_object) -> None:
                display(code_block(inspect.getsource(function_object)))


            def show_df(
                frame: pd.DataFrame,
                *,
                title: str | None = None,
                rows: int = 20,
                max_width_px: int = 340,
            ) -> None:
                if title:
                    display(Markdown(f"#### {title}"))
                display(Markdown(f"`{len(frame)}` linhas x `{len(frame.columns)}` colunas"))
                if frame.empty:
                    display(frame)
                    return
                subset = frame.head(rows).copy()
                with pd.option_context("display.max_colwidth", None, "display.max_columns", None, "display.width", None):
                    display(subset)
                if len(frame) > rows:
                    hidden = len(frame) - rows
                    display(
                        Markdown(
                            f"Mostrando as primeiras `{rows}` linhas. "
                            f"Ha mais `{hidden}` linhas ocultas. "
                            "Use `show_all(nome_df)` para tudo ou `show_row(nome_df, indice)` para uma linha vertical."
                        )
                    )


            def show_all(frame: pd.DataFrame, *, title: str | None = None, max_width_px: int = 340) -> None:
                show_df(frame, title=title, rows=max(len(frame), 1), max_width_px=max_width_px)


            def show_row(frame: pd.DataFrame, row_index, *, title: str | None = None, max_width_px: int = 820) -> None:
                if title:
                    display(Markdown(f"#### {title}"))
                if row_index in frame.index:
                    row = frame.loc[row_index]
                else:
                    row = frame.iloc[row_index]
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[0]
                vertical = (
                    row.rename("value")
                    .to_frame()
                    .reset_index()
                    .rename(columns={"index": "column"})
                )
                with pd.option_context("display.max_colwidth", None, "display.max_columns", None, "display.width", None):
                    display(vertical)


            def show_csv(path: str | Path, *, rows: int = 20, max_width_px: int = 340) -> pd.DataFrame:
                frame = pd.read_csv(path)
                show_df(frame, title=str(path), rows=rows, max_width_px=max_width_px)
                return frame


            def sample_rows_per_area(frame: pd.DataFrame, max_images_per_area: int = 2) -> pd.DataFrame:
                if frame.empty:
                    return frame.copy()
                groups = []
                for _, group in frame.sort_values("date").groupby("season_id", sort=False):
                    if len(group) <= max_images_per_area:
                        groups.append(group)
                        continue
                    positions = sorted({0, len(group) // 2, len(group) - 1})
                    groups.append(group.iloc[positions[:max_images_per_area]])
                return pd.concat(groups, ignore_index=True)


            def _image_to_base64(path: str) -> str | None:
                file_path = Path(path)
                if not file_path.exists():
                    return None
                return base64.b64encode(file_path.read_bytes()).decode("ascii")


            def gallery_html(frame: pd.DataFrame, *, title: str, subtitle_columns: list[str]) -> HTML:
                if frame.empty:
                    return HTML(f"<h2>{title}</h2><p>Sem imagens disponiveis.</p>")

                cards = [f"<h2>{title}</h2>"]
                for row in frame.itertuples(index=False):
                    image_b64 = _image_to_base64(str(row.image_path)) if pd.notna(row.image_path) else None
                    subtitle = " | ".join(
                        f"{column}={getattr(row, column)}" for column in subtitle_columns if hasattr(row, column)
                    )
                    cards.append(f"<h3>{row.area_label} | {pd.Timestamp(row.date).date()}</h3>")
                    cards.append(f"<p>{subtitle}</p>")
                    if image_b64 is None:
                        cards.append("<p>Imagem nao encontrada.</p>")
                        continue
                    cards.append(
                        f'<img src="data:image/jpeg;base64,{image_b64}" '
                        'style="max-width: 560px; border-radius: 8px; margin-bottom: 16px;" />'
                    )
                return HTML("\\n".join(cards))


            def event_gallery_html(events: pd.DataFrame, ndvi_clean: pd.DataFrame, max_events: int = 10) -> HTML:
                if events.empty or ndvi_clean.empty:
                    return HTML("<h2>Galeria de eventos</h2><p>Sem eventos ou imagens disponiveis.</p>")

                priority = {"queda_forte": 0, "queda": 1, "baixo_vigor": 2, "recuperacao": 3, "pico": 4}
                selected = events.copy()
                selected["priority"] = selected["event_type"].map(priority).fillna(9)
                selected = selected.sort_values(["priority", "week_start", "area_label"]).head(max_events)

                cards = ["<h2>Eventos NDVI mais relevantes</h2>"]
                for event in selected.itertuples(index=False):
                    candidates = ndvi_clean[ndvi_clean["season_id"] == event.season_id].copy()
                    if candidates.empty:
                        continue
                    candidates["date"] = pd.to_datetime(candidates["date"], errors="coerce")
                    event_date = pd.to_datetime(event.week_start, errors="coerce")
                    match = candidates.loc[(candidates["date"] - event_date).abs().idxmin()]
                    image_b64 = _image_to_base64(str(match["image_path"])) if pd.notna(match["image_path"]) else None
                    cards.append(f"<h3>{event.area_label} | {pd.Timestamp(event.week_start).date()} | {event.event_type}</h3>")
                    cards.append(f"<p><strong>Drivers:</strong> {event.drivers_summary}</p>")
                    cards.append(f"<p>{event.story_sentence}</p>")
                    if image_b64 is not None:
                        cards.append(
                            f'<img src="data:image/jpeg;base64,{image_b64}" '
                            'style="max-width: 580px; border-radius: 8px; margin-bottom: 18px;" />'
                        )
                return HTML("\\n".join(cards))


            def verify_current_outputs_against_workspace(workspace: dict, output_dir: Path) -> pd.DataFrame:
                with tempfile.TemporaryDirectory() as tmpdir:
                    tmp_path = Path(tmpdir)
                    save_complete_ndvi_outputs(workspace, tmp_path)
                    current_files = {path.name: path for path in output_dir.glob("*.csv")}
                    fresh_files = {path.name: path for path in tmp_path.glob("*.csv")}
                    rows = []
                    for name in sorted(set(current_files) | set(fresh_files)):
                        current_path = current_files.get(name)
                        fresh_path = fresh_files.get(name)
                        current_hash = hashlib.sha256(current_path.read_bytes()).hexdigest() if current_path else None
                        fresh_hash = hashlib.sha256(fresh_path.read_bytes()).hexdigest() if fresh_path else None
                        rows.append(
                            {
                                "csv": name,
                                "status": "MATCH" if current_hash == fresh_hash else "DIFF",
                                "current_exists": current_path is not None,
                                "fresh_exists": fresh_path is not None,
                                "current_sha256": current_hash,
                                "fresh_sha256": fresh_hash,
                            }
                        )
                    return pd.DataFrame(rows)
            """
        ),
        markdown_cell(
            """
            ## 1. Business Understanding

            O objetivo desta analise e explicar, com base estatistica e agronomica, por que em alguns momentos a area convencional supera a area com tecnologias 4.0 e em outros momentos ocorre o inverso.

            O material da aula de ciclo de vida do dado foi usado como referencia metodologica. O notebook segue a estrutura CRISP-DM com foco em problema, dados, preparo, modelagem e avaliacao.
            """
        ),
        code_cell(
            """
            info_path = PROJECT_DIR / "info.md"
            if info_path.exists():
                display(Markdown(info_path.read_text(encoding="utf-8")))
            else:
                print("info.md nao encontrado em", info_path)
            """
        ),
        markdown_cell(
            """
            ## 1.1 Leitura rapida do fluxo real

            Antes de entrar no detalhe tecnico, a leitura correta do projeto e esta:

            1. carregar o NDVI bruto e as bases auxiliares;
            2. validar o que realmente existe no dado;
            3. descartar cenas NDVI sem pixel valido na banda principal;
            4. transformar a serie por cena em serie semanal;
            5. cruzar NDVI com clima, operacao, pragas e solo;
            6. testar hipoteses e fechar a decisao final.

            Se voce entender bem esta secao, o resto do notebook vira aprofundamento e auditoria.
            """
        ),
        markdown_cell(
            """
            ### 1.1.1 Pergunta de negocio

            A pergunta principal do projeto nao e so "quem ganhou". A pergunta e:

            - por que em alguns pares o 4.0 ficou abaixo do convencional?
            - em quais semanas isso aconteceu?
            - quais fatores observaveis apareceram junto com essas semanas?
            - existe evidência estatistica suficiente para sustentar essa leitura?
            """
        ),
        code_cell(
            """
            pd.DataFrame(
                {
                    "dataset": ["ndvi_raw", "ndvi_clean", "pairwise_weekly_features", "ndvi_phase_timeline", "pair_effect_tests", "final_hypothesis_register"],
                    "o_que_e": [
                        "metadado bruto do OneSoil por cena",
                        "NDVI limpo por cena valida",
                        "base semanal integrada por area",
                        "timeline semanal com flags e drivers",
                        "teste estatistico principal do projeto",
                        "fechamento das hipoteses H1-H4",
                    ],
                }
            )
            """
        ),
        markdown_cell(
            """
            ### 1.1.2 Como as hipoteses estao separadas

            O projeto nao testa hipotese para cada area isoladamente. Ele testa hipoteses por **par de comparacao**:

            - `grao` = `Grao 4.0` vs `Grao Convencional`
            - `silagem` = `Silagem 4.0` vs `Silagem Convencional`

            Entao o desenho correto e sempre:
            - quatro hipoteses para o par `grao`;
            - quatro hipoteses para o par `silagem`;
            - usando H1-H4 como regras comuns para os dois pares.

            O que **nao** faz sentido neste projeto:
            - comparar `grao 4.0` com `silagem 4.0`;
            - comparar `grao convencional` com `silagem convencional`;
            - testar hipotese separada para uma unica area sem o seu par.

            O motivo e simples: `grao` e `silagem` sao contextos agronomicos diferentes. A unidade correta de comparacao e sempre o **par**.
            """
        ),
        code_cell("pair_scope"),
        markdown_cell(
            """
            As hipoteses formais do projeto sao estas:

            - `H1`: 4.0 sustenta maior nivel temporal de NDVI no par
            - `H2`: 4.0 reduz semanas de problema no NDVI
            - `H3`: as semanas problema do NDVI apresentam drivers identificaveis
            - `H4`: o outlook pre-colheita favorece o 4.0 dentro do par

            Essas quatro hipoteses sao aplicadas separadamente para `grao` e para `silagem`.
            """
        ),
        markdown_cell(
            """
            ### 1.1.3 Primeiro olhar do dado bruto

            Aqui a ideia e olhar como um humano olhando o CSV pela primeira vez: tamanho, colunas, amostra e o que parece importante.
            """
        ),
        code_cell("ndvi_raw.shape, ndvi_clean.shape, ndvi_phase_timeline.shape"),
        code_cell("ndvi_raw.head()"),
        code_cell("ndvi_raw[['filename', 'season_id', 'date', 'b1_valid_pixels', 'b1_mean', 'b1_pct_solo', 'b1_pct_veg_densa']].head(12)"),
        markdown_cell(
            """
            Repare que `date` nao existe originalmente como coluna do CSV bruto. Ela e criada na carga a partir do `filename`.
            """
        ),
        code_cell("ndvi_raw[['filename', 'date', 'image_path']].head(10)"),
        markdown_cell(
            """
            ### 1.1.4 Regra mais importante do preparo: remover cenas NDVI invalidas

            A primeira decisao que realmente muda o dado e:

            - se `b1_valid_pixels <= 0`, a cena sai da analise principal;
            - se `b1_valid_pixels > 0`, a cena pode entrar no `ndvi_clean`.

            Isso acontece porque a `b1` e a banda usada como base do NDVI no projeto.
            """
        ),
        code_cell(
            """
            pd.DataFrame(
                {
                    "total_linhas_brutas": [len(ndvi_raw)],
                    "linhas_com_b1_valido": [int(ndvi_raw["b1_valid_pixels"].fillna(0).gt(0).sum())],
                    "linhas_com_b1_invalido": [int(ndvi_raw["b1_valid_pixels"].fillna(0).le(0).sum())],
                    "linhas_apos_filtro": [len(ndvi_clean)],
                }
            )
            """
        ),
        code_cell("dropped_ndvi_rows[['filename', 'season_id', 'date', 'b1_valid_pixels']].head(20)"),
        markdown_cell(
            """
            ### 1.1.5 Como o dado muda depois do preparo

            Depois do filtro e das renomeacoes, o projeto passa a trabalhar com colunas analiticas mais claras.
            """
        ),
        code_cell("ndvi_clean[['season_id', 'area_label', 'date', 'week_start', 'ndvi_mean', 'ndvi_std', 'soil_pct', 'dense_veg_pct', 'ndvi_delta', 'ndvi_auc']].head(15)"),
        code_cell("workspace['pairwise_weekly_features'][['season_id', 'area_label', 'week_start', 'ndvi_mean_week', 'soil_pct_week', 'dense_veg_pct_week']].head(15)"),
        code_cell("ndvi_phase_timeline[['season_id', 'area_label', 'week_start', 'ndvi_mean_week', 'ndvi_delta_week', 'ndvi_norm_week', 'low_vigor_flag', 'major_drop_flag', 'recovery_flag', 'primary_driver']].head(20)"),
        markdown_cell(
            """
            ## 2. Data Understanding

            A partir daqui comeca o aprofundamento tecnico do notebook: inventario das fontes, rastreabilidade, contratos, funcoes e tabelas de auditoria.

            Se voce quiser uma leitura mais humana do projeto, a secao `1.1 Leitura rapida do fluxo real` deve ser lida primeiro.
            """
        ),
        markdown_cell(
            """
            ### 2.0 Como ler este notebook

            A ideia deste notebook e ser o documento mestre do projeto.

            Se voce quiser:
            - entender a historia inteira, siga as secoes em ordem;
            - auditar as decisoes tecnicas, use as tabelas de politicas, catalogos e contratos;
            - ver a implementacao real, use as celulas `show_function_source(...)`;
            - aprofundar alem do notebook, abra os modulos listados em `module_map`.
            """
        ),
        markdown_cell(
            """
            ### 2.0.1 Leitura das tabelas no notebook

            As tabelas grandes sao mostradas em modo resumido por padrao para evitar celulas ilegiveis.

            Funcoes uteis:
            - `show_df(nome_df)`: mostra um recorte com quebra de texto;
            - `show_all(nome_df)`: mostra todas as linhas;
            - `show_row(nome_df, indice)`: mostra uma linha na vertical;
            - `show_csv(caminho_csv)`: abre um CSV e o exibe no mesmo formato.

            Se voce precisar dos valores completos fora do notebook, abra os arquivos em `notebook_outputs/complete_ndvi/`.
            """
        ),
        code_cell("show_df(module_map, title='Mapa dos modulos do projeto', rows=10, max_width_px=360)"),
        markdown_cell(
            """
            ### 2.1 Como o notebook funciona de fato

            O notebook nao chama `build_ndvi_clean()` diretamente em uma celula. Em vez disso, ele chama uma funcao de entrada unica, `build_complete_ndvi_workspace(DATA_DIR)`, e essa funcao vai descendo pelas camadas do projeto:

            - `farmlab.complete_analysis.build_complete_ndvi_workspace`
            - `farmlab.ndvi_crispdm.build_ndvi_crispdm_workspace`
            - `farmlab.ndvi_deepdive.build_ndvi_deep_dive_workspace`
            - `farmlab.pairwise.build_phase1_workspace`
            - `farmlab.io.discover_dataset_paths` + `load_*`

            Ou seja: os `.py` sao executados sim. O notebook e uma orquestracao da pipeline Python do projeto, e o `workspace` carrega todas as tabelas intermediarias e finais ja processadas.
            """
        ),
        code_cell("show_df(pipeline_flow, title='Fluxo do pipeline', rows=10, max_width_px=360)"),
        code_cell("show_df(storytelling_steps, title='Storytelling do fluxo', rows=10, max_width_px=360)"),
        code_cell("show_df(decision_register, title='Registro de decisoes tecnicas', rows=10, max_width_px=420)"),
        markdown_cell(
            """
            ### 2.2 Proveniencia dos arquivos brutos

            Esta tabela lista os arquivos efetivamente lidos pelo pipeline, se eles existem e o papel de cada um dentro da analise.
            """
        ),
        code_cell("show_df(source_inventory, title='Inventario das fontes brutas', rows=20, max_width_px=420)"),
        code_cell(
            """
            ndvi_source_columns = [
                "filename",
                "season_id",
                "date",
                "b1_valid_pixels",
                "b1_mean",
                "b1_std",
                "b1_pct_solo",
                "b1_pct_veg_densa",
                "bounds_left",
                "bounds_bottom",
                "bounds_right",
                "bounds_top",
                "image_path",
            ]
            show_df(ndvi_raw[ndvi_source_columns], title="Amostra das colunas brutas do NDVI", rows=12, max_width_px=220)
            """
        ),
        markdown_cell(
            """
            ### 2.2.1 Catalogo das colunas brutas do NDVI

            A tabela abaixo responde, coluna por coluna, se o campo do `ndvi_metadata.csv` e usado diretamente, apenas preservado para auditoria ou ignorado na analise principal.
            """
        ),
        code_cell("show_df(ndvi_raw_column_catalog, title='Catalogo das colunas brutas do NDVI', rows=80, max_width_px=420)"),
        markdown_cell(
            """
            ### 2.2.2 Politica de filtragem por `valid_pixels`

            A regra atual do projeto usa `b1_valid_pixels` como criterio de entrada em `ndvi_clean`. Isso acontece porque a banda `b1` e a base das metricas de NDVI realmente usadas no pipeline (`ndvi_mean`, `ndvi_std`, `soil_pct`, `dense_veg_pct`).

            `b2_valid_pixels` e `b3_valid_pixels` existem no bruto e sao mostrados abaixo para auditoria, mas nao participam do filtro principal nem das metricas centrais do projeto atual.
            """
        ),
        code_cell("show_df(valid_pixels_policy, title='Politica de valid_pixels', rows=10, max_width_px=420)"),
        code_cell("show_df(band_catalog, title='Uso de b1, b2 e b3', rows=10, max_width_px=360)"),
        code_cell("show_df(valid_pixels_summary, title='Resumo de valid_pixels', rows=10, max_width_px=280)"),
        code_cell("show_df(valid_pixels_matrix, title='Matriz de combinacoes de valid_pixels', rows=20, max_width_px=220)"),
        code_cell(
            """
            show_df(
                dropped_ndvi_rows[
                    [
                        "filename",
                        "season_id",
                        "date",
                        "b1_valid_pixels",
                        "b2_valid_pixels",
                        "b3_valid_pixels",
                    ]
                ],
                title="Amostra das linhas descartadas por b1_valid_pixels <= 0",
                rows=20,
                max_width_px=300,
            )
            """
        ),
        markdown_cell(
            """
            ### 2.3 Renomeacoes e transformacoes aplicadas

            O notebook nao usa nomes brutos diretamente em toda a analise. As principais transformacoes abaixo mostram como os campos do OneSoil e das bases auxiliares viraram metricas analiticas.
            """
        ),
        code_cell("show_df(transform_catalog, title='Catalogo de transformacoes', rows=30, max_width_px=360)"),
        markdown_cell(
            """
            ### 2.3.1 Catalogo das colunas derivadas

            Esta tabela registra as colunas novas criadas pelo pipeline, de onde elas vieram e por que elas existem.
            """
        ),
        code_cell("show_df(derived_columns_catalog, title='Catalogo das colunas derivadas', rows=40, max_width_px=360)"),
        markdown_cell(
            """
            ### 2.4 Catalogo de metricas

            Esta tabela resume as metricas centrais do projeto, o nivel temporal em que elas existem e em que parte da decisao elas entram.
            """
        ),
        code_cell("show_df(metric_catalog, title='Catalogo de metricas', rows=20, max_width_px=320)"),
        markdown_cell(
            """
            ### 2.4.1 Glossario estatistico e de decisao

            Esta tabela traduz os termos que aparecem nos CSVs finais e nas hipoteses, para evitar ambiguidade na leitura de `p-value`, `IC95%`, `lift`, `score`, `vantagem_4_0` e outros.
            """
        ),
        code_cell("show_df(stat_term_catalog, title='Glossario estatistico e de decisao', rows=20, max_width_px=360)"),
        markdown_cell(
            """
            ### 2.5 Contratos dos datasets

            Esta tabela mostra o contrato de cada base principal: granularidade, chaves e papel dentro do projeto.
            """
        ),
        code_cell("show_df(dataset_contracts, title='Contratos dos datasets', rows=20, max_width_px=360)"),
        markdown_cell(
            """
            ### 2.5.1 Uso das fontes auxiliares

            Esta tabela resume quais campos de clima, operacao, pragas e solo realmente entram na analise e por que eles importam.
            """
        ),
        code_cell("show_df(auxiliary_source_usage_catalog, title='Uso das fontes auxiliares', rows=20, max_width_px=420)"),
        markdown_cell(
            """
            ### 2.6 Mapa das funcoes do projeto

            Esta tabela resume quais funcoes realmente executam a logica do pipeline. O notebook usa um ponto de entrada unico, mas as decisoes tecnicas vivem nessas funcoes.
            """
        ),
        code_cell("show_df(function_map, title='Mapa das funcoes do projeto', rows=20, max_width_px=360)"),
        markdown_cell(
            """
            ### 2.7 Mapa pergunta -> evidencia -> decisao

            Esta tabela conecta as perguntas centrais do projeto com as metricas, tabelas, graficos e CSVs que realmente sustentam cada resposta.
            """
        ),
        code_cell("show_df(analysis_question_map, title='Mapa pergunta -> evidencia -> decisao', rows=20, max_width_px=420)"),
        code_cell(
            """
            raw_frames = {
                "ndvi_clean": ndvi_clean,
                "weather_daily": weather_daily,
                "ops_area_daily": ops_area_daily,
                "miip_daily": miip_daily,
                "soil_context": soil_context,
                "ndvi_phase_timeline": ndvi_phase_timeline,
                "transition_model_frame": transition_model_frame,
            }

            shape_rows = []
            for name, frame in raw_frames.items():
                shape_rows.append(
                    {
                        "dataset": name,
                        "shape": frame.shape,
                        "columns": ", ".join(frame.columns[:10]),
                    }
                )
            show_df(pd.DataFrame(shape_rows), title="Shape das bases principais", rows=20, max_width_px=320)
            """
        ),
        code_cell("show_df(dataset_overview, title='Visao geral dos datasets', rows=20, max_width_px=280)"),
        code_cell(
            """
            talhoes = (
                ndvi_clean[
                    [
                        "season_id",
                        "area_label",
                        "treatment",
                        "crop_type",
                        "comparison_pair",
                        "bounds_left",
                        "bounds_bottom",
                        "bounds_right",
                        "bounds_top",
                    ]
                ]
                .drop_duplicates(subset=["season_id"])
                .merge(
                    area_inventory[["season_id", "bbox_area_ha"]],
                    on="season_id",
                    how="left",
                )
                .assign(
                    center_x=lambda frame: (frame["bounds_left"] + frame["bounds_right"]) / 2,
                    center_y=lambda frame: (frame["bounds_bottom"] + frame["bounds_top"]) / 2,
                    formato_talhao="bounding_box_derivado_do_ndvi",
                )
                .sort_values(["comparison_pair", "area_label"])
                .reset_index(drop=True)
            )

            display(Markdown("### Mapa dos talhoes"))
            show_df(
                talhoes[
                    [
                        "area_label",
                        "treatment",
                        "crop_type",
                        "comparison_pair",
                        "formato_talhao",
                        "bbox_area_ha",
                        "center_x",
                        "center_y",
                        "bounds_left",
                        "bounds_bottom",
                        "bounds_right",
                        "bounds_top",
                        "season_id",
                    ]
                ],
                rows=20,
                max_width_px=220,
            )
            """
        ),
        code_cell(
            """
            describe_targets = {
                "ndvi_clean": ndvi_clean[["ndvi_mean", "ndvi_std", "soil_pct", "dense_veg_pct", "ndvi_delta", "ndvi_auc"]],
                "weather_daily": weather_daily[
                    [
                        "precipitation_mm",
                        "evapotranspiration_mm",
                        "water_balance_mm",
                        "temp_avg_c",
                        "humidity_avg_pct",
                        "wind_avg_kmh",
                    ]
                ],
                "ops_area_daily": ops_area_daily[
                    [
                        column
                        for column in [
                            "harvest_yield_mean_kg_ha",
                            "fert_dose_gap_abs_mean_kg_ha",
                            "overlap_area_pct_bbox",
                            "stop_duration_h_per_bbox_ha",
                        ]
                        if column in ops_area_daily.columns
                    ]
                ],
                "miip_daily": miip_daily[
                    [
                        column
                        for column in [
                            "avg_pest_count",
                            "total_pest_count",
                            "alert_hits",
                            "control_hits",
                            "damage_hits",
                        ]
                        if column in miip_daily.columns
                    ]
                ],
            }

            for name, frame in describe_targets.items():
                display(Markdown(f"### `.describe()` de `{name}`"))
                if frame.empty:
                    print("Base vazia.")
                    continue
                show_df(frame.describe().reset_index().rename(columns={"index": "metric"}), rows=50, max_width_px=220)
            """
        ),
        code_cell(
            """
            fig = px.bar(
                data_audit,
                x="area_label",
                y=["ndvi_valid_ratio", "weather_coverage_ratio", "miip_coverage_ratio"],
                barmode="group",
                title="Cobertura por area: NDVI valido, clima e MIIP",
            )
            fig.update_layout(yaxis_title="Razao de cobertura", legend_title="")
            fig
            """
        ),
        markdown_cell(
            """
            ## 3. Data Preparation

            Nesta etapa os dados foram integrados por area e por semana, que e a granularidade mais defensavel para comparar dinamica de NDVI, risco operacional, clima e pragas.
            """
        ),
        markdown_cell(
            """
            ### 3.1 Como a integracao e feita

            - `ndvi_clean`: base de cenas validas por talhao.
            - `ops_area_daily`: consolidacao operacional diaria por area.
            - `miip_daily`: armadilhas e pragas por area e dia.
            - `pairwise_weekly_features`: uniao semanal das fontes.
            - `ndvi_phase_timeline`: tabela final de semanas, flags, drivers e contexto para testes e decisoes.
            """
        ),
        code_cell(
            """
            show_df(pd.DataFrame(
                {
                    "dataset": ["ndvi_raw", "ndvi_clean", "pairwise_weekly_features", "ndvi_phase_timeline"],
                    "rows": [len(ndvi_raw), len(ndvi_clean), len(workspace["pairwise_weekly_features"]), len(ndvi_phase_timeline)],
                    "grain": ["cena bruta", "cena valida", "semana por area", "semana por area com flags e drivers"],
                }
            ), title="Granularidade por etapa", rows=10, max_width_px=280)
            """
        ),
        markdown_cell(
            """
            ### 3.2 Codigo-fonte das etapas-chave

            Abaixo estao trechos reais das funcoes principais que o notebook executa por baixo dos panos. Isso evita duplicar a implementacao inteira no Jupyter, mas deixa explicito o que esta rodando.
            """
        ),
        markdown_cell(
            """
            #### `build_ndvi_clean`

            Esta e a porta de entrada analitica do NDVI.

            O que ela faz:
            - recebe o bruto `ndvi_raw`, os metadados oficiais dos talhoes e o clima diario;
            - converte `date` para data normalizada;
            - descarta linhas com `b1_valid_pixels <= 0`;
            - junta `season_id` com o mapeamento oficial de area, tratamento e par;
            - cria `week_start`, `ndvi_mean`, `ndvi_std`, `soil_pct`, `dense_veg_pct`, `ndvi_delta` e `ndvi_auc`.

            Decisao tecnica importante:
            - somente a `b1` entra na analise principal; por isso o filtro de validade usa apenas `b1_valid_pixels`.

            Saida:
            - `ndvi_clean`, que vira a base principal do NDVI por cena valida.
            """
        ),
        code_cell("show_function_source(build_ndvi_clean)"),
        markdown_cell(
            """
            #### `build_ndvi_weekly`

            Esta funcao transforma a base por cena em base semanal.

            O que ela faz:
            - agrupa o `ndvi_clean` por `season_id` e `week_start`;
            - calcula `ndvi_mean_week`, `ndvi_peak_week`, `ndvi_std_week`, `ndvi_auc_week`, `soil_pct_week` e `dense_veg_pct_week`;
            - conta quantas imagens validas contribuíram para cada semana.

            Por que isso existe:
            - o dado NDVI nao e diario continuo;
            - semana reduz ruido de cena isolada;
            - semana e a granularidade usada para comparar 4.0 vs convencional.

            Saida:
            - a serie semanal base de vigor e cobertura por area.
            """
        ),
        code_cell("show_function_source(build_ndvi_weekly)"),
        markdown_cell(
            """
            #### `build_pairwise_weekly_features`

            Esta e a funcao que integra as fontes do projeto em uma base semanal unica.

            O que ela faz:
            - parte do NDVI semanal;
            - agrega clima por semana;
            - agrega operacao por semana e por area;
            - agrega MIIP/pragas por semana e por area;
            - devolve uma tabela comparavel entre os talhoes.

            Por que isso importa:
            - sem essa etapa, o projeto teria varias bases paralelas sem chave temporal comum;
            - com ela, o NDVI passa a ser analisado junto com clima, operacao e pragas.

            Saida:
            - `pairwise_weekly_features`, o feature store semanal do projeto.
            """
        ),
        code_cell("show_function_source(build_pairwise_weekly_features)"),
        markdown_cell(
            """
            #### `build_ndvi_phase_timeline`

            Esta funcao adiciona interpretacao temporal ao NDVI semanal.

            O que ela faz:
            - calcula `ndvi_delta_week` e `ndvi_norm_week`;
            - cria flags como `major_drop_flag`, `severe_drop_flag`, `recovery_flag` e `low_vigor_flag`;
            - monta flags de risco de solo, clima, pragas e operacao;
            - escolhe `primary_driver`, `secondary_driver`, `drivers_summary` e `story_sentence`.

            Decisao tecnica importante:
            - aqui o projeto deixa de olhar apenas nivel de NDVI e passa a olhar eventos e drivers.

            Saida:
            - `ndvi_phase_timeline`, a tabela central de semanas, flags, drivers e contexto.
            """
        ),
        code_cell("show_function_source(build_ndvi_phase_timeline)"),
        markdown_cell(
            """
            #### `build_pair_effect_tests`

            Esta funcao fecha os testes pareados principais do projeto.

            O que ela faz:
            - usa a timeline semanal como base;
            - compara 4.0 e convencional dentro de cada par;
            - calcula `vantagem_4_0`, `p_value`, `ci_low`, `ci_high` e classificacoes de evidencia;
            - testa as metricas principais de H1 e H2.

            Como ler:
            - `vantagem_4_0 > 0` favorece 4.0;
            - `vantagem_4_0 < 0` favorece convencional;
            - `p_value` e `IC95%` qualificam a robustez do efeito.

            Saida:
            - `pair_effect_tests`, a principal tabela estatistica do projeto.
            """
        ),
        code_cell("show_function_source(build_pair_effect_tests)"),
        markdown_cell(
            """
            #### `build_final_hypothesis_register`

            Esta funcao transforma as evidencias tecnicas em status formais de hipotese.

            O que ela faz:
            - consome `pair_effect_tests`, `event_driver_lift`, `ndvi_outlook`, `ndvi_pair_diagnostics` e `deep_dive_gaps`;
            - fecha H1, H2, H3 e H4 para cada par;
            - escreve `status`, `proof_basis` e `known_limits`.

            Decisao tecnica importante:
            - a tabela final nao inventa conclusao nova; ela consolida o que as etapas anteriores ja sustentaram ou deixaram inconclusivo.

            Saida:
            - `final_hypothesis_register`, o fechamento executivo das hipoteses do projeto.
            """
        ),
        code_cell("show_function_source(build_final_hypothesis_register)"),
        code_cell("show_df(numeric_profiles, title='Perfis numericos', rows=40, max_width_px=280)"),
        code_cell("show_df(transition_model_frame, title='Amostra do transition_model_frame', rows=20, max_width_px=280)"),
        markdown_cell(
            """
            ### 3.3 Rastreabilidade dos outputs finais

            As tabelas abaixo deixam claro como cada CSV final nasce, o que ele responde e quando ele deve ser consultado.
            """
        ),
        markdown_cell(
            """
            #### Por que existem tantos CSVs?

            Porque o projeto nao faz uma coisa so. Ele faz seis tarefas diferentes:
            - inventariar e auditar as bases;
            - resumir o NDVI;
            - comparar 4.0 vs convencional;
            - procurar drivers associados;
            - testar uma modelagem interpretavel;
            - fechar hipoteses e decisao final.

            Por isso os CSVs foram separados por funcao. Nao e bagunca: cada arquivo responde uma pergunta diferente.
            """
        ),
        code_cell("show_df(csv_family_map, title='Familias de CSVs do projeto', rows=20, max_width_px=420)"),
        code_cell("show_df(output_lineage, title='Linhagem dos CSVs finais', rows=30, max_width_px=420)"),
        code_cell("show_df(csv_reading_guide, title='Guia de leitura dos CSVs', rows=30, max_width_px=420)"),
        markdown_cell(
            """
            ## 4. Exploratory Statistical Analysis

            Aqui entram estatistica descritiva, identificacao de outliers por z-score, trajetoria temporal e tendencia por area.
            """
        ),
        markdown_cell(
            """
            #### O que e `z-score` neste projeto?

            `z-score` mede o quanto uma observacao esta distante da media da propria serie, em unidades de desvio padrao.

            Leitura:
            - `z-score` perto de `0`: comportamento normal da serie;
            - `z-score` muito positivo: NDVI acima do padrao;
            - `z-score` muito negativo: NDVI abaixo do padrao;
            - o notebook marca outlier quando o valor passa dos limiares definidos pelo pipeline.

            Aqui o `z-score` nao decide hipoteses sozinho. Ele serve para destacar cenas anormais que merecem investigacao.
            """
        ),
        code_cell("show_df(ndvi_stats_by_area, title='Estatisticas de NDVI por area', rows=20, max_width_px=260)"),
        code_cell("show_df(ndvi_trend_tests, title='Testes de tendencia do NDVI', rows=20, max_width_px=280)"),
        code_cell(
            """
            fig = px.line(
                ndvi_phase_timeline.sort_values("week_start"),
                x="week_start",
                y="ndvi_mean_week",
                color="area_label",
                facet_row="comparison_pair",
                markers=True,
                title="NDVI medio semanal por area e por par",
            )
            fig.update_layout(height=850)
            fig
            """
        ),
        code_cell(
            """
            flagged = ndvi_outliers.copy()
            flagged["date"] = pd.to_datetime(flagged["date"], errors="coerce")

            fig = px.scatter(
                flagged,
                x="date",
                y="ndvi_zscore",
                color="area_label",
                symbol="outlier_flag",
                facet_row="comparison_pair",
                hover_data=["ndvi_mean", "outlier_direction"],
                title="Z-score do NDVI por cena",
            )
            fig.add_hline(y=2.0, line_dash="dash", line_color="#b91c1c")
            fig.add_hline(y=-2.0, line_dash="dash", line_color="#b91c1c")
            fig.update_layout(height=780)
            fig
            """
        ),
        code_cell(
            """
            fig = px.histogram(
                ndvi_outliers,
                x="ndvi_zscore",
                color="comparison_pair",
                nbins=30,
                barmode="overlay",
                opacity=0.7,
                title="Distribuicao do z-score do NDVI",
            )
            fig.add_vline(x=2.0, line_dash="dash", line_color="#b91c1c")
            fig.add_vline(x=-2.0, line_dash="dash", line_color="#b91c1c")
            fig
            """
        ),
        code_cell("show_df(ndvi_outliers[ndvi_outliers['outlier_flag']], title='Outliers de NDVI', rows=30, max_width_px=260)"),
        markdown_cell(
            """
            ## 5. Pairwise Statistical Testing

            Esta secao combina os testes pareados ja existentes no projeto com testes classicos adicionais.

            Perguntas desta etapa:
            - existe diferenca estatisticamente relevante entre 4.0 e convencional?
            - essa diferenca aparece no nivel medio do NDVI, na area sob a curva e nos sinais de problema?
            - qual teste e mais apropriado em cada caso, dado o comportamento dos gaps semanais?
            """
        ),
        markdown_cell(
            """
            #### O que esta acontecendo aqui, de fato?

            Esta e a secao que responde se o 4.0 ficou acima ou abaixo do convencional com evidencia estatistica.

            Existem duas tabelas porque elas respondem coisas complementares:
            - `pair_effect_tests`: e a tabela principal do projeto; fecha a leitura pareada usada nas hipoteses;
            - `pair_classic_tests`: faz uma validacao estatistica classica adicional sobre os gaps semanais.

            Como ler os campos principais:
            - `advantage_4_0`: diferenca media `4.0 - convencional`;
            - `p_value`: quao compativel o resultado e com ausencia de diferenca;
            - `ci_low` e `ci_high`: intervalo de confianca do efeito;
            - `paired_effect_size` ou `paired_effect_size_dz`: tamanho padronizado do efeito.
            """
        ),
        code_cell("show_df(pair_effect_tests, title='Testes pareados principais', rows=30, max_width_px=320)"),
        code_cell("show_df(pair_classic_tests, title='Testes classicos por par', rows=30, max_width_px=320)"),
        code_cell(
            """
            pvals = pair_effect_tests.copy()
            fig = px.bar(
                pvals,
                x="metric_label",
                y="p_value",
                color="winner",
                facet_row="comparison_pair",
                title="P-values dos testes pareados principais",
                hover_data=["advantage_4_0", "ci_low", "ci_high", "evidence_level"],
            )
            fig.add_hline(y=0.05, line_dash="dash", line_color="#b91c1c")
            fig.update_layout(height=900)
            fig
            """
        ),
        code_cell(
            """
            ndvi_gap_plot = pair_weekly_gaps.copy()
            fig = px.line(
                ndvi_gap_plot.dropna(subset=["gap_ndvi_mean_week_4_0_minus_convencional"]),
                x="week_start",
                y="gap_ndvi_mean_week_4_0_minus_convencional",
                color="comparison_pair",
                markers=True,
                title="Gap semanal de NDVI medio: 4.0 - convencional",
                labels={"gap_ndvi_mean_week_4_0_minus_convencional": "Gap NDVI"},
            )
            fig.add_hline(y=0, line_dash="dash", line_color="#94a3b8")
            fig
            """
        ),
        code_cell(
            """
            frame = pair_classic_tests.copy()
            fig = px.bar(
                frame,
                x="metric_label",
                y="mean_favorable_gap_4_0",
                color="favors",
                facet_row="comparison_pair",
                hover_data=["recommended_test", "recommended_p_value", "paired_effect_size_dz"],
                title="Gap medio favoravel ao 4.0 por metrica",
                color_discrete_map={
                    "favorece_4_0": "#0f766e",
                    "favorece_convencional": "#b91c1c",
                    "inconclusivo": "#64748b",
                },
            )
            fig.update_layout(height=900)
            fig
            """
        ),
        markdown_cell(
            """
            ## 6. Correlation Analysis

            Correlacao nao prova causalidade, mas ajuda a priorizar variaveis que merecem leitura agronomica mais cuidadosa.
            """
        ),
        code_cell("show_df(weekly_correlations, title='Correlacoes semanais', rows=40, max_width_px=320)"),
        code_cell(
            """
            corr_plot = weekly_correlations[
                (weekly_correlations["analysis_target"] == "delta_ndvi_seguinte")
                & (weekly_correlations["comparison_pair"] == "geral")
            ].head(15)

            fig = px.bar(
                corr_plot,
                x="strongest_abs_correlation",
                y="feature",
                orientation="h",
                color="direction",
                hover_data=["pearson_r", "pearson_p", "spearman_rho", "spearman_p", "strength"],
                title="Top correlacoes com o delta do NDVI da semana seguinte",
                color_discrete_map={"positiva": "#0f766e", "negativa": "#b91c1c", "sem_relacao": "#64748b"},
            )
            fig.update_layout(height=720, yaxis={"categoryorder": "total ascending"})
            fig
            """
        ),
        markdown_cell(
            """
            ## 7. Deep Dive de Eventos

            Esta secao detalha semanas problema, drivers associados e imagens de apoio.
            """
        ),
        code_cell("show_df(event_driver_lift, title='Drivers sobre-representados', rows=30, max_width_px=320)"),
        code_cell(
            """
            fig = px.bar(
                event_driver_lift,
                x="driver",
                y="delta_pp",
                color="evidence_level",
                facet_row="comparison_pair",
                title="Drivers sobre-representados nas semanas problema do NDVI",
                color_discrete_map={"alta": "#0f766e", "media": "#c58b00", "baixa": "#64748b"},
            )
            fig.update_layout(height=760)
            fig
            """
        ),
        code_cell("show_df(ndvi_events, title='Amostra de eventos NDVI', rows=20, max_width_px=320)"),
        code_cell("event_gallery_html(ndvi_events, ndvi_clean, max_events=12)"),
        markdown_cell(
            """
            ## 8. Modeling

            A modelagem foi mantida interpretavel. O alvo modelado e a variacao do NDVI na semana seguinte.
            """
        ),
        markdown_cell(
            """
            #### Existe modelo no projeto?

            Sim. Mas nao e um modelo de caixa-preta para vender previsao forte.

            O que o projeto faz:
            - monta `transition_model_frame` a partir da timeline semanal;
            - define como alvo `target_next_ndvi_delta`, isto e, a variacao do NDVI na semana seguinte;
            - ajusta um modelo linear regularizado e interpretavel;
            - mede desempenho dentro da amostra e em `leave-one-area-out`.

            Como ler esta secao:
            - se `loo_r2` for fraco ou negativo, o modelo nao deve ser vendido como previsao robusta;
            - mesmo assim os coeficientes ajudam a interpretar quais variaveis empurram ou pressionam o NDVI futuro.
            """
        ),
        code_cell("show_df(transition_model_summary, title='Resumo do modelo de transicao', rows=20, max_width_px=280)"),
        code_cell("show_df(transition_model_coefficients, title='Coeficientes do modelo', rows=20, max_width_px=300)"),
        code_cell(
            """
            fig = px.bar(
                transition_model_coefficients.head(20),
                x="coefficient",
                y="feature",
                orientation="h",
                color="direction",
                title="Coeficientes padronizados do modelo de transicao do NDVI",
                color_discrete_map={"aumenta_ndvi_futuro": "#0f766e", "pressiona_ndvi_futuro": "#b91c1c"},
            )
            fig.update_layout(height=760, yaxis={"categoryorder": "total ascending"})
            fig
            """
        ),
        code_cell(
            """
            fig = px.scatter(
                transition_model_predictions,
                x="target_next_ndvi_delta",
                y="loo_predicted_next_ndvi_delta",
                color="area_label",
                facet_row="comparison_pair",
                title="Ajuste leave-one-area-out: delta real vs previsto do NDVI",
                labels={
                    "target_next_ndvi_delta": "Delta real da proxima semana",
                    "loo_predicted_next_ndvi_delta": "Delta previsto",
                },
            )
            fig.add_shape(type="line", x0=-0.2, y0=-0.2, x1=0.2, y1=0.2, line={"dash": "dash", "color": "#94a3b8"})
            fig.update_layout(height=760)
            fig
            """
        ),
        markdown_cell(
            """
            ## 9. Evaluation

            Sintese das hipoteses, decisoes e lacunas ainda abertas.
            """
        ),
        markdown_cell(
            """
            ### 9.1 Painel visual das hipoteses

            Esta secao tenta responder do jeito certo para apresentacao:

            - qual e a hipotese;
            - qual grafico sustenta essa hipotese;
            - o que o grafico mostra;
            - qual foi o status final para `grao` e `silagem`.

            O objetivo aqui nao e so listar CSV. E mostrar visualmente por que cada hipotese ficou `suportada`, `nao_suportada` ou `inconclusiva`.
            """
        ),
        markdown_cell(
            """
            #### 9.1.1 Separacao correta dos pares

            Antes de olhar os graficos, a leitura correta e esta:

            - `grao`: compara `Grao 4.0` com `Grao Convencional`;
            - `silagem`: compara `Silagem 4.0` com `Silagem Convencional`.

            As mesmas quatro hipoteses H1-H4 sao avaliadas em cada par.
            """
        ),
        code_cell("pair_scope"),
        markdown_cell(
            """
            #### 9.1.2 Painel de status final

            Este painel resume o fechamento oficial das hipoteses por par.
            """
        ),
        code_cell(
            """
            status_order = {"nao_suportada": 0, "inconclusiva": 1, "suportada": 2}
            panel = final_hypothesis_register[["comparison_pair", "hypothesis_id", "status"]].copy()
            panel["status_score"] = panel["status"].map(status_order)
            panel = panel.pivot(index="hypothesis_id", columns="comparison_pair", values="status")
            panel = panel.reindex(index=["H1", "H2", "H3", "H4"], columns=["grao", "silagem"])
            z = panel.replace(status_order)

            fig = go.Figure(
                data=go.Heatmap(
                    z=z.values,
                    x=list(z.columns),
                    y=list(z.index),
                    text=panel.values,
                    texttemplate="%{text}",
                    textfont={"size": 14},
                    colorscale=[
                        [0.00, "#b91c1c"],
                        [0.33, "#b91c1c"],
                        [0.34, "#64748b"],
                        [0.66, "#64748b"],
                        [0.67, "#0f766e"],
                        [1.00, "#0f766e"],
                    ],
                    zmin=0,
                    zmax=2,
                    showscale=False,
                    hovertemplate="Par: %{x}<br>Hipotese: %{y}<br>Status: %{text}<extra></extra>",
                )
            )
            fig.update_layout(title="Painel visual das hipoteses por par", height=420)
            fig
            """
        ),
        code_cell("show_df(hypothesis_catalog, title='Catalogo das hipoteses', rows=10, max_width_px=320)"),
        code_cell("show_df(analysis_question_map, title='Pergunta -> evidencia -> decisao', rows=20, max_width_px=420)"),
        markdown_cell(
            """
            #### 9.1.3 H1 - 4.0 sustenta maior nivel temporal de NDVI no par

            Grafico usado:
            - `NDVI medio semanal por area e por par`;
            - e o grafico abaixo, que resume a `vantagem_4_0` com intervalo de confianca.

            Como ler:
            - barra acima de zero: 4.0 acima do convencional;
            - barra abaixo de zero: 4.0 abaixo do convencional;
            - se o intervalo de confianca nao cruza zero, a leitura fica mais robusta.
            """
        ),
        code_cell(
            """
            h1_frame = pair_effect_tests[pair_effect_tests["metric"] == "ndvi_mean_week"].copy()
            h1_frame["error_plus"] = h1_frame["ci_high"] - h1_frame["advantage_4_0"]
            h1_frame["error_minus"] = h1_frame["advantage_4_0"] - h1_frame["ci_low"]

            fig = go.Figure(
                data=[
                    go.Bar(
                        x=h1_frame["comparison_pair"],
                        y=h1_frame["advantage_4_0"],
                        marker_color=h1_frame["winner"].map(
                            {
                                "favorece_4_0": "#0f766e",
                                "favorece_convencional": "#b91c1c",
                                "inconclusivo": "#64748b",
                            }
                        ),
                        error_y=dict(
                            type="data",
                            symmetric=False,
                            array=h1_frame["error_plus"],
                            arrayminus=h1_frame["error_minus"],
                        ),
                        text=h1_frame["advantage_4_0"].round(3),
                        textposition="outside",
                        hovertemplate=(
                            "Par: %{x}<br>vantagem_4_0: %{y:.3f}<br>"
                            "IC95%: %{customdata[0]:.3f} a %{customdata[1]:.3f}<br>"
                            "p-value: %{customdata[2]:.4f}<br>"
                            "evidencia: %{customdata[3]}<extra></extra>"
                        ),
                        customdata=h1_frame[["ci_low", "ci_high", "p_value", "evidence_level"]].to_numpy(),
                    )
                ]
            )
            fig.add_hline(y=0, line_dash="dash", line_color="#94a3b8")
            fig.update_layout(
                title="H1 - vantagem do 4.0 no NDVI medio semanal",
                xaxis_title="Par",
                yaxis_title="vantagem_4_0 no NDVI medio semanal",
                height=460,
            )
            fig
            """
        ),
        code_cell(
            """
            show_df(
                pair_effect_tests[pair_effect_tests["metric"] == "ndvi_mean_week"],
                title="Evidencia estatistica de H1",
                rows=10,
                max_width_px=420,
            )
            """
        ),
        markdown_cell(
            """
            #### 9.1.4 H2 - 4.0 reduz semanas de problema no NDVI

            Grafico usado:
            - proporcao de semanas com `baixo vigor` e `queda relevante` por area.

            Como ler:
            - menor barra = menos semanas problema;
            - se o 4.0 tiver menos semanas problema de forma consistente, H2 tende a ser suportada;
            - se os lados ficarem muito parecidos ou contraditorios, H2 tende a ficar inconclusiva.
            """
        ),
        code_cell(
            """
            h2_rates = (
                ndvi_phase_timeline.groupby(["comparison_pair", "area_label", "treatment"], as_index=False)
                .agg(
                    total_weeks=("week_start", "nunique"),
                    low_vigor_rate=("low_vigor_flag", "mean"),
                    major_drop_rate=("major_drop_flag", "mean"),
                )
                .melt(
                    id_vars=["comparison_pair", "area_label", "treatment", "total_weeks"],
                    value_vars=["low_vigor_rate", "major_drop_rate"],
                    var_name="problem_metric",
                    value_name="problem_rate",
                )
            )
            h2_rates["problem_metric"] = h2_rates["problem_metric"].map(
                {
                    "low_vigor_rate": "baixo vigor",
                    "major_drop_rate": "queda relevante",
                }
            )

            fig = px.bar(
                h2_rates,
                x="area_label",
                y="problem_rate",
                color="problem_metric",
                facet_row="comparison_pair",
                barmode="group",
                hover_data=["treatment", "total_weeks"],
                title="H2 - proporcao de semanas problema por area",
            )
            fig.update_layout(height=760, yaxis_title="proporcao de semanas")
            fig
            """
        ),
        code_cell(
            """
            show_df(
                pair_effect_tests[pair_effect_tests["metric"].isin(["low_vigor_flag", "major_drop_flag"])],
                title="Evidencia estatistica de H2",
                rows=10,
                max_width_px=420,
            )
            """
        ),
        markdown_cell(
            """
            #### 9.1.5 H3 - as semanas problema apresentam drivers identificaveis

            Grafico usado:
            - drivers sobre-representados nas semanas problema.

            Como ler:
            - `delta_pp` alto indica que o driver aparece muito mais nas semanas problema do que no baseline;
            - `evidence_level` ajuda a separar driver robusto de driver fraco.
            """
        ),
        code_cell(
            """
            h3_frame = (
                event_driver_lift.sort_values(["comparison_pair", "delta_pp"], ascending=[True, False])
                .groupby("comparison_pair", as_index=False, sort=False)
                .head(5)
            )

            fig = px.bar(
                h3_frame,
                x="driver",
                y="delta_pp",
                color="evidence_level",
                facet_row="comparison_pair",
                hover_data=["problem_rate", "baseline_rate", "lift_ratio"],
                title="H3 - drivers mais fortes nas semanas problema",
                color_discrete_map={"alta": "#0f766e", "media": "#c58b00", "baixa": "#64748b"},
            )
            fig.update_layout(height=760, yaxis_title="delta_pp")
            fig
            """
        ),
        code_cell(
            """
            show_df(
                event_driver_lift.sort_values(["comparison_pair", "delta_pp"], ascending=[True, False]),
                title="Evidencia estatistica de H3",
                rows=20,
                max_width_px=420,
            )
            """
        ),
        markdown_cell(
            """
            #### 9.1.6 H4 - o outlook pre-colheita favorece o 4.0

            Grafico usado:
            - `trajectory_score` e `expected_vs_pair` por area.

            Como ler:
            - score maior sugere trajetoria final mais favoravel;
            - `expected_vs_pair` mostra se o 4.0 tende a chegar acima, abaixo ou sem vantagem clara no par;
            - esta hipotese nao depende so do NDVI medio, mas da leitura final da trajetoria e dos riscos acumulados.
            """
        ),
        code_cell(
            """
            h4_frame = ndvi_outlook.sort_values(["comparison_pair", "trajectory_score"], ascending=[True, False]).copy()

            fig = px.bar(
                h4_frame,
                x="area_label",
                y="trajectory_score",
                color="expected_vs_pair",
                facet_row="comparison_pair",
                hover_data=["outlook_band", "latest_phase", "latest_event", "major_drop_weeks", "low_vigor_weeks", "top_risks"],
                title="H4 - outlook pre-colheita por area",
                color_discrete_map={
                    "tende_a_chegar_acima_do_par": "#0f766e",
                    "sem_vantagem_clara_no_par": "#64748b",
                    "tende_a_chegar_abaixo_do_par": "#b91c1c",
                },
            )
            fig.update_layout(height=760, yaxis_title="trajectory_score")
            fig
            """
        ),
        code_cell("show_df(ndvi_outlook, title='Base de outlook usada em H4', rows=10, max_width_px=420)"),
        code_cell("show_df(ndvi_pair_diagnostics, title='Diagnostico pareado da trajetoria', rows=10, max_width_px=420)"),
        markdown_cell(
            """
            #### 9.1.7 Fechamento formal das hipoteses

            Depois dos graficos e das tabelas de evidencia, o fechamento oficial continua sendo este registro final.
            """
        ),
        code_cell("show_df(final_hypothesis_register, title='Registro final de hipoteses', rows=20, max_width_px=420)"),
        code_cell("show_df(decision_summary, title='Resumo final de decisao', rows=20, max_width_px=420)"),
        code_cell(
            """
            sample_rows = sample_rows_per_area(ndvi_clean, max_images_per_area=2)
            gallery_html(
                sample_rows,
                title="Galeria visual de apoio por area",
                subtitle_columns=["comparison_pair", "treatment", "ndvi_mean", "soil_pct", "dense_veg_pct"],
            )
            """
        ),
        code_cell(
            """
            show_df(pd.DataFrame({"gap": deep_dive_gaps}), title="Resumo dos gaps do deep dive", rows=20, max_width_px=480)
            """
        ),
        markdown_cell(
            """
            ### 9.2 Limites de confianca e pontos de cautela

            Esta tabela resume o que pode ser tratado como robusto, o que e intermediario e o que ainda depende de validacao adicional fora do notebook.
            """
        ),
        code_cell("show_df(trust_notes, title='Limites de confianca e cautelas', rows=20, max_width_px=420)"),
        markdown_cell(
            """
            ## 10. Export

            Esta celula grava os CSVs analiticos finais em `OUTPUT_DIR`.
            """
        ),
        code_cell(
            """
            written_files = save_complete_ndvi_outputs(workspace, OUTPUT_DIR)
            show_df(pd.DataFrame({"written_file": [str(path) for path in written_files]}), title="Arquivos escritos no export", rows=40, max_width_px=520)
            """
        ),
        markdown_cell(
            """
            ### 10.1 Verificacao de reprodutibilidade

            Esta verificacao recompõe os CSVs em um diretorio temporario e compara os hashes SHA-256 com os arquivos salvos em `OUTPUT_DIR`.
            """
        ),
        code_cell("show_df(verify_current_outputs_against_workspace(workspace, OUTPUT_DIR), title='Verificacao de reprodutibilidade por hash', rows=40, max_width_px=420)"),
    ]

    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python", "version": "3.11"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def main() -> None:
    project_dir = Path(__file__).resolve().parents[1]
    notebook_path = project_dir / "notebooks" / "complete_ndvi_analysis.ipynb"
    notebook_path.parent.mkdir(parents=True, exist_ok=True)
    notebook_path.write_text(json.dumps(build_notebook(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(notebook_path)


if __name__ == "__main__":
    main()
