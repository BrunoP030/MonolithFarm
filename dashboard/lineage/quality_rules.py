from __future__ import annotations

from typing import Any

import pandas as pd


def run_quality_rules(
    workspace: dict[str, Any] | None,
    outputs: dict[str, pd.DataFrame] | None = None,
) -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    workspace = workspace or {}
    outputs = outputs or {}
    rows: list[dict[str, Any]] = []
    examples: dict[str, pd.DataFrame] = {}

    def add_rule(
        *,
        rule_id: str,
        domain: str,
        severity: str,
        dataset: str,
        quality_dimension: str,
        affected_columns: list[str],
        rule: str,
        reason: str,
        impact: str,
        recommendation: str,
        action: str,
        affected: pd.DataFrame,
    ) -> None:
        affected = affected.copy()
        examples[rule_id] = affected.head(50)
        rows.append(
            {
                "rule_id": rule_id,
                "domain": domain,
                "severity": severity,
                "dataset": dataset,
                "quality_dimension": quality_dimension,
                "affected_columns": ", ".join(affected_columns),
                "affected_rows": int(len(affected)),
                "status": "ok" if affected.empty else "atenção",
                "rule": rule,
                "reason": reason,
                "impact": impact,
                "recommendation": recommendation,
                "action": action,
            }
        )

    ndvi_raw = workspace.get("ndvi_raw")
    if isinstance(ndvi_raw, pd.DataFrame) and not ndvi_raw.empty:
        add_rule(
            rule_id="ndvi_b1_valid_pixels_zero",
            domain="NDVI",
            severity="info",
            dataset="ndvi_raw",
            quality_dimension="completude",
            affected_columns=["b1_valid_pixels", "filename", "season_id"],
            rule="b1_valid_pixels <= 0",
            reason="Cenas sem pixel NDVI válido não representam vigor útil.",
            impact="Essas linhas são descartadas antes de ndvi_clean.",
            recommendation="Manter visível no painel e não usar em métricas de vigor.",
            action="descartado_da_base_analitica",
            affected=ndvi_raw[_numeric(ndvi_raw, "b1_valid_pixels").fillna(0).le(0)] if "b1_valid_pixels" in ndvi_raw else pd.DataFrame(),
        )
        add_rule(
            rule_id="ndvi_b1_mean_outside_range",
            domain="NDVI",
            severity="warning",
            dataset="ndvi_raw",
            quality_dimension="validade_fisica",
            affected_columns=["b1_mean"],
            rule="b1_mean fora de [-1, 1]",
            reason="NDVI físico deve ficar no intervalo [-1, 1].",
            impact="Valores fora da faixa indicam erro de escala ou extração.",
            recommendation="Auditar escala/origem do raster antes de usar a cena.",
            action="mantido_com_alerta",
            affected=ndvi_raw[_numeric(ndvi_raw, "b1_mean").lt(-1) | _numeric(ndvi_raw, "b1_mean").gt(1)] if "b1_mean" in ndvi_raw else pd.DataFrame(),
        )
        add_rule(
            rule_id="ndvi_duplicate_scene_key",
            domain="NDVI",
            severity="warning",
            dataset="ndvi_raw",
            quality_dimension="consistencia",
            affected_columns=["season_id", "filename", "date"],
            rule="season_id + filename duplicado",
            reason="A mesma cena duplicada pode pesar duas vezes na série temporal.",
            impact="Pode distorcer médias semanais, outliers e AUC.",
            recommendation="Confirmar se duplicatas representam reprocessamento ou repetição acidental.",
            action="mantido_com_alerta",
            affected=ndvi_raw[ndvi_raw.duplicated(subset=[c for c in ["season_id", "filename"] if c in ndvi_raw.columns], keep=False)]
            if {"season_id", "filename"}.issubset(ndvi_raw.columns)
            else pd.DataFrame(),
        )
        add_rule(
            rule_id="ndvi_temporal_gap_gt_12_days",
            domain="NDVI",
            severity="info",
            dataset="ndvi_raw",
            quality_dimension="atualidade",
            affected_columns=["season_id", "date"],
            rule="intervalo entre cenas válidas > 12 dias",
            reason="Lacunas longas reduzem precisão da trajetória semanal do NDVI.",
            impact="Pode esconder quedas rápidas ou atrasar detecção de recuperação.",
            recommendation="Interpretar semanas sem cena próxima com menor confiança.",
            action="mantido_com_alerta",
            affected=_temporal_gaps(ndvi_raw, "season_id", "date", max_days=12),
        )
        pct_columns = [column for column in ndvi_raw.columns if column.startswith("b1_pct_")]
        for column in pct_columns:
            values = _numeric(ndvi_raw, column)
            add_rule(
                rule_id=f"ndvi_{column}_outside_pct",
                domain="NDVI",
                severity="warning",
                dataset="ndvi_raw",
                quality_dimension="validade_fisica",
                affected_columns=[column],
                rule=f"{column} fora de 0-100%",
                reason="Percentuais de classes de cobertura devem ficar entre 0 e 100.",
                impact="Afeta leitura de solo exposto e vegetação densa.",
                recommendation="Auditar extração/classificação da cena.",
                action="mantido_com_alerta",
                affected=ndvi_raw[values.lt(0) | values.gt(100)],
            )
        if {"b1_pct_solo", "b1_pct_veg_densa"}.issubset(ndvi_raw.columns):
            coverage_sum = _numeric(ndvi_raw, "b1_pct_solo") + _numeric(ndvi_raw, "b1_pct_veg_densa")
            add_rule(
                rule_id="ndvi_soil_dense_veg_pct_sum_gt_100",
                domain="NDVI",
                severity="warning",
                dataset="ndvi_raw",
                quality_dimension="consistencia",
                affected_columns=["b1_pct_solo", "b1_pct_veg_densa"],
                rule="b1_pct_solo + b1_pct_veg_densa > 100",
                reason="Classes percentuais principais não deveriam exceder 100% quando interpretadas como partes da cena.",
                impact="Pode tornar solo_exposto e vegetação densa incoerentes.",
                recommendation="Verificar se as classes são mutuamente exclusivas na fonte OneSoil.",
                action="mantido_com_alerta",
                affected=ndvi_raw[coverage_sum.gt(100)],
            )

    ndvi_clean = workspace.get("ndvi_clean")
    if isinstance(ndvi_clean, pd.DataFrame) and not ndvi_clean.empty:
        add_rule(
            rule_id="ndvi_clean_missing_keys",
            domain="NDVI",
            severity="error",
            dataset="ndvi_clean",
            quality_dimension="completude",
            affected_columns=["season_id", "date", "ndvi_mean"],
            rule="season_id/date/ndvi_mean nulos",
            reason="Sem essas chaves não há rastreio temporal confiável.",
            impact="Linhas afetadas comprometem agregações semanais.",
            recommendation="Corrigir origem ou descartar linhas sem chave antes de publicar resultado.",
            action="bloqueia_confianca_da_linha",
            affected=_missing_any(ndvi_clean, ["season_id", "date", "ndvi_mean"]),
        )

    weather_daily = workspace.get("weather_daily")
    if isinstance(weather_daily, pd.DataFrame) and not weather_daily.empty:
        add_rule(
            rule_id="weather_temp_avg_implausible",
            domain="Clima",
            severity="warning",
            dataset="weather_daily",
            quality_dimension="validade_fisica",
            affected_columns=["temp_avg_c"],
            rule="temp_avg_c fora de [-10, 50]",
            reason="Faixa física plausível para temperatura média diária na região.",
            impact="Pode distorcer drivers de estresse climático.",
            recommendation="Conferir sensor, unidade e encoding do CSV Metos.",
            action="mantido_com_alerta",
            affected=weather_daily[_numeric(weather_daily, "temp_avg_c").lt(-10) | _numeric(weather_daily, "temp_avg_c").gt(50)] if "temp_avg_c" in weather_daily else pd.DataFrame(),
        )
        add_rule(
            rule_id="weather_humidity_outside_pct",
            domain="Clima",
            severity="warning",
            dataset="weather_daily",
            quality_dimension="validade_fisica",
            affected_columns=["humidity_avg_pct"],
            rule="humidity_avg_pct fora de 0-100%",
            reason="Umidade relativa é percentual.",
            impact="Indica erro de escala ou sensor.",
            recommendation="Conferir unidade e calibragem/extração da estação.",
            action="mantido_com_alerta",
            affected=weather_daily[_numeric(weather_daily, "humidity_avg_pct").lt(0) | _numeric(weather_daily, "humidity_avg_pct").gt(100)] if "humidity_avg_pct" in weather_daily else pd.DataFrame(),
        )
        add_rule(
            rule_id="weather_wind_implausible",
            domain="Clima",
            severity="warning",
            dataset="weather_daily",
            quality_dimension="precisao",
            affected_columns=["wind_avg_kmh"],
            rule="wind_avg_kmh > 120",
            reason="Vento médio diário acima desse valor é improvável para operação agrícola normal.",
            impact="Pode gerar interpretação incorreta de clima operacional.",
            recommendation="Auditar unidade e média diária.",
            action="mantido_com_alerta",
            affected=weather_daily[_numeric(weather_daily, "wind_avg_kmh").gt(120)] if "wind_avg_kmh" in weather_daily else pd.DataFrame(),
        )
        precipitation = _numeric(weather_daily, "precipitation_mm")
        add_rule(
            rule_id="weather_precipitation_negative_or_extreme",
            domain="Clima",
            severity="warning",
            dataset="weather_daily",
            quality_dimension="validade_fisica",
            affected_columns=["precipitation_mm"],
            rule="precipitation_mm < 0 ou > 300 mm/dia",
            reason="Chuva negativa é impossível; valores diários extremos precisam ser auditados.",
            impact="Afeta water_balance_mm_week e weather_stress_flag.",
            recommendation="Verificar acumulador, unidade e reset do sensor.",
            action="mantido_com_alerta",
            affected=weather_daily[precipitation.lt(0) | precipitation.gt(300)] if "precipitation_mm" in weather_daily else pd.DataFrame(),
        )
        if isinstance(ndvi_clean, pd.DataFrame) and not ndvi_clean.empty and "date" in ndvi_clean.columns:
            weather_start = pd.to_datetime(weather_daily["date"], errors="coerce").min()
            ndvi_before_weather = ndvi_clean[pd.to_datetime(ndvi_clean["date"], errors="coerce").lt(weather_start)]
            add_rule(
                rule_id="weather_starts_after_ndvi_window",
                domain="Clima",
                severity="warning",
                dataset="weather_daily",
                quality_dimension="atualidade",
                affected_columns=["date", "has_weather_coverage", "water_balance_mm_week"],
                rule="existem cenas NDVI antes do início do clima local",
                reason="Parte da safra não tem meteorologia local associada.",
                impact="Drivers clima->NDVI ficam parciais nas semanas iniciais.",
                recommendation="Adicionar série meteorológica desde o plantio ou marcar hipóteses climáticas como parciais.",
                action="mantido_com_alerta",
                affected=ndvi_before_weather,
            )

    ops = workspace.get("ops_area_daily")
    if isinstance(ops, pd.DataFrame) and not ops.empty:
        yield_values = _numeric(ops, "harvest_yield_mean_kg_ha")
        add_rule(
            rule_id="ops_yield_negative_or_extreme",
            domain="Operação",
            severity="warning",
            dataset="ops_area_daily",
            quality_dimension="validade_fisica",
            affected_columns=["harvest_yield_mean_kg_ha"],
            rule="harvest_yield_mean_kg_ha < 0 ou > 25000",
            reason="Produtividade negativa é impossível e valores muito altos pedem auditoria.",
            impact="Afeta qualquer tentativa de comparar NDVI com produtividade.",
            recommendation="Conferir camada de colheita, unidade e pontos de manobra/cabeceira.",
            action="mantido_com_alerta",
            affected=ops[yield_values.lt(0) | yield_values.gt(25000)] if "harvest_yield_mean_kg_ha" in ops else pd.DataFrame(),
        )
        add_rule(
            rule_id="ops_yield_zero_or_missing",
            domain="Operação",
            severity="info",
            dataset="ops_area_daily",
            quality_dimension="completude",
            affected_columns=["harvest_yield_mean_kg_ha"],
            rule="harvest_yield_mean_kg_ha == 0 ou nulo",
            reason="Zeros/nulos em produtividade podem representar ausência de colheita, manobra ou dado incompleto.",
            impact="A prova produtiva final fica fraca quando colheita/custo não estão consolidados.",
            recommendation="Separar zero real de dado ausente e carregar colheita final validada.",
            action="mantido_com_alerta",
            affected=ops[yield_values.fillna(0).le(0)] if "harvest_yield_mean_kg_ha" in ops else pd.DataFrame(),
        )
        population = _numeric(ops, "planting_population_mean_ha")
        add_rule(
            rule_id="ops_population_implausible",
            domain="Operação",
            severity="info",
            dataset="ops_area_daily",
            quality_dimension="precisao",
            affected_columns=["planting_population_mean_ha"],
            rule="planting_population_mean_ha <= 0 ou > 200000",
            reason="População por hectare fora dessa faixa pode representar manobra, ausência de operação ou erro de escala.",
            impact="Afeta interpretação de stand e falhas de plantio.",
            recommendation="Checar se zeros são ausência de aplicação/plantio ou falha de telemetria.",
            action="mantido_com_alerta",
            affected=ops[population.le(0) | population.gt(200000)] if "planting_population_mean_ha" in ops else pd.DataFrame(),
        )
        for column in ["spray_pressure_mean_psi", "fert_dose_gap_abs_mean_kg_ha", "overlap_area_pct_bbox", "stop_duration_h_per_bbox_ha"]:
            if column in ops.columns:
                values = _numeric(ops, column)
                add_rule(
                    rule_id=f"ops_{column}_mostly_zero",
                    domain="Operação",
                    severity="info",
                    dataset="ops_area_daily",
                    quality_dimension="completude",
                    affected_columns=[column],
                    rule=f"{column} zerado em mais de 80% das linhas",
                    reason="Muitos zeros podem indicar dado ausente codificado como zero ou camada sem cobertura.",
                    impact="Drivers operacionais derivados podem ficar subestimados.",
                    recommendation="Validar se zero é medição real ou falta de dado na camada EKOS.",
                    action="mantido_com_alerta",
                    affected=ops[values.fillna(0).eq(0)] if values.fillna(0).eq(0).mean() > 0.8 else pd.DataFrame(),
                )

    miip = workspace.get("miip_daily")
    if isinstance(miip, pd.DataFrame) and not miip.empty:
        pest = _numeric(miip, "avg_pest_count")
        add_rule(
            rule_id="miip_pest_negative_or_extreme",
            domain="MIIP",
            severity="warning",
            dataset="miip_daily",
            quality_dimension="validade_fisica",
            affected_columns=["avg_pest_count", "total_pest_count"],
            rule="avg_pest_count < 0 ou > 10000",
            reason="Contagens negativas são impossíveis e extremos pedem revisão.",
            impact="Afeta pest_risk_flag e driver pressao_de_pragas.",
            recommendation="Conferir detecção, agregação e duplicidade de eventos de armadilha.",
            action="mantido_com_alerta",
            affected=miip[pest.lt(0) | pest.gt(10000)] if "avg_pest_count" in miip else pd.DataFrame(),
        )
        add_rule(
            rule_id="miip_missing_spatial_assignment",
            domain="MIIP",
            severity="warning",
            dataset="miip_daily",
            quality_dimension="inconsistencia_espacial",
            affected_columns=["season_id", "area_label"],
            rule="season_id ou area_label nulo",
            reason="Evento de praga sem área não pode ser associado com segurança ao NDVI.",
            impact="Pode remover ou deslocar pressão de pragas entre áreas.",
            recommendation="Validar coordenadas das armadilhas e regra de atribuição espacial.",
            action="mantido_com_alerta",
            affected=_missing_any(miip, ["season_id", "area_label"]),
        )

    for name, frame in {**workspace, **outputs}.items():
        if not isinstance(frame, pd.DataFrame) or frame.empty:
            continue
        empty_columns = [column for column in frame.columns if frame[column].isna().all()]
        if empty_columns:
            affected = pd.DataFrame({"empty_column": empty_columns})
        else:
            affected = pd.DataFrame(columns=["empty_column"])
        add_rule(
            rule_id=f"{name}_empty_columns",
            domain="Estrutura",
            severity="info",
            dataset=str(name),
            quality_dimension="completude",
            affected_columns=empty_columns,
            rule="colunas completamente vazias",
            reason="Colunas vazias podem ser esperadas por fonte ausente, mas precisam ficar visíveis.",
            impact="Indica lacuna de cobertura ou campo não preenchido.",
            recommendation="Remover da narrativa analítica ou preencher com fonte confiável antes de concluir.",
            action="mantido_visivel_com_alerta",
            affected=affected,
        )
        duplicate_columns = list(frame.columns[frame.columns.duplicated()])
        add_rule(
            rule_id=f"{name}_duplicate_columns",
            domain="Estrutura",
            severity="warning",
            dataset=str(name),
            quality_dimension="consistencia",
            affected_columns=duplicate_columns,
            rule="nomes de colunas duplicados",
            reason="Colunas duplicadas dificultam lineage e podem causar seleção incorreta.",
            impact="Pode quebrar interpretação coluna-a-coluna.",
            recommendation="Renomear ou consolidar colunas duplicadas.",
            action="bloqueia_confianca_da_coluna",
            affected=pd.DataFrame({"duplicate_column": duplicate_columns}) if duplicate_columns else pd.DataFrame(columns=["duplicate_column"]),
        )

    summary = pd.DataFrame(rows)
    if not summary.empty:
        severity_order = {"error": 0, "warning": 1, "info": 2}
        summary["_severity_order"] = summary["severity"].map(severity_order).fillna(99)
        summary = summary.sort_values(["_severity_order", "affected_rows"], ascending=[True, False]).drop(columns=["_severity_order"])
    return summary.reset_index(drop=True), examples


def _numeric(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(dtype="float64", index=frame.index)
    return pd.to_numeric(frame[column], errors="coerce")


def _missing_any(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    available = [column for column in columns if column in frame.columns]
    if not available:
        return pd.DataFrame()
    mask = frame[available].isna().any(axis=1)
    return frame[mask]


def _temporal_gaps(frame: pd.DataFrame, group_col: str, date_col: str, *, max_days: int) -> pd.DataFrame:
    if frame.empty or group_col not in frame.columns or date_col not in frame.columns:
        return pd.DataFrame()
    data = frame[[group_col, date_col]].copy()
    data[date_col] = pd.to_datetime(data[date_col], errors="coerce")
    data = data.dropna(subset=[group_col, date_col]).sort_values([group_col, date_col])
    data["previous_date"] = data.groupby(group_col)[date_col].shift(1)
    data["gap_days"] = (data[date_col] - data["previous_date"]).dt.days
    return data[data["gap_days"].gt(max_days)]
