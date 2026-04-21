from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SourceDocumentation:
    key: str
    title: str
    source_group: str
    summary: str
    practical_context: str
    farm_docs_url: str
    documentation_status: str = "documentado_parcialmente"
    relevant_excerpt: str = ""


@dataclass(frozen=True)
class ColumnDocumentation:
    column: str
    definition: str
    practical_interpretation: str
    pipeline_usage: str
    usage_status: str
    source: str = "inferido_do_codigo_e_documentacao"
    documentation_status: str = "documentado_parcialmente"


@dataclass(frozen=True)
class DriverDocumentation:
    driver: str
    flag_feature: str
    title: str
    definition: str
    born_table: str
    source_columns: list[str]
    rule: str
    raw_sources: list[str]
    hypotheses: list[str] = field(default_factory=lambda: ["H3"])
    charts: list[str] = field(default_factory=lambda: ["drivers_problem_weeks"])
    final_csvs: list[str] = field(default_factory=lambda: ["event_driver_lift.csv", "final_hypothesis_register.csv", "decision_summary.csv"])
    interpretation: str = ""
    limitations: list[str] = field(default_factory=list)


FARMLAB_DOC_ROUTES: dict[str, str] = {
    "overview": "https://farm.labs.unimar.br/docs/dados",
    "satelite": "https://farm.labs.unimar.br/docs/dados/satelite",
    "meteorologia": "https://farm.labs.unimar.br/docs/dados/meteorologia",
    "solo": "https://farm.labs.unimar.br/docs/dados/solo",
    "ekos_camadas": "https://farm.labs.unimar.br/docs/dados/ekos_camadas",
    "miip": "https://farm.labs.unimar.br/docs/dados/miip",
    "geotiff": "https://farm.labs.unimar.br/docs/guias/geotiff",
    "series_temporais": "https://farm.labs.unimar.br/docs/guias/series-temporais",
}


SOURCE_DOCUMENTATION: dict[str, SourceDocumentation] = {
    "OneSoil": SourceDocumentation(
        key="OneSoil",
        title="OneSoil - imagens NDVI",
        source_group="OneSoil",
        summary=(
            "Série temporal de NDVI/NDRE derivada de satélite para os talhões monitorados. "
            "No projeto local, a análise usa o CSV ndvi_metadata.csv e imagens JPG de apoio visual."
        ),
        practical_context=(
            "É a fonte central do estudo. O pipeline usa b1 como banda NDVI, filtra cenas sem pixel válido "
            "e transforma estatísticas raster em métricas temporais por área e semana."
        ),
        farm_docs_url=FARMLAB_DOC_ROUTES["satelite"],
        relevant_excerpt=(
            "A documentação pública do FarmLab descreve 196 imagens, 4 talhões, 49 datas, Sentinel-2 "
            "e metadados raster em ndvi_metadata.csv. O bundle também registra a regra de filtrar "
            "b1_valid_pixels > 0 antes da série temporal."
        ),
    ),
    "Metos": SourceDocumentation(
        key="Metos",
        title="Metos Pro - meteorologia",
        source_group="Metos",
        summary="Série meteorológica horária com temperatura, chuva, radiação, umidade, vento e evapotranspiração.",
        practical_context=(
            "É agregada para dia e semana para contextualizar estresse hídrico/climático e possíveis impactos no NDVI."
        ),
        farm_docs_url=FARMLAB_DOC_ROUTES["meteorologia"],
        relevant_excerpt="A documentação pública cita estação Metos Pro, série horária, precipitação, temperatura, radiação, ETP e vento.",
    ),
    "Cropman": SourceDocumentation(
        key="Cropman",
        title="Cropman - análise de solo",
        source_group="Cropman",
        summary="Dados de análise de solo, zonas e indicadores físicos/químicos da área.",
        practical_context="Entra como contexto agronômico. No foco NDVI atual, solo aparece principalmente como limitação/driver interpretativo.",
        farm_docs_url=FARMLAB_DOC_ROUTES["solo"],
        relevant_excerpt="A documentação pública cita Cropman, pontos de solo, zonas e macro/micronutrientes.",
    ),
    "EKOS Layers": SourceDocumentation(
        key="EKOS Layers",
        title="EKOS - camadas operacionais",
        source_group="EKOS Layers",
        summary="Camadas georreferenciadas de plantio, colheita, adubação, sobreposição, velocidade, parada e telemetria.",
        practical_context=(
            "São transformadas em resumos diários/semanais por área para explicar drivers operacionais do NDVI."
        ),
        farm_docs_url=FARMLAB_DOC_ROUTES["ekos_camadas"],
        relevant_excerpt="A documentação pública cita camadas EKOS em CSV/WKT para operações agrícolas.",
    ),
    "EKOS Pest": SourceDocumentation(
        key="EKOS Pest",
        title="MIIP / EKOS Pest - armadilhas e pragas",
        source_group="EKOS Pest",
        summary="Dados de armadilhas, lista de pragas, eventos, alertas e contagens associadas ao MIIP.",
        practical_context="É agregado por área e semana para formar pressao_de_pragas e contexto de manejo fitossanitário.",
        farm_docs_url=FARMLAB_DOC_ROUTES["miip"],
        relevant_excerpt="A documentação pública cita monitoramento MIIP, armadilhas, pragas e detecção por IA.",
    ),
}


COLUMN_DOCUMENTATION: dict[str, ColumnDocumentation] = {
    "filename": ColumnDocumentation(
        column="filename",
        definition="Nome do arquivo da cena raster/imagem de onde o metadado foi extraído.",
        practical_interpretation="No pipeline, a data da cena é extraída do nome do arquivo quando a coluna date não existe no CSV bruto.",
        pipeline_usage="Usada em farmlab.io.load_ndvi_metadata para derivar date e image_path.",
        usage_status="usada",
    ),
    "season_id": ColumnDocumentation(
        column="season_id",
        definition="Identificador do talhão/área/ciclo usado para ligar fontes diferentes.",
        practical_interpretation="É a chave mais importante para conectar NDVI, operação, clima por semana e MIIP.",
        pipeline_usage="Usada em quase todos os joins e agregações por área.",
        usage_status="usada",
    ),
    "date": ColumnDocumentation(
        column="date",
        definition="Data normalizada do evento, cena ou medição.",
        practical_interpretation="Permite ordenar o ciclo, criar week_start e alinhar dados diários/semanais.",
        pipeline_usage="Criada a partir de filename no NDVI e usada em filtros temporais, diffs e agregações.",
        usage_status="usada",
    ),
    "week_start": ColumnDocumentation(
        column="week_start",
        definition="Início da semana usado para alinhar séries temporais heterogêneas.",
        practical_interpretation="É a chave temporal semanal para comparar 4.0 vs convencional.",
        pipeline_usage="Criada via dt.to_period('W').start_time e usada em pairwise_weekly_features.",
        usage_status="usada",
    ),
    "b1_mean": ColumnDocumentation(
        column="b1_mean",
        definition="Média da banda b1, que neste pacote representa o NDVI.",
        practical_interpretation="Quanto maior, maior o vigor verde médio observado na cena, com saturação possível em vegetação muito densa.",
        pipeline_usage="Renomeada para ndvi_mean após filtrar b1_valid_pixels > 0.",
        usage_status="usada",
    ),
    "b1_std": ColumnDocumentation(
        column="b1_std",
        definition="Desvio-padrão do NDVI dentro da cena.",
        practical_interpretation="Mede heterogeneidade espacial do vigor dentro da área naquela data.",
        pipeline_usage="Renomeada para ndvi_std em ndvi_clean.",
        usage_status="usada",
    ),
    "b1_valid_pixels": ColumnDocumentation(
        column="b1_valid_pixels",
        definition="Quantidade de pixels válidos na banda b1/NDVI.",
        practical_interpretation="Se for 0, a imagem não tem NDVI útil para análise, geralmente por nuvem, nodata ou ausência de cobertura.",
        pipeline_usage="Filtro crítico: build_ndvi_clean mantém apenas b1_valid_pixels > 0.",
        usage_status="usada",
    ),
    "b1_pct_solo": ColumnDocumentation(
        column="b1_pct_solo",
        definition="Percentual da cena classificado como solo exposto na banda NDVI.",
        practical_interpretation="Aumentos indicam menor cobertura vegetal aparente, falhas, colheita, preparo, estresse ou problemas de cobertura.",
        pipeline_usage="Renomeada para soil_pct e depois agregada para soil_pct_week.",
        usage_status="usada",
    ),
    "b1_pct_veg_densa": ColumnDocumentation(
        column="b1_pct_veg_densa",
        definition="Percentual da cena classificado como vegetação densa.",
        practical_interpretation="Indica maior cobertura verde densa e costuma acompanhar fases de maior vigor.",
        pipeline_usage="Renomeada para dense_veg_pct e agregada semanalmente.",
        usage_status="usada",
    ),
    "b2_valid_pixels": ColumnDocumentation(
        column="b2_valid_pixels",
        definition="Pixels válidos da banda b2, descrita na documentação FarmLab como máscara de qualidade/confiança.",
        practical_interpretation="Útil para auditoria da qualidade do raster, mas não é a banda de NDVI usada na métrica principal.",
        pipeline_usage="Não participa da lógica atual; o filtro analítico oficial usa b1_valid_pixels porque b1 é o NDVI.",
        usage_status="ignorada_no_modelo_atual",
    ),
    "b3_valid_pixels": ColumnDocumentation(
        column="b3_valid_pixels",
        definition="Pixels válidos da banda b3, descrita como máscara/limite estático do talhão.",
        practical_interpretation="Ajuda a entender cobertura espacial da área, mas não representa vigor vegetal.",
        pipeline_usage="Não participa da lógica atual; a análise usa estatísticas de b1/NDVI.",
        usage_status="ignorada_no_modelo_atual",
    ),
    "Productivity (kg/ha)": ColumnDocumentation(
        column="Productivity (kg/ha)",
        definition="Produtividade estimada/registrada em kg por hectare na camada de colheita.",
        practical_interpretation="Valores zero podem representar manobras/cabeceira, conforme a documentação pública do FarmLab.",
        pipeline_usage="Agregada em ops_area_daily quando há camada de colheita.",
        usage_status="usada_quando_disponivel",
    ),
    "InvalidCommunication": ColumnDocumentation(
        column="InvalidCommunication",
        definition="Indicador bruto de comunicação inválida na telemetria.",
        practical_interpretation="Sinaliza instabilidade na transmissão de dados da máquina.",
        pipeline_usage="Resumo diário vira invalid_telemetry_share e depois telemetry_risk_flag.",
        usage_status="usada",
    ),
}


COLUMN_DOCUMENTATION.update(
    {
        "driver": ColumnDocumentation(
            column="driver",
            definition="Driver GDAL/raster usado na leitura do arquivo original exportado pela fonte.",
            practical_interpretation="Serve para auditoria técnica da origem do raster; não mede vigor diretamente.",
            pipeline_usage="Mantida no bruto OneSoil para inspeção; não entra nas métricas NDVI.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "dtype": ColumnDocumentation(
            column="dtype",
            definition="Tipo de dado numérico armazenado no raster ou arquivo exportado.",
            practical_interpretation="Ajuda a verificar se a escala dos valores está coerente.",
            pipeline_usage="Coluna técnica de auditoria; não entra nas features.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "nodata": ColumnDocumentation(
            column="nodata",
            definition="Valor reservado para pixel sem dado válido no raster.",
            practical_interpretation="Ajuda a explicar por que alguns pixels são descartados.",
            pipeline_usage="A qualidade analítica é controlada por b1_valid_pixels > 0.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "width": ColumnDocumentation(
            column="width",
            definition="Largura do raster em pixels.",
            practical_interpretation="Indica resolução matricial da cena recortada para o talhão.",
            pipeline_usage="Usada somente como metadado espacial.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "height": ColumnDocumentation(
            column="height",
            definition="Altura do raster em pixels.",
            practical_interpretation="Indica resolução matricial da cena recortada para o talhão.",
            pipeline_usage="Usada somente como metadado espacial.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "count": ColumnDocumentation(
            column="count",
            definition="Quantidade de bandas presentes no raster.",
            practical_interpretation="No NDVI local, b1 é a banda analítica principal; b2/b3 são mantidas para auditoria.",
            pipeline_usage="Usada somente como metadado técnico.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "crs": ColumnDocumentation(
            column="crs",
            definition="Sistema de referência de coordenadas do raster.",
            practical_interpretation="Necessário para entender localização espacial e coerência de geometrias.",
            pipeline_usage="Usado como metadado espacial, não como feature.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "bounds_left": ColumnDocumentation(
            column="bounds_left",
            definition="Coordenada mínima X do retângulo envolvente da imagem/talhão.",
            practical_interpretation="Ajuda a montar bbox e associar geometrias operacionais às áreas.",
            pipeline_usage="Usada indiretamente em build_season_geometries para bbox/centroide.",
            usage_status="usada",
        ),
        "bounds_bottom": ColumnDocumentation(
            column="bounds_bottom",
            definition="Coordenada mínima Y do retângulo envolvente da imagem/talhão.",
            practical_interpretation="Ajuda a montar bbox e associar geometrias operacionais às áreas.",
            pipeline_usage="Usada indiretamente em build_season_geometries para bbox/centroide.",
            usage_status="usada",
        ),
        "bounds_right": ColumnDocumentation(
            column="bounds_right",
            definition="Coordenada máxima X do retângulo envolvente da imagem/talhão.",
            practical_interpretation="Ajuda a montar bbox e associar geometrias operacionais às áreas.",
            pipeline_usage="Usada indiretamente em build_season_geometries para bbox/centroide.",
            usage_status="usada",
        ),
        "bounds_top": ColumnDocumentation(
            column="bounds_top",
            definition="Coordenada máxima Y do retângulo envolvente da imagem/talhão.",
            practical_interpretation="Ajuda a montar bbox e associar geometrias operacionais às áreas.",
            pipeline_usage="Usada indiretamente em build_season_geometries para bbox/centroide.",
            usage_status="usada",
        ),
        "res_x": ColumnDocumentation(
            column="res_x",
            definition="Tamanho do pixel no eixo X, na unidade do CRS.",
            practical_interpretation="Permite auditar resolução espacial da cena.",
            pipeline_usage="Metadado técnico; não entra diretamente nas métricas.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "res_y": ColumnDocumentation(
            column="res_y",
            definition="Tamanho do pixel no eixo Y, na unidade do CRS.",
            practical_interpretation="Permite auditar resolução espacial da cena.",
            pipeline_usage="Metadado técnico; não entra diretamente nas métricas.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "Data": ColumnDocumentation(
            column="Data",
            definition="Data/hora da observação meteorológica horária.",
            practical_interpretation="É normalizada para data diária e week_start para alinhar clima com NDVI.",
            pipeline_usage="Usada em build_weather_daily e build_weather_weekly.",
            usage_status="usada",
        ),
        "Estação": ColumnDocumentation(
            column="Estação",
            definition="Identificador/nome da estação meteorológica.",
            practical_interpretation="Ajuda a auditar a fonte do clima usado para todo o estudo.",
            pipeline_usage="Contexto; a análise atual agrega a série temporal da estação disponível.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "Latitude": ColumnDocumentation(
            column="Latitude",
            definition="Latitude da estação, armadilha ou ponto georreferenciado.",
            practical_interpretation="Usada para validar localização e, em MIIP, associar armadilhas a áreas.",
            pipeline_usage="Usada quando disponível para atribuição espacial; caso contrário fica como contexto.",
            usage_status="usada_quando_disponivel",
        ),
        "Longitude": ColumnDocumentation(
            column="Longitude",
            definition="Longitude da estação, armadilha ou ponto georreferenciado.",
            practical_interpretation="Usada para validar localização e, em MIIP, associar armadilhas a áreas.",
            pipeline_usage="Usada quando disponível para atribuição espacial; caso contrário fica como contexto.",
            usage_status="usada_quando_disponivel",
        ),
        "Radiação Solar (W/m2)": ColumnDocumentation(
            column="Radiação Solar (W/m2)",
            definition="Radiação solar média horária em watts por metro quadrado.",
            practical_interpretation="Contextualiza crescimento, evapotranspiração e estresse climático.",
            pipeline_usage="Agregada para solar_radiation_w_m2_week.",
            usage_status="usada",
        ),
        "Precipitação (mm)": ColumnDocumentation(
            column="Precipitação (mm)",
            definition="Chuva acumulada no período da medição, em milímetros.",
            practical_interpretation="Chuva reduz déficit hídrico, mas excesso pode dificultar operação e manejo.",
            pipeline_usage="Agregada para precipitation_mm_week e entra em water_balance_mm_week.",
            usage_status="usada",
        ),
        "Vel do Vento Média (km/h)": ColumnDocumentation(
            column="Vel do Vento Média (km/h)",
            definition="Velocidade média do vento em km/h.",
            practical_interpretation="Ajuda a interpretar risco operacional e plausibilidade de aplicação/pulverização.",
            pipeline_usage="Agregada para wind_avg_kmh_week e checagens de plausibilidade.",
            usage_status="usada",
        ),
        "Temp. Mínima (°C)": ColumnDocumentation(
            column="Temp. Mínima (°C)",
            definition="Temperatura mínima observada no período.",
            practical_interpretation="Extremos frios podem afetar desenvolvimento vegetativo.",
            pipeline_usage="Agregada para temp_min_c_week.",
            usage_status="usada",
        ),
        "Temp. Média (°C)": ColumnDocumentation(
            column="Temp. Média (°C)",
            definition="Temperatura média observada no período.",
            practical_interpretation="Contextualiza desenvolvimento da cultura e estresse climático.",
            pipeline_usage="Agregada para temp_avg_c_week.",
            usage_status="usada",
        ),
        "Temp. Máxima (°C)": ColumnDocumentation(
            column="Temp. Máxima (°C)",
            definition="Temperatura máxima observada no período.",
            practical_interpretation="Extremos altos podem elevar demanda hídrica e estresse.",
            pipeline_usage="Agregada para temp_max_c_week.",
            usage_status="usada",
        ),
        "Umidade Rel. Média (%)": ColumnDocumentation(
            column="Umidade Rel. Média (%)",
            definition="Umidade relativa média do ar.",
            practical_interpretation="Contextualiza demanda evaporativa e condições de manejo.",
            pipeline_usage="Agregada para humidity_avg_pct_week.",
            usage_status="usada",
        ),
        "Rajada de Vento (km/h)": ColumnDocumentation(
            column="Rajada de Vento (km/h)",
            definition="Maior rajada de vento observada no período.",
            practical_interpretation="Pode indicar risco operacional e interferência em pulverização.",
            pipeline_usage="Checada como plausibilidade/contexto meteorológico; não é driver principal atual.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "Evapotranspiração (mm)": ColumnDocumentation(
            column="Evapotranspiração (mm)",
            definition="Evapotranspiração estimada em milímetros.",
            practical_interpretation="Quanto maior, maior a perda potencial de água do sistema solo-planta.",
            pipeline_usage="Usada com chuva para calcular water_balance_mm_week e weather_stress_flag.",
            usage_status="usada",
        ),
        "geometry": ColumnDocumentation(
            column="geometry",
            definition="Geometria WKT da linha/polígono/ponto operacional exportado pelo EKOS.",
            practical_interpretation="Permite associar operação à área/talhão por interseção, bbox ou centroide.",
            pipeline_usage="Usada para atribuir registros EKOS a season_id em build_ops_area_daily/build_ops_support_daily.",
            usage_status="usada",
        ),
        "Timestamp": ColumnDocumentation(
            column="Timestamp",
            definition="Timestamp bruto do evento operacional.",
            practical_interpretation="Auxilia auditoria temporal do registro original.",
            pipeline_usage="Contexto; Date Time é a coluna normalmente normalizada para date.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "Date Time": ColumnDocumentation(
            column="Date Time",
            definition="Data/hora do evento operacional EKOS.",
            practical_interpretation="Base temporal para agregar eventos operacionais por dia e semana.",
            pipeline_usage="Usada para criar date e week_start nas bases operacionais.",
            usage_status="usada",
        ),
        "Machine Name": ColumnDocumentation(
            column="Machine Name",
            definition="Nome/identificador da máquina agrícola.",
            practical_interpretation="Permite verificar qual máquina gerou o registro.",
            pipeline_usage="Contexto de auditoria; não é agregada como feature principal.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "MachineName": ColumnDocumentation(
            column="MachineName",
            definition="Nome/identificador da máquina agrícola em arquivos que usam grafia sem espaço.",
            practical_interpretation="Permite verificar qual máquina gerou o registro.",
            pipeline_usage="Contexto de auditoria; não é agregada como feature principal.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "Operation": ColumnDocumentation(
            column="Operation",
            definition="Tipo de operação agrícola registrada.",
            practical_interpretation="Ajuda a conferir se o evento corresponde a plantio, colheita, pulverização, parada etc.",
            pipeline_usage="Usada como contexto e em alguns filtros/agrupamentos operacionais.",
            usage_status="usada_quando_disponivel",
        ),
        "operation": ColumnDocumentation(
            column="operation",
            definition="Tipo de operação agrícola registrada em arquivos que usam grafia minúscula.",
            practical_interpretation="Ajuda a conferir se o evento corresponde à camada operacional correta.",
            pipeline_usage="Contexto/filtro de auditoria da camada.",
            usage_status="usada_quando_disponivel",
        ),
        "Alarm": ColumnDocumentation(
            column="Alarm",
            definition="Alarme operacional registrado pela máquina/sistema.",
            practical_interpretation="Sinaliza condição operacional que pode afetar execução ou disponibilidade.",
            pipeline_usage="Agregado para alarm_events_week e alert_risk_flag.",
            usage_status="usada",
        ),
        "Alert": ColumnDocumentation(
            column="Alert",
            definition="Alerta parametrizado registrado pela camada operacional.",
            practical_interpretation="Sinaliza condição operacional monitorada por regra do sistema.",
            pipeline_usage="Agregado para param_alert_events_week e alert_risk_flag.",
            usage_status="usada",
        ),
        "Duration": ColumnDocumentation(
            column="Duration",
            definition="Duração do evento ou alerta na camada operacional.",
            practical_interpretation="Ajuda a quantificar persistência de anomalias/alertas.",
            pipeline_usage="Contexto operacional; Duration - h é usado diretamente no driver tempo_parado.",
            usage_status="usada_quando_disponivel",
        ),
        "Duration - h": ColumnDocumentation(
            column="Duration - h",
            definition="Duração da parada em horas.",
            practical_interpretation="Quanto maior por hectare, maior o sinal de interrupção operacional.",
            pipeline_usage="Agregada para stop_duration_h_per_bbox_ha_week e stop_risk_flag.",
            usage_status="usada",
        ),
        "Reason for stopping": ColumnDocumentation(
            column="Reason for stopping",
            definition="Motivo textual da parada/alerta operacional.",
            practical_interpretation="Ajuda a interpretar por que houve interrupção ou alerta.",
            pipeline_usage="Contexto explicativo; o driver usa duração/ocorrência agregada.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "event": ColumnDocumentation(
            column="event",
            definition="Descrição/tipo do evento de parada.",
            practical_interpretation="Ajuda a qualificar o motivo do tempo parado.",
            pipeline_usage="Contexto; o driver usa Duration - h agregado.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "Area - ha": ColumnDocumentation(
            column="Area - ha",
            definition="Área operacional registrada em hectares.",
            practical_interpretation="Permite normalizar produção, aplicação e cobertura operacional.",
            pipeline_usage="Usada em resumos operacionais quando disponível.",
            usage_status="usada_quando_disponivel",
        ),
        "AppliedDos - kg/ha": ColumnDocumentation(
            column="AppliedDos - kg/ha",
            definition="Dose aplicada de fertilizante/insumo em kg/ha.",
            practical_interpretation="Comparada com dose configurada para encontrar desvios de execução.",
            pipeline_usage="Usada em fert_dose_gap_abs_mean_kg_ha_week e fert_risk_flag.",
            usage_status="usada",
        ),
        "Configured - kg/ha": ColumnDocumentation(
            column="Configured - kg/ha",
            definition="Dose configurada/prescrita em kg/ha.",
            practical_interpretation="Base de comparação contra a dose realmente aplicada.",
            pipeline_usage="Usada em fert_dose_gap_abs_mean_kg_ha_week e fert_risk_flag.",
            usage_status="usada",
        ),
        "Weight - kg": ColumnDocumentation(
            column="Weight - kg",
            definition="Peso registrado na operação, em kg.",
            practical_interpretation="Contexto de aplicação ou colheita conforme a camada.",
            pipeline_usage="Contexto operacional; produtividade/dose são as features principais.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "Yield - kg/ha": ColumnDocumentation(
            column="Yield - kg/ha",
            definition="Produtividade registrada na colheita em kg/ha.",
            practical_interpretation="Contextualiza resultado produtivo quando a camada de colheita cobre a área.",
            pipeline_usage="Agregada para harvest_yield_mean_kg_ha.",
            usage_status="usada",
        ),
        "Humidity - %": ColumnDocumentation(
            column="Humidity - %",
            definition="Umidade do produto colhido em percentual.",
            practical_interpretation="Ajuda a qualificar condição de colheita.",
            pipeline_usage="Contexto; não é feature central do NDVI atual.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "OverlapArea - ha": ColumnDocumentation(
            column="OverlapArea - ha",
            definition="Área sobreposta na operação, em hectares.",
            practical_interpretation="Sinaliza possível reaplicação, tráfego duplicado ou ineficiência operacional.",
            pipeline_usage="Normalizada para overlap_area_pct_bbox_week e overlap_risk_flag.",
            usage_status="usada",
        ),
        "Population - ha": ColumnDocumentation(
            column="Population - ha",
            definition="População/stand registrado no plantio por hectare.",
            practical_interpretation="Ajuda a avaliar densidade/stand e possíveis falhas de plantio.",
            pipeline_usage="Agregada para planting_population_mean_ha.",
            usage_status="usada",
        ),
        "Speed - km/h": ColumnDocumentation(
            column="Speed - km/h",
            definition="Velocidade operacional da máquina.",
            practical_interpretation="Velocidade muito baixa/alta pode indicar execução irregular.",
            pipeline_usage="Contexto de qualidade operacional; não é driver principal atual.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "Pressure - psi": ColumnDocumentation(
            column="Pressure - psi",
            definition="Pressão de pulverização em psi.",
            practical_interpretation="Ajuda a auditar qualidade potencial de aplicação.",
            pipeline_usage="Contexto; não foi transformada em driver final nesta versão.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "MachineState": ColumnDocumentation(
            column="MachineState",
            definition="Estado operacional da máquina.",
            practical_interpretation="Pode indicar trabalho, ociosidade ou condição de máquina.",
            pipeline_usage="Contexto operacional; flags atuais usam camadas específicas de suporte.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "EngineRotation - rpm": ColumnDocumentation(
            column="EngineRotation - rpm",
            definition="Rotação do motor em rotações por minuto.",
            practical_interpretation="Rotação baixa/ociosa pode indicar operação parada ou inconsistente.",
            pipeline_usage="Agregada para engine_idle_share_week e engine_risk_flag.",
            usage_status="usada",
        ),
        "EngineTemperature - ºC": ColumnDocumentation(
            column="EngineTemperature - ºC",
            definition="Temperatura do motor em graus Celsius.",
            practical_interpretation="Temperatura elevada pode indicar anomalia operacional.",
            pipeline_usage="Agregada para engine_temp_max_c_week e engine_risk_flag.",
            usage_status="usada",
        ),
        "FuelConsumption - L/h": ColumnDocumentation(
            column="FuelConsumption - L/h",
            definition="Consumo de combustível em litros por hora.",
            practical_interpretation="Consumo zero ou irregular ajuda a identificar telemetria/operação anômala.",
            pipeline_usage="Agregada para fuel_zero_share_week e engine_risk_flag.",
            usage_status="usada",
        ),
        "TelemetryCommunication": ColumnDocumentation(
            column="TelemetryCommunication",
            definition="Indicador bruto de comunicação de telemetria.",
            practical_interpretation="Ajuda a auditar cobertura de comunicação da máquina.",
            pipeline_usage="Contexto junto de InvalidCommunication.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "ExternalGPS": ColumnDocumentation(
            column="ExternalGPS",
            definition="Indicador de GPS externo na telemetria.",
            practical_interpretation="Ajuda a interpretar qualidade espacial da operação.",
            pipeline_usage="Contexto de auditoria; não entra em driver final.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "trapCode": ColumnDocumentation(
            column="trapCode",
            definition="Código da armadilha eletrônica.",
            practical_interpretation="Chave para conectar leituras/eventos de MIIP.",
            pipeline_usage="Usada em joins de MIIP e rastreio de pragas.",
            usage_status="usada",
        ),
        "trapId": ColumnDocumentation(
            column="trapId",
            definition="Identificador interno da armadilha.",
            practical_interpretation="Ajuda a rastrear registros de MIIP por armadilha.",
            pipeline_usage="Usada quando disponível para joins/eventos de armadilhas.",
            usage_status="usada_quando_disponivel",
        ),
        "pestCount": ColumnDocumentation(
            column="pestCount",
            definition="Contagem de pragas detectadas/registradas.",
            practical_interpretation="Sinaliza pressão de pragas quando elevada ou associada a alerta/dano.",
            pipeline_usage="Agregada para avg_pest_count_week e pest_risk_flag.",
            usage_status="usada",
        ),
        "createdAt": ColumnDocumentation(
            column="createdAt",
            definition="Data/hora de criação do registro MIIP.",
            practical_interpretation="Base temporal para agregar dados de pragas por dia/semana.",
            pipeline_usage="Usada para criar date em miip_daily quando aplicável.",
            usage_status="usada",
        ),
        "photoTime": ColumnDocumentation(
            column="photoTime",
            definition="Horário/frequência de foto associado à praga ou armadilha.",
            practical_interpretation="Contexto da captura/detecção por imagem.",
            pipeline_usage="Contexto de MIIP; não entra em driver final diretamente.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "primaryPest": ColumnDocumentation(
            column="primaryPest",
            definition="Praga principal monitorada pela armadilha.",
            practical_interpretation="Ajuda a interpretar qual praga está associada à pressão observada.",
            pipeline_usage="Contexto; contagem e eventos alimentam pest_risk_flag.",
            usage_status="contexto_ou_nao_usada_diretamente",
        ),
        "alert": ColumnDocumentation(
            column="alert",
            definition="Indicador de alerta associado a praga/evento MIIP.",
            practical_interpretation="Quando verdadeiro/positivo, sugere condição que exige atenção fitossanitária.",
            pipeline_usage="Agregado para alert_hits_week e pest_risk_flag.",
            usage_status="usada",
        ),
        "control": ColumnDocumentation(
            column="control",
            definition="Indicador/recomendação de controle associado à praga.",
            practical_interpretation="Ajuda a qualificar necessidade de intervenção.",
            pipeline_usage="Agregado para control_hits em MIIP.",
            usage_status="usada_quando_disponivel",
        ),
        "damage": ColumnDocumentation(
            column="damage",
            definition="Indicador de dano associado à praga.",
            practical_interpretation="Sinaliza maior severidade potencial da pressão de pragas.",
            pipeline_usage="Agregado para damage_hits_week e pest_risk_flag.",
            usage_status="usada",
        ),
    }
)


DRIVER_DOCUMENTATION: dict[str, DriverDocumentation] = {
    "solo_exposto": DriverDocumentation(
        driver="solo_exposto",
        flag_feature="high_soil_flag",
        title="Solo exposto elevado",
        definition="Semana em que a proporção de solo exposto ficou alta em relação ao histórico semanal do pacote.",
        born_table="ndvi_phase_timeline",
        source_columns=["soil_pct_week"],
        rule="high_soil_flag = soil_pct_week >= quantil 75% observado; fallback técnico: 20%.",
        raw_sources=["ndvi_metadata.csv / b1_pct_solo"],
        interpretation="Pode indicar falhas de stand, baixa cobertura, preparo/colheita, senescência, estresse ou problema de classificação.",
        limitations=["É associação visual/temporal, não prova causa.", "Solo exposto também aumenta naturalmente no início/fim do ciclo."],
    ),
    "risco_de_motor": DriverDocumentation(
        driver="risco_de_motor",
        flag_feature="engine_risk_flag",
        title="Risco de motor",
        definition="Semana com sinais operacionais de motor fora do padrão usado no pacote.",
        born_table="ndvi_phase_timeline",
        source_columns=["engine_temp_max_c_week", "engine_idle_share_week", "fuel_zero_share_week"],
        rule="engine_risk_flag = temperatura alta ou marcha lenta alta ou fuel_zero_share_week >= 0.4.",
        raw_sources=["EKOS Layers / engine_temperature, engine_rotation, fuel_consumption"],
        interpretation="Pode sinalizar operação irregular, máquina parada/ociosa ou telemetria de consumo incompatível.",
        limitations=["Não prova impacto direto no NDVI.", "Depende da cobertura e qualidade da telemetria."],
    ),
    "alertas_de_maquina": DriverDocumentation(
        driver="alertas_de_maquina",
        flag_feature="alert_risk_flag",
        title="Alertas de máquina",
        definition="Semana com alarmes ou alertas parametrizados detectados.",
        born_table="ndvi_phase_timeline",
        source_columns=["alarm_events_week", "param_alert_events_week"],
        rule="alert_risk_flag = alarm_events_week > 0 ou param_alert_events_week > 0.",
        raw_sources=["EKOS Layers / alarm_layer, parameterized_alert_layer"],
        interpretation="Indica evento operacional que pode afetar execução de manejo, aplicação ou disponibilidade de máquina.",
        limitations=["Nem todo alerta tem impacto agronômico.", "Sem severidade detalhada, a interpretação é conservadora."],
    ),
    "falha_de_telemetria": DriverDocumentation(
        driver="falha_de_telemetria",
        flag_feature="telemetry_risk_flag",
        title="Falha de telemetria",
        definition="Semana com proporção elevada de comunicação inválida.",
        born_table="ndvi_phase_timeline",
        source_columns=["invalid_telemetry_share_week"],
        rule="telemetry_risk_flag = invalid_telemetry_share_week >= quantil 75%; fallback técnico: 0.05.",
        raw_sources=["EKOS Layers / telemetry_communication_layer"],
        interpretation="Reduz confiança em decisões automáticas e dificulta rastrear se a operação realmente ocorreu como planejada.",
        limitations=["Pode ser problema de captura, não necessariamente problema agronômico."],
    ),
    "pressao_de_pragas": DriverDocumentation(
        driver="pressao_de_pragas",
        flag_feature="pest_risk_flag",
        title="Pressão de pragas",
        definition="Semana com contagem média de pragas elevada ou eventos de alerta/dano.",
        born_table="ndvi_phase_timeline",
        source_columns=["avg_pest_count_week", "alert_hits_week", "damage_hits_week"],
        rule="pest_risk_flag = avg_pest_count_week >= quantil 75% ou alert_hits_week > 0 ou damage_hits_week > 0.",
        raw_sources=["EKOS Pest / traps_data, traps_events, pest_list, pest_details"],
        interpretation="Pode explicar queda ou baixo vigor quando coincide com semanas problema de NDVI.",
        limitations=["Armadilhas medem pontos específicos; há incerteza espacial até o talhão."],
    ),
    "sobreposicao_operacional": DriverDocumentation(
        driver="sobreposicao_operacional",
        flag_feature="overlap_risk_flag",
        title="Sobreposição operacional",
        definition="Semana com sobreposição de operação acima do limiar interno.",
        born_table="ndvi_phase_timeline",
        source_columns=["overlap_area_pct_bbox_week"],
        rule="overlap_risk_flag = overlap_area_pct_bbox_week >= quantil 75%; fallback técnico: 0.04.",
        raw_sources=["EKOS Layers / overlap_layer"],
        interpretation="Pode indicar aplicação repetida, pisoteio/tráfego ou execução operacional irregular.",
        limitations=["Depende da qualidade da geometria e da normalização por área."],
    ),
    "falha_de_dose_na_adubacao": DriverDocumentation(
        driver="falha_de_dose_na_adubacao",
        flag_feature="fert_risk_flag",
        title="Falha/desvio de dose na adubação",
        definition="Semana com diferença absoluta de dose de adubação acima do limiar interno.",
        born_table="ndvi_phase_timeline",
        source_columns=["fert_dose_gap_abs_mean_kg_ha_week"],
        rule="fert_risk_flag = fert_dose_gap_abs_mean_kg_ha_week >= quantil 75%; fallback técnico: 150 kg/ha.",
        raw_sources=["EKOS Layers / fertilization_layer"],
        interpretation="Pode afetar vigor se houver subdosagem, sobredosagem, atraso ou execução irregular.",
        limitations=["Não separa causa operacional, prescrição incorreta e contexto do solo."],
    ),
    "estresse_climatico": DriverDocumentation(
        driver="estresse_climatico",
        flag_feature="weather_stress_flag",
        title="Estresse climático/hídrico",
        definition="Semana com balanço hídrico baixo e cobertura meteorológica disponível.",
        born_table="ndvi_phase_timeline",
        source_columns=["water_balance_mm_week", "has_weather_coverage_week"],
        rule="weather_stress_flag = has_weather_coverage_week e water_balance_mm_week <= quantil 25%; fallback técnico: -10 mm.",
        raw_sources=["Metos Pro / weather_hourly"],
        interpretation="Ajuda a diferenciar queda de vigor por manejo de queda associada a déficit hídrico/clima.",
        limitations=["Estação única pode não capturar microclima de cada talhão."],
    ),
    "tempo_parado": DriverDocumentation(
        driver="tempo_parado",
        flag_feature="stop_risk_flag",
        title="Tempo parado operacional",
        definition="Semana com tempo parado por hectare acima do limiar interno.",
        born_table="ndvi_phase_timeline",
        source_columns=["stop_duration_h_per_bbox_ha_week"],
        rule="stop_risk_flag = stop_duration_h_per_bbox_ha_week >= quantil 75%; fallback técnico: 0.02 h/ha.",
        raw_sources=["EKOS Layers / stop_reason_layer"],
        interpretation="Pode indicar atraso, interrupção ou problema de execução no manejo.",
        limitations=["Nem toda parada afeta diretamente o desenvolvimento da cultura."],
    ),
}


def source_documentation_for_group(source_group: str) -> SourceDocumentation | None:
    return SOURCE_DOCUMENTATION.get(source_group)


def column_documentation_for(column: str) -> ColumnDocumentation:
    if column in COLUMN_DOCUMENTATION:
        return COLUMN_DOCUMENTATION[column]
    lower = column.lower()
    upper_base = column.upper().removesuffix("_2")
    soil_nutrients = {
        "AMOSTRA": "Identificador da amostra de solo.",
        "ARGILA": "Teor de argila do solo.",
        "SILTE": "Teor de silte do solo.",
        "AREIA": "Teor de areia do solo.",
        "MO": "Matéria orgânica do solo.",
        "CTC": "Capacidade de troca catiônica.",
        "CTCE": "CTC efetiva.",
        "CTCA": "CTC a pH corrigido/potencial.",
        "PHCACL2": "pH medido em CaCl2.",
        "CA": "Cálcio no solo.",
        "MG": "Magnésio no solo.",
        "K": "Potássio no solo.",
        "P": "Fósforo no solo.",
        "S": "Enxofre no solo.",
        "B": "Boro no solo.",
        "ZN": "Zinco no solo.",
        "MN": "Manganês no solo.",
        "CU": "Cobre no solo.",
        "FE": "Ferro no solo.",
        "AL": "Alumínio no solo.",
        "HAL": "Acidez potencial H+Al.",
        "SB": "Soma de bases.",
        "SATCA": "Saturação por cálcio.",
        "SATMG": "Saturação por magnésio.",
        "SATK": "Saturação por potássio.",
        "SATB": "Saturação por bases.",
        "SATAL": "Saturação por alumínio.",
    }
    if upper_base in soil_nutrients:
        suffix = " na camada/profundidade secundária" if column.upper().endswith("_2") else ""
        return ColumnDocumentation(
            column=column,
            definition=f"{soil_nutrients[upper_base]}{suffix}",
            practical_interpretation="Contextualiza fertilidade, textura ou limitação química/física do solo.",
            pipeline_usage="No foco NDVI atual, entra como documentação/contexto agronômico; não foi usado como feature quantitativa final.",
            usage_status="contexto_ou_nao_usada_diretamente",
            documentation_status="documentado_por_regra",
        )
    if lower in {"umidade rel. mín. (%)", "umidade rel. máx. (%)"}:
        return ColumnDocumentation(
            column=column,
            definition="Extremo diário/horário de umidade relativa do ar.",
            practical_interpretation="Ajuda a validar plausibilidade climática e condições de evaporação/aplicação.",
            pipeline_usage="Contexto meteorológico; a feature semanal principal usa Umidade Rel. Média (%).",
            usage_status="contexto_ou_nao_usada_diretamente",
            documentation_status="documentado_por_regra",
        )
    if lower in {"service order", "operator number", "operator name", "team", "valor"}:
        return ColumnDocumentation(
            column=column,
            definition="Campo administrativo/operacional do registro EKOS.",
            practical_interpretation="Ajuda auditoria de execução, equipe, ordem de serviço ou parâmetro registrado.",
            pipeline_usage="Mantido para inspeção de linha; não é feature principal do NDVI.",
            usage_status="contexto_ou_nao_usada_diretamente",
            documentation_status="documentado_por_regra",
        )
    if "latitude" == lower or "longitude" == lower:
        axis = "latitude" if "latitude" == lower else "longitude"
        return ColumnDocumentation(
            column=column,
            definition=f"Coordenada geográfica de {axis}.",
            practical_interpretation="Usada para validar posição e associar armadilhas/pontos a talhões quando possível.",
            pipeline_usage="Usada em atribuição espacial quando a fonte contém coordenadas úteis.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["miip_pest", "pest", "trap", "pheromone", "adhesive", "infestation"]):
        return ColumnDocumentation(
            column=column,
            definition="Campo bruto do MIIP/EKOS Pest relacionado a praga, armadilha, captura, foto ou classificação.",
            practical_interpretation="Ajuda a explicar pressão de pragas e rastrear eventos fitossanitários.",
            pipeline_usage="Contagens, alertas e danos alimentam miip_daily, avg_pest_count_week e pest_risk_flag; demais campos ficam como contexto.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["installationdate", "photoprogrammedat", "fridayphoto", "mondayphoto", "saturdayphoto", "sundayphoto", "thursdayphoto", "tuesdayphoto", "wednesdayphoto", "seconddphototime", "secondphototime"]):
        return ColumnDocumentation(
            column=column,
            definition="Campo temporal/programação de captura da armadilha MIIP.",
            practical_interpretation="Contextualiza rotina de fotos e manutenção da armadilha.",
            pipeline_usage="Contexto de auditoria; os eventos/contagens agregados são usados no driver de pragas.",
            usage_status="contexto_ou_nao_usada_diretamente",
            documentation_status="documentado_por_regra",
        )
    if lower.startswith("b1_"):
        return ColumnDocumentation(
            column=column,
            definition="Coluna estatística da banda b1, usada como banda NDVI neste pacote.",
            practical_interpretation="Pode ser usada para descrever vigor, cobertura ou distribuição do NDVI, dependendo do sufixo.",
            pipeline_usage="Apenas subconjunto de b1 é usado diretamente: b1_valid_pixels, b1_mean, b1_std, b1_pct_solo e b1_pct_veg_densa.",
            usage_status="parcialmente_usada",
        )
    if lower.startswith("b2_"):
        return ColumnDocumentation(
            column=column,
            definition="Coluna estatística da banda b2, associada à máscara de qualidade/confiança na documentação pública.",
            practical_interpretation="Serve para auditoria do raster, mas não representa a métrica de vigor usada no projeto.",
            pipeline_usage="Não usada na lógica analítica principal; documentada para transparência.",
            usage_status="ignorada_no_modelo_atual",
        )
    if lower.startswith("b3_"):
        return ColumnDocumentation(
            column=column,
            definition="Coluna estatística da banda b3, associada à máscara/limite do talhão na documentação pública.",
            practical_interpretation="Serve para contexto espacial, não para vigor vegetativo.",
            pipeline_usage="Não usada na lógica analítica principal; documentada para transparência.",
            usage_status="ignorada_no_modelo_atual",
        )
    if "precip" in lower or "rain" in lower or "chuva" in lower:
        return ColumnDocumentation(
            column=column,
            definition="Variável de precipitação/chuva.",
            practical_interpretation="Ajuda a interpretar estresse ou alívio hídrico no NDVI.",
            pipeline_usage="Agregada em weather_daily/weather_weekly quando reconhecida pelo pipeline.",
            usage_status="usada_quando_disponivel",
        )
    if "temp" in lower:
        return ColumnDocumentation(
            column=column,
            definition="Variável de temperatura.",
            practical_interpretation="Usada para checar plausibilidade climática e contexto de estresse.",
            pipeline_usage="Agregada em weather_daily/weather_weekly quando reconhecida pelo pipeline.",
            usage_status="usada_quando_disponivel",
        )
    if any(token in lower for token in ["geometry", "timestamp", "date time", "createdat", "data", "date"]):
        return ColumnDocumentation(
            column=column,
            definition="Campo temporal ou espacial usado para posicionar o registro no tempo/espaço.",
            practical_interpretation="Normalmente serve para criar date/week_start ou associar registros a áreas.",
            pipeline_usage="Usado quando reconhecido por loaders e joins; caso contrário fica como contexto de auditoria.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["engine", "fuel", "alarm", "telemetry", "gps"]):
        return ColumnDocumentation(
            column=column,
            definition="Campo bruto de telemetria/suporte operacional da máquina.",
            practical_interpretation="Ajuda a explicar risco operacional, falha de comunicação, alertas ou motor.",
            pipeline_usage="Subconjunto alimenta engine_risk_flag, alert_risk_flag e telemetry_risk_flag.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["yield", "productivity", "population", "speed", "pressure", "overlap", "duration", "dose", "dos", "configured", "applied"]):
        return ColumnDocumentation(
            column=column,
            definition="Campo operacional de execução agrícola.",
            practical_interpretation="Ajuda a verificar dose, colheita, plantio, velocidade, pressão, sobreposição ou tempo parado.",
            pipeline_usage="Subconjunto alimenta drivers de dose, sobreposição, stand, colheita ou parada; demais ficam como contexto.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["area_ha", "weight_kg"]):
        return ColumnDocumentation(
            column=column,
            definition="Métrica operacional derivada de área ou peso.",
            practical_interpretation="Ajuda a quantificar cobertura executada, volume colhido/aplicado ou normalização por hectare.",
            pipeline_usage="Gerada em ops_area_daily e inventários operacionais.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if lower in {"area_label", "comparison_pair", "treatment", "crop_type", "season_id_4_0", "season_id_convencional", "tech_area_label", "conv_area_label"}:
        return ColumnDocumentation(
            column=column,
            definition="Coluna de identificação de área, tratamento, cultura ou par de comparação.",
            practical_interpretation="Serve para separar Grão/Silagem e 4.0/Convencional, preservando rastreabilidade entre tabelas.",
            pipeline_usage="Usada como chave de filtro, agrupamento, comparação pareada e visualização.",
            usage_status="usada_como_chave",
            documentation_status="documentado_por_regra",
        )
    if lower.startswith("area_label") or lower.endswith("_4_0") or lower.endswith("_convencional"):
        return ColumnDocumentation(
            column=column,
            definition="Coluna pareada separada por tratamento 4.0 ou convencional.",
            practical_interpretation="Permite comparar lado a lado os dois tratamentos dentro do mesmo par.",
            pipeline_usage="Criada em tabelas pareadas/gaps para cálculo 4.0 menos convencional.",
            usage_status="usada_como_chave",
            documentation_status="documentado_por_regra",
        )
    if lower in {"rows", "columns", "numeric_columns", "null_cells", "null_ratio", "unique_seasons", "unique_areas", "sample_columns"}:
        return ColumnDocumentation(
            column=column,
            definition="Métrica de inventário/perfil do dataset.",
            practical_interpretation="Ajuda a auditar volume, completude e estrutura de tabelas geradas.",
            pipeline_usage="Gerada em CSVs de overview/perfil; não é entrada agronômica.",
            usage_status="usada_para_documentacao",
            documentation_status="documentado_por_regra",
        )
    if lower in {"scene_start", "scene_end", "first_scene", "last_scene", "date_start", "date_end", "weather_start", "weather_end"}:
        return ColumnDocumentation(
            column=column,
            definition="Marco temporal mínimo/máximo da cobertura de dados.",
            practical_interpretation="Mostra início/fim de cenas, clima ou observações disponíveis.",
            pipeline_usage="Usada para auditoria de cobertura temporal.",
            usage_status="usada_para_documentacao",
            documentation_status="documentado_por_regra",
        )
    if lower in {"missing", "q1", "q3", "cv", "skew", "kurtosis"}:
        return ColumnDocumentation(
            column=column,
            definition="Estatística descritiva de distribuição ou completude.",
            practical_interpretation="Ajuda a avaliar dispersão, assimetria, caudas, variação e dados faltantes.",
            pipeline_usage="Gerada em perfis estatísticos dos CSVs.",
            usage_status="usada_para_documentacao",
            documentation_status="documentado_por_regra",
        )
    if lower == "image_path":
        return ColumnDocumentation(
            column=column,
            definition="Caminho local para a imagem JPG associada à cena NDVI.",
            practical_interpretation="Permite abrir visualmente a cena quando a imagem existe no projeto.",
            pipeline_usage="Criada a partir do filename em load_ndvi_metadata; usada no rastreio por linha.",
            usage_status="usada",
            documentation_status="documentado_por_regra",
        )
    if "flag" in lower or lower.startswith("outlier"):
        return ColumnDocumentation(
            column=column,
            definition="Flag booleana/categórica derivada para marcar condição relevante.",
            practical_interpretation="Indica presença/ausência de evento, risco, outlier ou condição de diagnóstico.",
            pipeline_usage="Usada em timeline, drivers, testes, gaps e qualidade conforme a tabela.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if lower in {"metric", "metric_label", "analysis_target", "feature", "driver", "direction", "favors", "recommended_test"}:
        return ColumnDocumentation(
            column=column,
            definition="Identificador de métrica, feature, driver, alvo ou teste estatístico.",
            practical_interpretation="Ajuda a saber qual variável está sendo comparada, testada ou interpretada.",
            pipeline_usage="Usada em CSVs de testes, correlação, drivers e hipóteses.",
            usage_status="usada_para_documentacao",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["p_value", "_p", "shapiro", "pearson", "spearman", "correlation", "effect_size", "ci_low", "ci_high", "significant", "observations", "strength"]):
        return ColumnDocumentation(
            column=column,
            definition="Métrica estatística derivada para teste, correlação, intervalo de confiança ou força de evidência.",
            practical_interpretation="Ajuda a avaliar incerteza, direção e intensidade da relação observada.",
            pipeline_usage="Gerada em tabelas de testes pareados, testes clássicos ou correlações semanais.",
            usage_status="usada_para_evidencia",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["supported_hypotheses", "not_supported_hypotheses", "next_step", "top_problem_driver", "ndvi_effect_direction", "ndvi_effect_value"]):
        return ColumnDocumentation(
            column=column,
            definition="Campo sintético de decisão executiva ou fechamento de hipótese.",
            practical_interpretation="Resume efeito observado, hipótese sustentada/não sustentada, driver dominante ou próxima ação.",
            pipeline_usage="Gerado em decision_summary e usado para leitura final.",
            usage_status="usada_para_documentacao",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["mapping_source", "bbox_area", "soil_samples_available", "soil_context_only", "weather_window"]):
        return ColumnDocumentation(
            column=column,
            definition="Campo de auditoria/cobertura espacial ou contextual.",
            practical_interpretation="Ajuda a entender como a área foi mapeada, qual janela de dados existe ou qual contexto está disponível.",
            pipeline_usage="Gerado em inventários e tabelas de auditoria.",
            usage_status="usada_para_documentacao",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["phase", "event_type", "pair_position", "primary_driver", "secondary_driver", "story_sentence"]):
        return ColumnDocumentation(
            column=column,
            definition="Campo narrativo da timeline NDVI: fase, evento, posição no par ou driver interpretativo.",
            practical_interpretation="Transforma a série temporal em leitura humana sobre o que aconteceu naquela semana.",
            pipeline_usage="Gerado em ndvi_phase_timeline e reaproveitado em eventos, outlook e hipóteses.",
            usage_status="usada_para_documentacao",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["outlook", "expected_vs_pair", "latest_event", "latest_phase", "last_ndvi_norm", "top_risks", "pair_context"]):
        return ColumnDocumentation(
            column=column,
            definition="Campo de outlook/leitura final da trajetória NDVI.",
            practical_interpretation="Resume condição recente, risco dominante, expectativa relativa ao par e contexto de decisão.",
            pipeline_usage="Gerado em ndvi_outlook e usado no fechamento de H4/decisão.",
            usage_status="usada_para_documentacao",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["action_ray", "detected_boxes", "detected_species"]):
        return ColumnDocumentation(
            column=column,
            definition="Campo derivado de detecção/monitoramento MIIP.",
            practical_interpretation="Ajuda a qualificar leitura das armadilhas, espécie detectada ou raio de ação.",
            pipeline_usage="Gerado em miip_daily e usado como contexto fitossanitário.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if lower in {"has_weather_coverage", "solar_radiation_w_m2", "wind_avg_kmh", "water_balance_mm", "water_balance_mm_ma14"}:
        return ColumnDocumentation(
            column=column,
            definition="Campo meteorológico derivado ou indicador de cobertura climática.",
            practical_interpretation="Ajuda a entender se havia clima disponível e qual era o balanço/radiação/vento no período.",
            pipeline_usage="Gerado em weather_daily/weather_weekly e usado em contexto de estresse climático.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if lower in {"pair", "variable"}:
        return ColumnDocumentation(
            column=column,
            definition="Identificador de par ou variável no CSV de diagnóstico/perfil.",
            practical_interpretation="Indica qual par de comparação ou variável estatística está sendo descrita.",
            pipeline_usage="Usada em tabelas de diagnóstico e perfis numéricos.",
            usage_status="usada_para_documentacao",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["intercept", "r_value", "r_squared", "trend_direction", "coefficient", "residual", "alpha", "mae", "rmse", "r2", "model_choice", "features"]):
        return ColumnDocumentation(
            column=column,
            definition="Campo de modelo exploratório, tendência ou avaliação preditiva.",
            practical_interpretation="Ajuda a avaliar relação temporal, erro, coeficientes e direção de tendência.",
            pipeline_usage="Gerado em CSVs de modelo/transition e tendência; usado como apoio exploratório, não como prova causal isolada.",
            usage_status="usada_para_evidencia",
            documentation_status="documentado_por_regra",
        )
    if lower.endswith("_week") or "week" in lower or "weeks" in lower:
        return ColumnDocumentation(
            column=column,
            definition="Coluna semanal derivada para alinhar NDVI, clima, operação e MIIP na mesma granularidade temporal.",
            practical_interpretation="Permite comparar semanas equivalentes entre 4.0 e convencional.",
            pipeline_usage="Criada por agregação semanal ou por cálculo na timeline NDVI.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["ratio", "rate", "pct", "share", "percent"]):
        return ColumnDocumentation(
            column=column,
            definition="Métrica proporcional/percentual derivada.",
            practical_interpretation="Facilita comparar áreas de tamanho diferente ou frequências em janelas diferentes.",
            pipeline_usage="Usada em diagnósticos, qualidade, drivers ou leitura executiva conforme o CSV/tabela.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["count", "total", "hits", "days", "images", "events", "readings"]):
        return ColumnDocumentation(
            column=column,
            definition="Contagem ou total derivado a partir de registros brutos.",
            practical_interpretation="Indica cobertura, frequência ou volume de eventos observados.",
            pipeline_usage="Usada para auditoria de cobertura, agregações e apoio às hipóteses.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["mean", "median", "std", "min", "max", "peak", "auc", "delta", "gap", "score", "zscore"]):
        return ColumnDocumentation(
            column=column,
            definition="Estatística ou score derivado no pipeline analítico.",
            practical_interpretation="Resume nível, variação, diferença, pico ou anomalia de uma série/feature.",
            pipeline_usage="Usada em CSVs finais, gráficos, testes e hipóteses conforme o dataframe de origem.",
            usage_status="usada_quando_disponivel",
            documentation_status="documentado_por_regra",
        )
    if any(token in lower for token in ["status", "winner", "evidence", "interpretation", "message", "note", "summary", "hypothesis", "decision", "basis", "limits", "recommend"]):
        return ColumnDocumentation(
            column=column,
            definition="Coluna textual/categórica de interpretação, decisão ou documentação do resultado.",
            practical_interpretation="Ajuda a explicar o resultado para leitura humana; normalmente não é entrada numérica de modelo.",
            pipeline_usage="Gerada nos CSVs finais de decisão, hipótese, auditoria ou relatório.",
            usage_status="usada_para_documentacao",
            documentation_status="documentado_por_regra",
        )
    return ColumnDocumentation(
        column=column,
        definition="Coluna detectada automaticamente nos arquivos do projeto.",
        practical_interpretation="Sem definição externa específica encontrada; inspecione exemplos e uso no pipeline.",
        pipeline_usage="Uso inferido pelo catálogo de lineage. Se não aparecer em features/tabelas, fica como coluna de contexto ou ignorada.",
        usage_status="inferida",
        documentation_status="inferido_do_codigo",
    )


def driver_documentation_rows() -> list[dict[str, Any]]:
    return [
        {
            "driver": doc.driver,
            "flag_feature": doc.flag_feature,
            "title": doc.title,
            "born_table": doc.born_table,
            "source_columns": doc.source_columns,
            "rule": doc.rule,
            "hypotheses": doc.hypotheses,
            "charts": doc.charts,
            "final_csvs": doc.final_csvs,
        }
        for doc in DRIVER_DOCUMENTATION.values()
    ]
