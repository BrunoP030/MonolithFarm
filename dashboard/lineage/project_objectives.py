from __future__ import annotations

from copy import deepcopy
from typing import Any


PROJECT_OBJECTIVES: dict[str, Any] = {
    "centralProblem": [
        {
            "title": "Comparar duas areas de milho na mesma safra",
            "statement": "Entender por que a area convencional performa melhor em alguns pontos e a area com tecnologias 4.0 performa melhor em outros.",
            "evidenceType": "documentacao_oficial_do_projeto",
            "interpretationStatus": "objetivo_do_projeto",
        }
    ],
    "businessAcademicGoals": [
        {
            "title": "Explicacao tecnica defensavel",
            "statement": "Construir uma leitura auditavel sobre desenvolvimento vegetativo, NDVI, produtividade, operacao, clima, pragas, solo e custo x resultado.",
            "evidenceType": "documentacao_oficial_do_projeto",
            "interpretationStatus": "meta_analitica",
        },
        {
            "title": "Base para apresentacao academica e manutencao futura",
            "statement": "Permitir que professores, pesquisadores, equipe tecnica e usuarios nao tecnicos naveguem da pergunta ate as evidencias e limitacoes.",
            "evidenceType": "documentacao_oficial_do_projeto",
            "interpretationStatus": "meta_de_produto",
        },
    ],
    "priorityTopics": [
        {"name": "Plantio", "domain": "operacao", "expectedEvidence": "populacao, datas, area e cobertura operacional"},
        {"name": "Adubacao", "domain": "operacao", "expectedEvidence": "dose aplicada, dose configurada e falhas de dose"},
        {"name": "Pulverizacao", "domain": "operacao", "expectedEvidence": "pressao, velocidade, sobreposicao e janela operacional"},
        {"name": "NDVI, imagens e metadados", "domain": "satelite", "expectedEvidence": "b1_mean, solo exposto, vegetacao densa, pixels validos e datas"},
        {"name": "Pragas, MIIP e armadilhas", "domain": "miip", "expectedEvidence": "contagens, alertas, eventos, dano e controle"},
        {"name": "Colheita", "domain": "produtividade", "expectedEvidence": "yield kg/ha, area colhida e cobertura por talhao/area"},
        {"name": "Clima e estacao meteorologica", "domain": "meteorologia", "expectedEvidence": "chuva, temperatura, umidade, vento e balanco hidrico"},
    ],
    "mainHypothesis": [
        {
            "title": "Tecnologias 4.0 podem melhorar custo-beneficio",
            "statement": "A area 4.0 tende a entregar melhor relacao custo-beneficio quando ha monitoramento mais rapido, acao operacional mais precisa, menor perda por pragas/estresse e melhor eficiencia por hectare.",
            "evidenceType": "hipotese",
            "interpretationStatus": "nao_comprovada_por_si_so",
            "requiredEvidence": ["produtividade consolidada", "custos reais por operacao/hectare", "drivers temporais", "qualidade e cobertura dos dados"],
        }
    ],
    "currentDataSituation": [
        {
            "topic": "Cobertura ampla de fontes",
            "status": "disponivel_parcial",
            "detail": "Existem dados de operacao, NDVI, clima, solo e pragas, mas cada fonte tem escala espacial, chaves e granularidade temporal diferentes.",
        },
        {
            "topic": "Colheita e custos",
            "status": "limitacao_conhecida",
            "detail": "Dados de colheita/custo podem estar incompletos; a prova economica forte depende de custo por hectare e por operacao.",
        },
        {
            "topic": "Raster NDVI",
            "status": "limitacao_conhecida",
            "detail": "O pacote local trabalha com metadados e JPGs de apoio; GeoTIFF/pixel original nao e assumido como disponivel.",
        },
    ],
    "technicalQuestions": [
        {
            "question": "Quais variaveis mais explicam a diferenca de produtividade entre as areas?",
            "currentAnswerStatus": "dependente_de_produtividade_e_custos_consolidados",
            "safeInterpretation": "Ate consolidar colheita e custos, drivers sao sinais associados, nao causa fechada.",
        },
        {
            "question": "Qual o efeito temporal de chuva/temperatura sobre variacao de NDVI?",
            "currentAnswerStatus": "avaliavel_com_correlacoes_e_janelas_temporais",
            "safeInterpretation": "Correlacoes e lags podem indicar associacao temporal; nao provam causalidade isolada.",
        },
        {
            "question": "Onde ha sobreposicao operacional, paradas e perda de eficiencia?",
            "currentAnswerStatus": "avaliavel_por_camadas_ekos_e_flags_operacionais",
            "safeInterpretation": "Usar area aproximada, duracao e flags como triagem auditavel de risco operacional.",
        },
        {
            "question": "Qual o efeito da pressao de pragas por area e por janela temporal?",
            "currentAnswerStatus": "avaliavel_por_miip_e_event_driver_lift",
            "safeInterpretation": "Pressao de pragas e driver associado quando aparece mais em semanas-problema.",
        },
        {
            "question": "Qual area gera maior retorno por real investido, por exemplo kg/R$?",
            "currentAnswerStatus": "bloqueada_sem_custos_reais",
            "safeInterpretation": "Sem custos consolidados, a conclusao economica deve permanecer explicitamente bloqueada.",
        },
    ],
    "interpretationRisks": [
        {
            "risk": "Dados meteorologicos podem ter lacunas, outliers e representar apenas contexto macro.",
            "mitigation": "Exibir cobertura, outliers e caveat antes de ligar clima a NDVI.",
            "evidenceType": "limitacao_conhecida",
        },
        {
            "risk": "Escalas espaciais diferem entre pixel NDVI, poligono operacional e ponto de armadilha.",
            "mitigation": "Separar associacao espacial aproximada de prova espacial direta.",
            "evidenceType": "limitacao_conhecida",
        },
        {
            "risk": "Chaves divergem entre talhao, ordem de servico, area e armadilha.",
            "mitigation": "Expor chaves usadas, joins e status de cobertura no lineage.",
            "evidenceType": "limitacao_conhecida",
        },
        {
            "risk": "Causa e efeito podem ter atraso temporal.",
            "mitigation": "Mostrar periodo, janela e possibilidade de lag nas leituras.",
            "evidenceType": "limitacao_conhecida",
        },
        {
            "risk": "Dados de colheita/custo podem estar incompletos.",
            "mitigation": "Bloquear conclusao economica forte ate custo e produtividade estarem completos.",
            "evidenceType": "limitacao_conhecida",
        },
    ],
    "recommendedExecutionPlan": [
        {"step": 1, "title": "Consolidar ingestao completa", "status": "em_andamento", "deliverable": "inventario de fontes e arquivos"},
        {"step": 2, "title": "Padronizar tempo, CRS e chaves de join", "status": "em_andamento", "deliverable": "lineage de chaves e tabelas intermediarias"},
        {"step": 3, "title": "Gerar feature store por area e periodo", "status": "implementado_parcial", "deliverable": "pairwise_weekly_features e ndvi_phase_timeline"},
        {"step": 4, "title": "Executar EDA e modelos explicativos", "status": "implementado_parcial", "deliverable": "testes pareados, correlacoes e drivers"},
        {"step": 5, "title": "Fechar camada economica com custos reais", "status": "bloqueado_por_dado", "deliverable": "kg/R$ e custo por hectare"},
        {"step": 6, "title": "Transformar resultados em narrativa de causa e evidencia", "status": "em_andamento", "deliverable": "Atlas, storytelling e gates de aceitacao"},
    ],
    "domainSupport": [
        {
            "need": "Validacao agronomica",
            "detail": "Hipoteses e drivers devem ser revisados com suporte agronomico antes de virarem conclusao forte.",
        },
        {
            "need": "Alinhamento com equipe fornecedora dos dados",
            "detail": "Confirmar nomes de areas, chaves oficiais, lacunas e disponibilidade de custos/produtividade.",
        },
    ],
    "referenceLinks": [
        {"label": "FarmLab", "url": "https://farm.labs.unimar.br", "sourceType": "documentacao_oficial"},
        {"label": "Guia GeoTIFF", "url": "https://farm.labs.unimar.br/docs/guias/geotiff", "sourceType": "documentacao_oficial"},
        {"label": "Dados MIIP", "url": "https://farm.labs.unimar.br/docs/dados/miip", "sourceType": "documentacao_oficial"},
        {"label": "Guia Shapefile", "url": "https://farm.labs.unimar.br/docs/guias/shapefile", "sourceType": "documentacao_oficial"},
    ],
    "evidenceLegend": [
        {"label": "dado_real", "meaning": "Valor ou arquivo completo acessivel apenas no Data Vault autenticado."},
        {"label": "metadado", "meaning": "Schema, contagem, nome de coluna, periodo, tipo ou cobertura publicavel."},
        {"label": "documentacao_oficial", "meaning": "Conteudo extraido ou referenciado do FarmLab/docs do projeto."},
        {"label": "inferencia", "meaning": "Descricao derivada de codigo, nomes de colunas ou comportamento observado."},
        {"label": "associacao_estatistica", "meaning": "Correlacao, lift ou comparacao que nao prova causalidade."},
        {"label": "hipotese", "meaning": "Pergunta ou tese ainda dependente de evidencia suficiente."},
        {"label": "limitacao_conhecida", "meaning": "Risco que restringe interpretacao ou conclusao."},
    ],
}


def project_objectives() -> dict[str, Any]:
    return deepcopy(PROJECT_OBJECTIVES)
