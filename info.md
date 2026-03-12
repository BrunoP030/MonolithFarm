# Contexto do Projeto (Versao Organizada)

## 1. Problema Central

Comparar duas areas de milho:

- Area A: manejo convencional;
- Area B: manejo com tecnologias 4.0 (ex.: armadilhas eletronicas e monitoramento mais intensivo).

Pergunta principal:

- por que em alguns pontos a area convencional performa melhor e em outros a area 4.0 performa melhor?

## 2. Objetivo de Negocio/Academico

Construir uma explicacao tecnica e defensavel para:

- diferencas de desenvolvimento vegetativo (NDVI);
- diferencas de produtividade;
- impacto de operacao, clima, pragas e solo;
- comparacao de custo x resultado entre manejo convencional e 4.0.

## 3. Topicos Prioritarios da Analise

- Plantio;
- Adubacao;
- Pulverizacao;
- NDVI (imagens e metadados);
- Pragas (MIIP e armadilhas);
- Colheita;
- Clima (estacao meteorologica).

## 4. Hipotese Principal

A area com tecnologias 4.0 tende a entregar melhor relacao custo-beneficio quando:

- ha monitoramento mais rapido e acao operacional mais precisa;
- ha menor perda por pragas/estresse;
- ha melhor eficiencia operacional por hectare.

## 5. Situacao Atual dos Dados

- Ja existem muitos dados de operacao, NDVI, clima e pragas;
- dados de colheita/custo podem estar incompletos dependendo da versao do pacote recebido;
- para prova economica forte, e obrigatorio consolidar custo por hectare e por operacao.

## 6. Perguntas Tecnicas que Precisam ser Respondidas

- Quais variaveis mais explicam a diferenca de produtividade entre as areas?
- Qual o efeito temporal de chuva/temperatura sobre variacao de NDVI?
- Onde ha sobreposicao operacional, paradas e perda de eficiencia?
- Qual o efeito da pressao de pragas por area e por janela temporal?
- Qual area gera maior retorno por real investido (`kg/R$`)?

## 7. Riscos de Interpretacao (para controlar)

- dados meteorologicos podem ter lacunas, outliers e representar apenas contexto macro;
- diferencas de escala espacial (pixel NDVI x poligono operacional x ponto de armadilha);
- divergencia de chave entre fontes (talhao, service order, trap);
- atraso temporal entre causa e efeito (ex.: chuva hoje, resposta de NDVI depois).

## 8. Plano de Execucao Recomendado

1. consolidar ingestao completa de todas as fontes (EKOS, MIIP, OneSoil, Metos, solo);
2. padronizar tempo, CRS e chaves de join;
3. gerar feature store por area e por periodo;
4. executar analise exploratoria + modelos explicativos;
5. fechar camada economica com custos reais;
6. transformar resultados em narrativa de causa e evidencia.

## 9. Apoio de Dominio

- envolver suporte agronomico para validacao das hipoteses e dos resultados;
- manter alinhamento com os dados que serao fornecidos pela equipe da Jacto.

## 10. Links de Referencia

- https://farm.labs.unimar.br
- https://farm.labs.unimar.br/docs/guias/geotiff
- https://farm.labs.unimar.br/docs/dados/miip
- https://farm.labs.unimar.br/docs/guias/shapefile
