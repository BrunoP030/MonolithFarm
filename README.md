# MonolithFarm

Plataforma analitica para comparacao entre areas de milho com manejo convencional e areas acompanhadas por tecnologias 4.0, com foco em NDVI, produtividade, clima, pragas e contexto operacional.

## Resumo

Este repositorio materializa um MVP de ciencia de dados agricola para responder uma pergunta central:

- por que uma area apresentou melhor desempenho agronomico e produtivo do que outra na mesma safra?

O projeto integra fontes heterogeneas (satelite, operacao, clima, solo e pragas), persiste em banco local DuckDB e disponibiliza leitura visual em Streamlit.

## Objetivo do Projeto

Comparar, com base tecnica, areas de milho:

- manejo convencional;
- manejo apoiado por tecnologias 4.0.

O objetivo final e sustentar explicacoes sobre vigor vegetativo, produtividade e eficiencia operacional, preparando o caminho para comparacao economica (custo por hectare e retorno).

## Arquitetura da Solucao

O sistema esta organizado em quatro camadas:

1. ingestao: descoberta e leitura automatica dos arquivos do pacote `FarmLab`;
2. tratamento: normalizacao, conversoes de tipos e enriquecimentos;
3. persistencia: materializacao em `DuckDB`;
4. visualizacao: painel analitico em `Streamlit`.

Fluxo tecnico principal:

- [farmlab/io.py](farmlab/io.py)
- [farmlab/analysis.py](farmlab/analysis.py)
- [farmlab/database.py](farmlab/database.py)
- [streamlit_app.py](streamlit_app.py)

## Mapeamento do Ecossistema FarmLab

Documentacao oficial consultada:

- <https://farm.labs.unimar.br>
- <https://farm.labs.unimar.br/docs/dados/miip>
- <https://farm.labs.unimar.br/docs/guias/geotiff>
- <https://farm.labs.unimar.br/docs/guias/shapefile>

Categorias de dados descritas no portal:

- `satelite` (OneSoil): recortes NDVI e metadados estatisticos;
- `meteorologia` (Metos): serie horaria;
- `solo` (Cropman): pontos, zonas e recomendacoes;
- `ekos_camadas`: operacoes geoespaciais de campo;
- `ekos_cadastros`: talhoes, maquinas, operadores e implementos;
- `ekos_ordens`: ordens de servico e regras de alerta;
- `ekos_paradas`: motivos de parada e atividades;
- `miip`: catalogo de pragas e eventos de armadilhas.

## Escopo Atual no Repositorio

No estado atual, o app ja processa bem:

- NDVI metadata + imagens;
- clima horario;
- armadilhas (lista e leituras consolidadas);
- camadas operacionais de plantio e colheita;
- sugestao de vinculacao entre recortes NDVI e areas.

Pontos ainda pendentes para "fluxo completo" do portal:

- ingestao integral de `miip` (incluindo `traps_events` com maior profundidade analitica);
- ingestao de `ekos_cadastros`, `ekos_ordens` e `ekos_paradas`;
- ampliacao para todas as camadas EKOS descritas no portal;
- camada economica robusta (custos por hectare/operacao) para prova de eficiencia financeira.

## Dados Esperados

O projeto resolve o diretorio de dados nesta ordem:

1. variavel de ambiente `MONOLITHFARM_DATA_DIR`;
2. pasta local `./FarmLab` (raiz do repositorio);
3. fallback legado `C:\Users\Morgado\Downloads\FarmLab`.

## Execucao Rapida

### Windows

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh
```

### Linux/macOS

```bash
REFRESH=1 ./scripts/start_dashboard.sh
```

Depois, acessar:

```text
http://127.0.0.1:8501
```

Se a porta estiver ocupada:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -Port 8502
```

```bash
REFRESH=1 ./scripts/start_dashboard.sh "" "storage/monolithfarm.duckdb" 8502
```

## Guias de Execucao

- [docs/COMO_EXECUTAR.md](docs/COMO_EXECUTAR.md)
- [docs/PRIMEIRO_USO_FACULDADE.md](docs/PRIMEIRO_USO_FACULDADE.md)

## Estrutura Principal do Repositorio

- [streamlit_app.py](streamlit_app.py): interface principal do painel
- [farmlab/analysis.py](farmlab/analysis.py): regras analiticas e sintese dos dados
- [farmlab/database.py](farmlab/database.py): persistencia e leitura do banco local
- [farmlab/io.py](farmlab/io.py): descoberta e ingestao dos arquivos brutos
- [scripts/start_dashboard.ps1](scripts/start_dashboard.ps1): inicializacao assistida (Windows)
- [scripts/start_dashboard.sh](scripts/start_dashboard.sh): inicializacao assistida (Linux/macOS)

## Arquivos Auxiliares

- [templates/season_mapping.example.csv](templates/season_mapping.example.csv)
- [templates/costs.example.csv](templates/costs.example.csv)

## Limitacoes Atuais

- recortes NDVI sem identificacao oficial de talhao no arquivo fonte;
- pacote atual com JPG + metadados, sem TIFF numerico original no repositorio;
- solo sem chave espacial direta para cada talhao no pipeline atual;
- estacao meteorologica representa contexto macro da fazenda;
- comparacao economica depende de custos externos fornecidos pelo usuario.

## Proximos Passos Recomendados

1. integrar todas as tabelas EKOS e MIIP descritas na documentacao oficial;
2. padronizar joins por `Service Order`, `field`, `trap`, `data/hora`;
3. consolidar feature store por area e janela temporal;
4. aplicar modelagem explicativa/preditiva para causalidade tecnica;
5. fechar comparacao economica com custo por hectare e retorno produtivo.
