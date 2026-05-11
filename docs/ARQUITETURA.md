# Arquitetura do Repositorio

O repositorio foi separado em camadas para manter a análise NDVI, os dados privados, os outputs gerados e a interface de auditoria com responsabilidades claras.

## 0. Dados privados e bootstrap local

A pasta `data/` é privada e não deve ser versionada. O repositório contém apenas a lógica para resolver, baixar e extrair essa pasta quando ela não existir.

Configuração:

- `.env`: arquivo local ignorado pelo Git, usado para segredos e paths locais;
- `.env.example`: template sem segredos;
- `MONOLITHFARM_DATA_DIR`: destino local da pasta de dados, por padrão `data`;
- `MONOLITHFARM_DATA_ARCHIVE_URL`: URL privada do pacote compactado;
- `MONOLITHFARM_DATA_COOKIE_FILE`: opcional, cookies de sessão para downloads privados;
- `MONOLITHFARM_DATA_ARCHIVE_SHA256`: opcional, checksum de integridade.

Implementação:

- `scripts/bootstrap_data.py`: baixa, valida e extrai o pacote em diretório temporário; só move para `data/` ao final;
- `scripts/start_lineage_atlas.ps1`: carrega `.env`, garante `data/`, gera o atlas e sobe o frontend;
- `dashboard/lineage/runtime.py`: lê `.env` e respeita `MONOLITHFARM_DATA_DIR`;
- `scripts/export_lineage_atlas_data.py`: também garante `data/` antes de montar o JSON do Atlas.

Regras de segurança:

- o link real do pacote privado não aparece em docs nem no frontend;
- `.env` fica ignorado pelo Git;
- arquivos compactados são extraídos com proteção contra path traversal;
- downloads privados podem exigir cookie local do usuário autorizado;
- o pacote é baixado para pasta temporária e não substitui `data/` parcialmente.

Deploy:

- Render deve ser configurado como Web Service, pois Static Site não executa `/api/private/*`;
- `scripts/render_build.sh` instala o pacote Python, garante `data/`, exporta `atlas-data.json` e compila o frontend;
- `scripts/render_start.sh` sobe `vite preview` em `0.0.0.0:$PORT` com o plugin privado ativo;
- usuário, senha e URL privada entram somente como variáveis de ambiente do serviço.

## 1. Pipeline Analitico NDVI

Este e o fluxo oficial do projeto para estudo, notebook, hipoteses, metricas e CSVs finais.

Entradas principais:

- `data/OneSoil - Imagens de Satelite/CSV/ndvi_metadata.csv`
- `data/Metos - Estacao Meteorologica/...`
- `data/EKOS - Telemetria e Armadilhas Eletronicas/...`
- `data/Cropman - Analise de solo/...`

Implementacao:

- `farmlab/io.py`
- `farmlab/pairwise.py`
- `farmlab/ndvi_deepdive.py`
- `farmlab/ndvi_crispdm.py`
- `farmlab/complete_analysis.py`

Artefato principal:

- `notebooks/complete_ndvi_analysis.ipynb`

Saidas principais:

- `notebook_outputs/complete_ndvi/*.csv`

## 2. Dashboard Streamlit

Este e um subsistema separado para leitura interativa. Ele nao e a fonte de verdade das hipoteses H1-H4 nem dos CSVs finais do pipeline NDVI.

Implementacao:

- `dashboard/app.py`
- `dashboard/workspace.py`
- `dashboard/database.py`

Inicializacao:

- `scripts/start_dashboard.ps1`
- `scripts/start_dashboard.sh`

Persistencia:

- `storage/monolithfarm.duckdb`

## 3. Atlas de Auditoria e Lineage NDVI

Este subsistema existe para inspeção humana do pipeline NDVI: arquivos brutos, tabelas intermediárias, CSVs finais, features derivadas, gráficos e hipóteses.

Implementacao:

- `lineage_atlas/src/App.tsx`
- `lineage_atlas/src/styles.css`
- `lineage_atlas/server/private-data-plugin.ts`
- `lineage_atlas/server/table_reader.py`
- `scripts/export_lineage_atlas_data.py`
- `dashboard/lineage/registry.py`
- `dashboard/lineage/runtime.py`
- `dashboard/lineage/column_catalog.py`
- `dashboard/lineage/column_lineage.py`
- `dashboard/lineage/docs_registry.py`
- `dashboard/lineage/doc_scraper.py`
- `dashboard/lineage/quality_rules.py`

Inicializacao:

- `scripts/start_lineage_atlas.ps1`
- `scripts/start_feature_lineage_app.sh` redireciona para o Atlas React por compatibilidade.

### Data Vault autenticado

O Atlas tem duas superfícies de dados:

- `atlas-data.json`: público/local, sem conteúdo completo, previews reais ou auditoria linha-a-linha;
- `/api/private/*`: endpoints locais autenticados para abrir arquivos completos.

O Data Vault permite visualizar:

- CSVs brutos de `data/`;
- Parquets brutos de `data/`;
- imagens NDVI e documentos;
- CSVs finais em `notebook_outputs/complete_ndvi`;
- tabelas intermediárias;
- arquivos de lineage, auditoria e revisão.

A leitura de CSV/Parquet é paginada e feita sob demanda. O navegador recebe apenas a página solicitada, não o dataset inteiro. A API privada usa sessão por cookie HttpOnly, IDs opacos de arquivo e allowlist limitada a `data/` e `notebook_outputs/complete_ndvi/`.

## 4. Wrappers de compatibilidade

Os arquivos abaixo foram mantidos apenas para nao quebrar comandos e imports antigos:

- `streamlit_app.py`
- `farmlab/analysis.py`
- `farmlab/database.py`

Eles nao devem mais ser tratados como implementacao principal.

## 5. Regra pratica

- quer entender a análise completa: abra `notebooks/complete_ndvi_analysis.ipynb`;
- quer inspecionar a lógica analítica: abra os módulos `farmlab/*` do pipeline NDVI;
- quer rastrear feature, coluna, CSV e hipótese: abra o Atlas em `lineage_atlas/`;
- quer ver conteúdo integral dos arquivos: use `Dados privados` no Atlas após login local;
- quer regenerar dados e UI: rode `scripts/start_lineage_atlas.ps1`.
