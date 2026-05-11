# MonolithFarm Atlas NDVI

Esta camada sustenta o Atlas React/TypeScript em `lineage_atlas/`. Ela não é mais uma interface Streamlit de lineage; o Streamlit permanece apenas no dashboard operacional geral em `dashboard/app.py`.

O objetivo é explicar, com dados reais, como arquivos brutos, colunas, features, tabelas intermediárias, CSVs finais, gráficos e hipóteses H1-H4 se conectam.

## Interface principal

- `lineage_atlas/src/App.tsx`: experiência web, canvas React Flow, painéis de detalhe, correlações, auditoria e storytelling.
- `lineage_atlas/src/styles.css`: tema claro/escuro, layout premium e estilos do canvas.
- `lineage_atlas/public/atlas-data.json`: payload consumido pelo frontend.
- `scripts/export_lineage_atlas_data.py`: gera o payload do atlas a partir dos dados reais, registries e documentação FarmLab.
- `scripts/start_lineage_atlas.ps1`: exporta dados e abre o Vite local.

## Camada de metadados

- `dashboard/lineage/registry.py`: registry auditável de tabelas, CSVs, features, hipóteses e gráficos.
- `dashboard/lineage/runtime.py`: resolução de caminhos, pipeline real, outputs e arquivos brutos.
- `dashboard/lineage/docs_registry.py`: documentação consolidada de fontes, colunas e drivers.
- `dashboard/lineage/doc_scraper.py`: extração/cache da documentação pública do FarmLab.
- `dashboard/lineage/column_catalog.py`: catálogo de colunas brutas, features e colunas geradas.
- `dashboard/lineage/column_lineage.py`: mapeamento coluna bruta -> feature -> CSV -> gráfico/hipótese.
- `dashboard/lineage/data_profiler.py`: perfil estatístico simples de tabelas e colunas.
- `dashboard/lineage/quality_rules.py`: regras de qualidade e plausibilidade dos dados.
- `dashboard/lineage/interactive_network.py`: estruturas de grafo e manifesto para navegação.
- `dashboard/lineage/cache/farmlab_docs_cache.json`: cache local extraído de `https://farm.labs.unimar.br/docs`.

## Como rodar

Windows PowerShell:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_lineage_atlas.ps1 -Port 5173 -RefreshDocs
```

Linux/macOS/WSL:

```bash
./scripts/start_feature_lineage_app.sh 5173
```

Ou via CLI Python:

```bash
monolithfarm-audit --port 5173
```

URL esperada:

```text
http://127.0.0.1:5173
```

## Páginas do Atlas

- `Visão geral`: problema central, números principais e atalhos.
- `Canvas`: mapa tipo segundo cérebro com foco, vizinhança, filtros, zoom e detalhe lateral.
- `Arquivos`: inventário real de `data/`, previews, datas e colunas.
- `Colunas`: busca global de coluna, significado, origem, uso e documentação FarmLab.
- `Features`: definição, função geradora, colunas de entrada, thresholds, CSVs e hipóteses.
- `Tabelas`: tabelas intermediárias, joins, filtros, agregações e preview.
- `CSVs finais`: outputs, método, colunas explicadas, gráficos e hipóteses.
- `H1-H4`: pergunta, regra de decisão, evidência, decisão e limitação.
- `Correlações`: explorer de Pearson/Spearman com aliases como `solo_exposto` -> `soil_pct_week` e `risco_de_motor` -> `engine_risk_flag`.
- `Auditoria`: filtro por safra, área, par, tratamento e semana.
- `Storytelling`: síntese final sobre grão, silagem, 4.0 vs convencional e limitações.
- `Docs FarmLab`: páginas e schemas extraídos da documentação oficial.

## CSV vs DuckDB

Os CSVs brutos continuam sendo a fonte oficial auditável. O Atlas usa CSVs finais pequenos, manifesto e previews para manter rastreabilidade. DuckDB continua opcional como cache de consulta pesada, sem substituir os arquivos originais em `data/`.

## Manifesto fora da interface

Para materializar os artefatos de auditoria sem abrir o frontend:

```bash
monolithfarm-audit --export-manifest --outputs-only
```
