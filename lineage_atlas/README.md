# MonolithFarm Atlas NDVI

Frontend React para explorar o projeto como um catálogo de dados com lineage visual.

Ele consome `public/atlas-data.json`. A partir da raiz do repositório, gere esse arquivo com:

```powershell
.\.venv_win\Scripts\python.exe scripts\export_lineage_atlas_data.py
```

Antes de exportar, o script garante a existência de `data/`. Se a pasta não existir, ele lê `.env` e usa `MONOLITHFARM_DATA_ARCHIVE_URL` para baixar e extrair o pacote privado. O link real deve ficar apenas no `.env` local.

Rodar em modo desenvolvimento:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_lineage_atlas.ps1 -Port 5173
```

O script gera uma senha temporária local para a página `Dados privados` quando `MONOLITH_ATLAS_PASSWORD` não está definido. Para usar credenciais fixas:

```powershell
$env:MONOLITH_ATLAS_USER="monolito_farm"
$env:MONOLITH_ATLAS_PASSWORD="uma-senha-local-forte"
powershell -ExecutionPolicy Bypass -File .\scripts\start_lineage_atlas.ps1 -Port 5173
```

Os conteúdos completos de `data/` e `notebook_outputs/complete_ndvi/` não ficam no bundle público. Eles são servidos sob demanda por `/api/private/*` somente após login, com cookie HttpOnly, IDs opacos de arquivo, allowlist de diretórios e paginação para CSV/Parquet.

Na interface, a página `Dados privados` mostra:

- CSVs brutos;
- Parquets brutos;
- imagens e documentos;
- CSVs finais;
- tabelas intermediárias;
- arquivos de lineage/auditoria;
- revisões geradas.

Se aparecer aviso de API privada inativa, reinicie pelo script `scripts/start_lineage_atlas.ps1`; rodar `npm run dev` diretamente não executa o bootstrap de `data/` nem gera senha temporária.

Para deploy no Render, use Web Service:

```bash
bash scripts/render_build.sh
bash scripts/render_start.sh
```

No Render, configure `MONOLITHFARM_DATA_ARCHIVE_URL`, `MONOLITH_ATLAS_USER`, `MONOLITH_ATLAS_PASSWORD` e `MONOLITH_ATLAS_COOKIE_SECURE=1` como variáveis de ambiente do serviço. Não exponha esses valores em repositório ou documentação.

Atualizar também o cache das páginas oficiais FarmLab:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_lineage_atlas.ps1 -Port 5173 -RefreshDocs
```

Áreas principais:

- `Canvas`: grafo arrastável com arquivos brutos, tabelas intermediárias, CSVs finais, hipóteses e gráficos. Busca ou modo `Colunas` traz colunas brutas/finais para o canvas.
- `Arquivos`: todos os arquivos brutos detectados em `data/`, agrupáveis por fonte.
- `Dados privados`: login e visualização completa, paginada e autenticada dos CSVs brutos, CSVs gerados, Parquets, imagens, PDFs e textos.
- `Colunas`: busca global no manifesto de lineage coluna-a-coluna.
- `Features`: definição, geração, código, filtros, thresholds, CSVs e hipóteses por feature.
- `Docs FarmLab`: rotas `/docs`, schemas e colunas extraídos do portal FarmLab.
- `História`: síntese final da análise NDVI.
