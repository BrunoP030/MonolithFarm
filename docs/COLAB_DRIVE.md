# Google Colab com Drive

Instrucao para rodar o notebook completo do projeto no Google Colab usando a pasta do Drive onde o repositorio e os dados estao salvos.

## Objetivo

Executar `notebooks/complete_ndvi_analysis.ipynb` no Colab com leitura da pasta `data/` do proprio projeto.

## Estrutura no Drive

Exemplo:

```text
My Drive/
  7 - Semestre/
    Fábrica de Projeto - Ettore/
      Projeto-FarmLab/
        data/
        notebooks/
        scripts/
        farmlab/
        pyproject.toml
```

Esse e o primeiro caminho procurado automaticamente pelos notebooks no Colab.

Tambem sao aceitas as variantes com `MyDrive` e com a pasta `Fábrica` em forma Unicode alternativa.

## Passo a Passo

1. Envie o repositorio inteiro para o Google Drive.
2. Abra `notebooks/complete_ndvi_analysis.ipynb` no Colab.
3. Rode a primeira celula.
4. Autorize a montagem do Drive quando o Colab solicitar.
5. Confirme os caminhos impressos:
   - `PROJECT_DIR`
   - `DATA_DIR`
   - `OUTPUT_DIR`
6. Execute o restante do notebook.

## Arquivo Local de Paths

Para nao commitar rotas pessoais, crie um arquivo local na raiz do projeto:

```text
.monolithfarm.paths.json
```

Esse arquivo esta ignorado no Git. Use [monolithfarm.paths.example.json](../monolithfarm.paths.example.json) como modelo.

Exemplo:

```json
{
  "default_profile": "colab_drive",
  "profiles": {
    "local": {
      "notebook_mode": "jupyter",
      "project_dir": ".",
      "data_dir": "data",
      "output_root": "notebook_outputs",
      "auto_install": false
    },
    "colab_drive": {
      "notebook_mode": "colab",
      "project_dir": "/content/drive/My Drive/7 - Semestre/Fábrica de Projeto - Ettore/Projeto-FarmLab",
      "data_dir": "/content/drive/My Drive/7 - Semestre/Fábrica de Projeto - Ettore/Projeto-FarmLab/data",
      "output_root": "/content/drive/My Drive/7 - Semestre/Fábrica de Projeto - Ettore/Projeto-FarmLab/notebook_outputs",
      "auto_install": true
    }
  }
}
```

Selecao do perfil:

```python
import os
os.environ["MONOLITHFARM_PROFILE"] = "colab_drive"
```

Ou:

```python
import os
os.environ["MONOLITHFARM_PROFILE"] = "local"
```

## Variaveis de Ambiente Opcionais

Para definir os caminhos explicitamente, use antes da execucao da primeira celula:

```python
import os
os.environ["MONOLITHFARM_NOTEBOOK_MODE"] = "colab"
os.environ["MONOLITHFARM_PROJECT_DIR"] = "/content/drive/My Drive/7 - Semestre/Fábrica de Projeto - Ettore/Projeto-FarmLab"
os.environ["MONOLITHFARM_DATA_DIR"] = "/content/drive/My Drive/7 - Semestre/Fábrica de Projeto - Ettore/Projeto-FarmLab/data"
os.environ["MONOLITHFARM_OUTPUT_DIR"] = "/content/drive/My Drive/7 - Semestre/Fábrica de Projeto - Ettore/Projeto-FarmLab/notebook_outputs/complete_ndvi"
```

## Instalacao Automatica

No Colab, o notebook usa `MONOLITHFARM_AUTO_INSTALL=1` por padrao e tenta instalar o projeto e as dependencias automaticamente.

Para controlar a instalacao manualmente:

```python
import os
os.environ["MONOLITHFARM_AUTO_INSTALL"] = "0"
```

Depois instale manualmente:

```python
!python -m pip install -q -e /content/drive/MyDrive/MonolithFarm
```

## Erros Comuns

- `Nao foi possivel localizar pyproject.toml`:
  - ajuste `COLAB_PROJECT_HINTS`;
  - ou defina `MONOLITHFARM_PROJECT_DIR`.
- `Diretorio de dados nao encontrado`:
  - confirme a existencia da pasta `data/`;
  - ou defina `MONOLITHFARM_DATA_DIR`.
- `scipy` ou `farmlab` nao encontrado:
  - deixe `MONOLITHFARM_AUTO_INSTALL=1`;
  - ou instale manualmente com `pip install -e`.

## Artefatos Gerados

Ao final, o notebook salva os CSVs em `OUTPUT_DIR`, com destaque para:

- `dataset_overview.csv`
- `numeric_profiles.csv`
- `ndvi_outliers.csv`
- `pair_classic_tests.csv`
- `weekly_correlations.csv`
- `decision_summary.csv`
