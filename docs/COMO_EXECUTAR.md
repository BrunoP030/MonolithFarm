# Como Executar o Projeto

Este guia reúne o passo a passo necessário para rodar o projeto após baixar o repositório.

Para uma conferência mais direta de ambiente em outra máquina:

- [PRIMEIRO_USO_FACULDADE.md](PRIMEIRO_USO_FACULDADE.md)

## Pré-requisitos

Antes da execução, é necessário ter:

- `Python 3.11` instalado e disponível no terminal;
- acesso ao pacote de dados `FarmLab`;
- PowerShell habilitado para execução local.

## Estrutura esperada dos dados

Por padrão, o projeto procura os dados no caminho:

```text
C:\Users\Morgado\Downloads\FarmLab
```

Se o diretório estiver em outro local, o caminho poderá ser informado manualmente no comando de execução.

## Execução rápida

Após clonar ou baixar o repositório, abrir um terminal dentro da pasta do projeto e executar:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh
```

Esse comando realiza as seguintes etapas:

1. cria o ambiente virtual `.venv`, caso ainda não exista;
2. instala as dependências do projeto;
3. atualiza o banco local `DuckDB`;
4. inicia o painel `Streamlit`.

Depois disso, o painel pode ser acessado em:

```text
http://127.0.0.1:8501
```

## Execução rápida com diretório de dados personalizado

Se os dados não estiverem no caminho padrão:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -DataDir "D:\Dados\FarmLab"
```

## Execução rápida em outra porta

Se a porta `8501` estiver ocupada:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_dashboard.ps1 -Refresh -Port 8502
```

Nesse caso, o acesso será feito por:

```text
http://127.0.0.1:8502
```

## Execução manual

Caso seja necessário executar o projeto manualmente, o fluxo é o seguinte:

1. criar o ambiente virtual:

```powershell
python -m venv .venv
```

2. ativar o ambiente:

```powershell
.\.venv\Scripts\Activate.ps1
```

3. instalar o projeto:

```powershell
pip install -e .
```

4. materializar o banco local:

```powershell
monolithfarm-ingest --data-dir "C:\Users\Morgado\Downloads\FarmLab" --db-path "storage\monolithfarm.duckdb"
```

5. iniciar o painel:

```powershell
streamlit run streamlit_app.py
```

## Arquivos importantes para a execução

- [scripts/start_dashboard.ps1](../scripts/start_dashboard.ps1): inicialização assistida
- [streamlit_app.py](../streamlit_app.py): painel principal
- [storage/monolithfarm.duckdb](../storage/monolithfarm.duckdb): banco local gerado após ingestão

## Observações

- o terminal deve permanecer aberto enquanto o painel estiver em uso;
- ao fechar o terminal, o servidor local do `Streamlit` será encerrado;
- a interface também permite atualizar o banco local diretamente pela barra lateral.
