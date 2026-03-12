param(
    [string]$DataDir = "C:\Users\Morgado\Downloads\FarmLab",
    [string]$DbPath = "storage\monolithfarm.duckdb",
    [int]$Port = 8501,
    [switch]$Refresh
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $DataDir)) {
    throw "O diretório de dados não foi encontrado: $DataDir"
}

if (-not (Test-Path ".\.venv\Scripts\python.exe")) {
    Write-Host "Criando ambiente virtual..."
    python -m venv .venv
}

$python = Resolve-Path ".\.venv\Scripts\python.exe"

Write-Host "Instalando dependencias do projeto..."
& $python -m pip install -e . | Out-Host

if ($Refresh -or -not (Test-Path $DbPath)) {
    Write-Host "Atualizando banco DuckDB..."
    & $python -m farmlab.database --data-dir $DataDir --db-path $DbPath | Out-Host
}

Write-Host "Abrindo dashboard em http://127.0.0.1:$Port"
& $python -m streamlit run streamlit_app.py --server.address 127.0.0.1 --server.port $Port
