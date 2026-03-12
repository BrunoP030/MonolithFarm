param(
    [string]$DataDir = "",
    [string]$DbPath = "storage/monolithfarm.duckdb",
    [int]$Port = 8501,
    [switch]$Refresh
)

$ErrorActionPreference = "Stop"

function Resolve-DefaultDataDir {
    if ($env:MONOLITHFARM_DATA_DIR) {
        return $env:MONOLITHFARM_DATA_DIR
    }

    $localDataDir = Join-Path (Get-Location) "FarmLab"
    if (Test-Path $localDataDir) {
        return $localDataDir
    }

    return "C:\Users\Morgado\Downloads\FarmLab"
}

function Resolve-VenvPython {
    if ($IsWindows) {
        return Join-Path (Get-Location) ".venv\Scripts\python.exe"
    }

    return Join-Path (Get-Location) ".venv/bin/python"
}

function Ensure-Environment {
    param([string]$PythonPath)

    if (-not (Test-Path $PythonPath)) {
        if (Get-Command uv -ErrorAction SilentlyContinue) {
            Write-Host "Criando ambiente virtual com uv..."
            uv venv .venv | Out-Host
        }
        else {
            Write-Host "Criando ambiente virtual com python -m venv..."
            python -m venv .venv | Out-Host
        }
    }

    if (-not (Test-Path $PythonPath)) {
        throw "Nao foi possivel criar/identificar o Python em $PythonPath"
    }

    Write-Host "Instalando dependencias do projeto..."
    try {
        & $PythonPath -m pip --version | Out-Null
        & $PythonPath -m pip install -e . | Out-Host
    }
    catch {
        if (Get-Command uv -ErrorAction SilentlyContinue) {
            uv pip install --python $PythonPath -e . | Out-Host
        }
        else {
            throw "pip nao esta disponivel no ambiente virtual e uv nao foi encontrado."
        }
    }
}

if ([string]::IsNullOrWhiteSpace($DataDir)) {
    $DataDir = Resolve-DefaultDataDir
}

if (-not (Test-Path $DataDir)) {
    throw "O diretorio de dados nao foi encontrado: $DataDir"
}

$python = Resolve-VenvPython
Ensure-Environment -PythonPath $python

if ($Refresh -or -not (Test-Path $DbPath)) {
    Write-Host "Atualizando banco DuckDB..."
    & $python -m farmlab.database --data-dir $DataDir --db-path $DbPath | Out-Host
}

Write-Host "Abrindo dashboard em http://127.0.0.1:$Port"
& $python -m streamlit run streamlit_app.py --server.address 127.0.0.1 --server.port $Port
