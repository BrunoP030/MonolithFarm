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

    $localDataDir = Join-Path (Get-Location) "data"
    if (Test-Path $localDataDir) {
        return $localDataDir
    }

    $localDataDir = Join-Path (Get-Location) "FarmLab"
    if (Test-Path $localDataDir) {
        return $localDataDir
    }

    return "C:\Users\Morgado\Downloads\FarmLab"
}

function Resolve-VenvPython {
    $isWindowsHost = ($env:OS -eq "Windows_NT") -or ([System.IO.Path]::DirectorySeparatorChar -eq "\")
    if ($isWindowsHost) {
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
    & $PythonPath -m pip --version | Out-Null
    $hasPip = ($LASTEXITCODE -eq 0)

    if ($hasPip) {
        & $PythonPath -m pip install -e . | Out-Host
        if ($LASTEXITCODE -ne 0) {
            throw "Falha ao instalar dependencias com pip."
        }
    }
    else {
        if (Get-Command uv -ErrorAction SilentlyContinue) {
            uv pip install --python $PythonPath -e . | Out-Host
            if ($LASTEXITCODE -ne 0) {
                throw "Falha ao instalar dependencias com uv."
            }
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
    & $python -m dashboard.database --data-dir $DataDir --db-path $DbPath | Out-Host
}

Write-Host "Abrindo dashboard em http://127.0.0.1:$Port"
& $python -m streamlit run dashboard/app.py --server.address 127.0.0.1 --server.port $Port
