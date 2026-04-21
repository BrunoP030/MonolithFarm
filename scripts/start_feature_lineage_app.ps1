param(
    [int]$Port = 8502
)

$ErrorActionPreference = "Stop"

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

$python = Resolve-VenvPython
Ensure-Environment -PythonPath $python

Write-Host "Abrindo app de auditoria NDVI em http://127.0.0.1:$Port"
& $python -m streamlit run dashboard/feature_lineage_app.py --server.address 127.0.0.1 --server.port $Port
