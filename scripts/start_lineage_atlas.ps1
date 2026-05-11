param(
    [int]$Port = 5173,
    [switch]$RefreshDocs,
    [string]$AuthUser = $env:MONOLITH_ATLAS_USER,
    [string]$AuthPassword = $env:MONOLITH_ATLAS_PASSWORD
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot

function Import-DotEnv {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        return
    }
    Get-Content -LiteralPath $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) {
            return
        }
        $parts = $line.Split("=", 2)
        $key = $parts[0].Trim().Trim([char]0xFEFF)
        $value = $parts[1].Trim().Trim('"').Trim("'")
        if ($key -and -not [Environment]::GetEnvironmentVariable($key, "Process")) {
            [Environment]::SetEnvironmentVariable($key, $value, "Process")
        }
    }
}

Import-DotEnv (Join-Path $root ".env")

if (-not $AuthUser) {
    $AuthUser = $env:MONOLITH_ATLAS_USER
}
if (-not $AuthPassword) {
    $AuthPassword = $env:MONOLITH_ATLAS_PASSWORD
}

$pythonCandidates = @(
    (Join-Path $root ".venv\Scripts\python.exe"),
    (Join-Path $root ".venv_win\Scripts\python.exe"),
    (Join-Path $root ".venv_msys\Scripts\python.exe"),
    (Join-Path $root ".venv\bin\python.exe"),
    (Join-Path $root ".venv\bin\python")
)

$python = $pythonCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $python) {
    $python = "python"
}

$atlasDir = Join-Path $root "lineage_atlas"
if (-not (Test-Path (Join-Path $atlasDir "node_modules"))) {
    Push-Location $atlasDir
    npm install | Out-Host
    Pop-Location
}

if (-not $AuthUser) {
    $AuthUser = "monolito_farm"
}
$env:MONOLITH_ATLAS_USER = $AuthUser

if (-not $AuthPassword -and -not $env:MONOLITH_ATLAS_PASSWORD_HASH) {
    $bytes = New-Object byte[] 18
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    $rng.GetBytes($bytes)
    $rng.Dispose()
    $AuthPassword = ([Convert]::ToBase64String($bytes) -replace '[+/=]', '').Substring(0, 18)
    Write-Host "Senha temporaria para dados privados: $AuthPassword"
}
if ($AuthPassword) {
    $env:MONOLITH_ATLAS_PASSWORD = $AuthPassword
}

& $python "scripts\bootstrap_data.py" | Out-Host

$exportArgs = @("scripts\export_lineage_atlas_data.py")
if ($RefreshDocs) {
    $exportArgs += "--refresh-docs"
}
& $python $exportArgs | Out-Host

Push-Location $atlasDir
Write-Host "Abrindo MonolithFarm Atlas NDVI em http://127.0.0.1:$Port"
npm run dev -- --port $Port
Pop-Location
