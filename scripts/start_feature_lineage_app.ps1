param(
    [int]$Port = 5173,
    [switch]$RefreshDocs
)

$ErrorActionPreference = "Stop"

$atlasScript = Join-Path $PSScriptRoot "start_lineage_atlas.ps1"
if (-not (Test-Path $atlasScript)) {
    throw "Script do Atlas nao encontrado em $atlasScript"
}

Write-Host "start_feature_lineage_app.ps1 agora redireciona para o MonolithFarm Atlas NDVI em React."
if ($RefreshDocs) {
    & $atlasScript -Port $Port -RefreshDocs
}
else {
    & $atlasScript -Port $Port
}
