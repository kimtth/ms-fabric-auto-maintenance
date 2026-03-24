<#
.SYNOPSIS
    Updates the semantic model definition in a remote Fabric workspace.

.DESCRIPTION
    Reads TMDL files from fabric/semantic-model/definition/, base64-encodes them,
    and POSTs to the Fabric Items API updateDefinition endpoint.

    Requires: az CLI authenticated via `az login`.

.PARAMETER WorkspaceName
    Display name of the target Fabric workspace.

.PARAMETER ModelName
    Display name of the semantic model item to update.

.EXAMPLE
    .\update-semantic-model.ps1 -WorkspaceName "MyWorkspace" -ModelName "MySalesModel"
#>
param(
    [Parameter(Mandatory)]
    [string]$WorkspaceName,

    [Parameter(Mandatory)]
    [string]$ModelName
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$FABRIC_API = "https://api.fabric.microsoft.com"
$DEFINITION_ROOT = "$PSScriptRoot\..\fabric\semantic-model"

function Convert-Base64File([string]$path) {
    [Convert]::ToBase64String([System.IO.File]::ReadAllBytes($path))
}

# ── 1. Resolve workspace ID ──────────────────────────────────────────────────
Write-Host "Resolving workspace '$WorkspaceName'..."
$WS_ID = az rest --method get `
    --resource $FABRIC_API `
    --url "$FABRIC_API/v1/workspaces" `
    --query "value[?displayName=='$WorkspaceName'] | [0].id" `
    --output tsv

if (-not $WS_ID) {
    Write-Error "Workspace '$WorkspaceName' not found. Run 'az login' and verify the name."
    exit 1
}
Write-Host "  Workspace ID: $WS_ID"

# ── 2. Resolve semantic model ID ─────────────────────────────────────────────
Write-Host "Resolving semantic model '$ModelName'..."
$MODEL_ID = az rest --method get `
    --resource $FABRIC_API `
    --url "$FABRIC_API/v1/workspaces/$WS_ID/items?type=SemanticModel" `
    --query "value[?displayName=='$ModelName'] | [0].id" `
    --output tsv

if (-not $MODEL_ID) {
    Write-Error "Semantic model '$ModelName' not found in workspace '$WorkspaceName'."
    exit 1
}
Write-Host "  Semantic Model ID: $MODEL_ID"

# ── 3. Encode all TMDL parts ─────────────────────────────────────────────────
Write-Host "Encoding TMDL parts..."

$parts = [System.Collections.Generic.List[hashtable]]::new()

# Root files
foreach ($file in @("definition.pbism")) {
    $fullPath = Join-Path $DEFINITION_ROOT $file
    $parts.Add(@{
        path        = $file
        payload     = Convert-Base64File $fullPath
        payloadType = "InlineBase64"
    })
}

# definition/*.tmdl files
$defDir = Join-Path $DEFINITION_ROOT "definition"
foreach ($file in Get-ChildItem -Path $defDir -Filter "*.tmdl" -File) {
    $parts.Add(@{
        path        = "definition/$($file.Name)"
        payload     = Convert-Base64File $file.FullName
        payloadType = "InlineBase64"
    })
}

# definition/tables/*.tmdl files
$tablesDir = Join-Path $defDir "tables"
foreach ($file in Get-ChildItem -Path $tablesDir -Filter "*.tmdl" -File) {
    $parts.Add(@{
        path        = "definition/tables/$($file.Name)"
        payload     = Convert-Base64File $file.FullName
        payloadType = "InlineBase64"
    })
}

Write-Host "  $($parts.Count) parts encoded:"
$parts | ForEach-Object { Write-Host "    - $($_.path)" }

# ── 4. Build and POST the updateDefinition payload ───────────────────────────
$body = @{
    definition = @{
        format = "TMDL"
        parts  = $parts
    }
} | ConvertTo-Json -Depth 10

$tmpBody = Join-Path $env:TEMP "sm-update-body.json"
$body | Set-Content -Path $tmpBody -Encoding UTF8

Write-Host "`nPosting updateDefinition to workspace $WS_ID / model $MODEL_ID..."

$response = az rest --method post --verbose `
    --resource $FABRIC_API `
    --url "$FABRIC_API/v1/workspaces/$WS_ID/semanticModels/$MODEL_ID/updateDefinition" `
    --headers "Content-Type=application/json" `
    --body "@$tmpBody" `
    2>&1

# ── 5. Poll LRO if 202 Accepted ──────────────────────────────────────────────
$opLine = $response | Select-String "'x-ms-operation-id': '([^']+)'"
if ($opLine) {
    $opId = $opLine.Matches.Groups[1].Value
    $pollUrl = "$FABRIC_API/v1/operations/$opId"
    Write-Host "LRO started (Operation-Id: $opId). Polling..."

    do {
        Start-Sleep -Seconds 5
        $status = az rest --method get `
            --resource $FABRIC_API `
            --url $pollUrl `
            --output json | ConvertFrom-Json
        Write-Host "  Status: $($status.status)"
    } while ($status.status -in @("Running", "NotStarted"))

    if ($status.status -eq "Succeeded") {
        Write-Host "`nSemantic model '$ModelName' updated successfully."
    }
    else {
        Write-Error "Update failed. Status: $($status.status). Error: $($status.error.message)"
    }
}
else {
    # Synchronous success (no LRO)
    Write-Host "`nSemantic model '$ModelName' updated successfully (synchronous)."
}

Remove-Item -Path $tmpBody -Force -ErrorAction SilentlyContinue
