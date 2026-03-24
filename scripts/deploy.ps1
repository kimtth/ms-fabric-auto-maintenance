<#
.SYNOPSIS
    Deploy the Predictive Maintenance Fabric platform end-to-end.

.DESCRIPTION
    Creates workspace, lakehouses, notebooks, semantic model, report, and pipeline.
    All environment-specific IDs are resolved dynamically — no hardcoded GUIDs.

    Notebooks still require a one-time manual lakehouse attachment in the
    Fabric portal before they can execute (see FABRIC.md § Lessons Learned).

.PARAMETER WorkspaceName
    Name for the Fabric workspace (created if not found).

.PARAMETER CapacityId
    Fabric capacity ID to assign. Leave empty to skip assignment.

.PARAMETER Location
    Azure region reported by the capacity (for logging only).

.PARAMETER SqlEndpointServer
    SQL Analytics Endpoint server hostname for the semantic model's M expressions.
    Find it in Fabric portal: Lakehouse → SQL Endpoint → Settings → SQL connection string.
    Example: abc123xyz.datawarehouse.fabric.microsoft.com

.EXAMPLE
    .\scripts\deploy.ps1 -WorkspaceName "my-workspace" -CapacityId "<guid>" -SqlEndpointServer "abc123.datawarehouse.fabric.microsoft.com"
#>
param(
    [Parameter(Mandatory)]
    [string]$WorkspaceName,

    [string]$CapacityId = "",

    [string]$Location = "",

    [string]$SqlEndpointServer = "YOUR_SQL_ENDPOINT_SERVER"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
$FabricApi = "https://api.fabric.microsoft.com"

function Invoke-FabricApi {
    param(
        [string]$Method,
        [string]$Url,
        [string]$BodyFile = $null
    )
    $args_ = @("rest", "--method", $Method, "--resource", $FabricApi, "--url", "$FabricApi$Url")
    if ($BodyFile) {
        $args_ += @("--body", "@$BodyFile", "--headers", "Content-Type=application/json")
    }
    $result = & az @args_ 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "az rest $Method $Url failed (exit $LASTEXITCODE)"
    }
    if ($result) { return $result | ConvertFrom-Json }
    return $null
}

function Invoke-FabricApiRaw {
    param([string]$Method, [string]$Url, [string]$BodyFile = $null)
    $args_ = @("rest", "--method", $Method, "--resource", $FabricApi, "--url", "$FabricApi$Url", "--verbose")
    if ($BodyFile) {
        $args_ += @("--body", "@$BodyFile", "--headers", "Content-Type=application/json")
    }
    return & az @args_ 2>&1 | Out-String
}

function Wait-FabricLro {
    param([string]$VerboseOutput, [int]$PollSeconds = 15, [int]$MaxAttempts = 20)
    $opMatch = [regex]::Match($VerboseOutput, "x-ms-operation-id': '([a-f0-9-]+)'")
    if (-not $opMatch.Success) { return $null }
    $opId = $opMatch.Groups[1].Value
    for ($i = 0; $i -lt $MaxAttempts; $i++) {
        Start-Sleep -Seconds $PollSeconds
        $status = Invoke-FabricApi -Method GET -Url "/v1/operations/$opId"
        if ($status.status -eq "Succeeded") { return $status }
        if ($status.status -eq "Failed") {
            $msg = if ($status.error) { $status.error.message } else { "unknown" }
            throw "LRO $opId failed: $msg"
        }
    }
    throw "LRO $opId timed out after $($PollSeconds * $MaxAttempts)s"
}

function B64([string]$text) {
    [Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($text))
}

function Find-ItemId {
    param([string]$WorkspaceId, [string]$Type, [string]$DisplayName)
    $items = Invoke-FabricApi -Method GET -Url "/v1/workspaces/$WorkspaceId/items?type=$Type"
    $match = $items.value | Where-Object { $_.displayName -eq $DisplayName }
    if ($match) { return $match.id }
    return $null
}

function Write-Step([string]$msg) { Write-Host "`n>>> $msg" -ForegroundColor Cyan }

# ─────────────────────────────────────────────
# Pre-flight check
# ─────────────────────────────────────────────
Write-Step "Pre-flight: verifying az login"
$account = az account show 2>$null | ConvertFrom-Json
if (-not $account) { throw "Not logged in. Run: az login" }
Write-Host "  Logged in as $($account.user.name) (tenant $($account.tenantId))"

$ScriptRoot = $PSScriptRoot
$RepoRoot   = Split-Path $ScriptRoot -Parent

# ─────────────────────────────────────────────
# Step 1 — Workspace
# ─────────────────────────────────────────────
Write-Step "Step 1: Create or find workspace '$WorkspaceName'"
$workspaces = Invoke-FabricApi -Method GET -Url "/v1/workspaces"
$ws = $workspaces.value | Where-Object { $_.displayName -eq $WorkspaceName }
if ($ws) {
    $WS_ID = $ws.id
    Write-Host "  Found existing workspace: $WS_ID"
} else {
    $body = @{ displayName = $WorkspaceName } | ConvertTo-Json -Compress
    [System.IO.File]::WriteAllText("$env:TEMP\_ws.json", $body)
    $created = Invoke-FabricApi -Method POST -Url "/v1/workspaces" -BodyFile "$env:TEMP\_ws.json"
    $WS_ID = $created.id
    Write-Host "  Created workspace: $WS_ID"
}

# ─────────────────────────────────────────────
# Step 2 — Capacity assignment
# ─────────────────────────────────────────────
if ($CapacityId) {
    Write-Step "Step 2: Assign capacity $CapacityId"
    $capBody = @{ capacityId = $CapacityId } | ConvertTo-Json -Compress
    [System.IO.File]::WriteAllText("$env:TEMP\_cap.json", $capBody)
    Invoke-FabricApi -Method POST -Url "/v1/workspaces/$WS_ID/assignToCapacity" -BodyFile "$env:TEMP\_cap.json"
    Write-Host "  Capacity assigned"
} else {
    Write-Host "`n>>> Step 2: Skipping capacity assignment (no CapacityId provided)"
}

# ─────────────────────────────────────────────
# Step 3 — Lakehouses (Bronze / Silver / Gold)
# ─────────────────────────────────────────────
Write-Step "Step 3: Create lakehouses"
$lakehouses = @{}
foreach ($name in @("automotive_bronze", "automotive_silver", "automotive_gold")) {
    $existing = Find-ItemId $WS_ID "Lakehouse" $name
    if ($existing) {
        Write-Host "  $name already exists: $existing"
        $lakehouses[$name] = $existing
    } else {
        $body = @{ displayName = $name; type = "Lakehouse" } | ConvertTo-Json -Compress
        [System.IO.File]::WriteAllText("$env:TEMP\_lh.json", $body)
        $lh = Invoke-FabricApi -Method POST -Url "/v1/workspaces/$WS_ID/items" -BodyFile "$env:TEMP\_lh.json"
        $lakehouses[$name] = $lh.id
        Write-Host "  Created $name : $($lh.id)"
    }
}
$BRONZE_LH = $lakehouses["automotive_bronze"]
$SILVER_LH = $lakehouses["automotive_silver"]
$GOLD_LH   = $lakehouses["automotive_gold"]

# ─────────────────────────────────────────────
# Step 4a — Notebook folder
# ─────────────────────────────────────────────
Write-Step "Step 4a: Create or find 'Notebook' workspace folder"
$folders = Invoke-FabricApi -Method GET -Url "/v1/workspaces/$WS_ID/folders"
$nbFolder = $folders.value | Where-Object { $_.displayName -eq "Notebook" }
if ($nbFolder) {
    $NOTEBOOK_FOLDER_ID = $nbFolder.id
    Write-Host "  Notebook folder already exists: $NOTEBOOK_FOLDER_ID"
} else {
    $folderBody = @{ displayName = "Notebook" } | ConvertTo-Json -Compress
    [System.IO.File]::WriteAllText("$env:TEMP\_folder.json", $folderBody)
    $created = Invoke-FabricApi -Method POST -Url "/v1/workspaces/$WS_ID/folders" -BodyFile "$env:TEMP\_folder.json"
    $NOTEBOOK_FOLDER_ID = $created.id
    Write-Host "  Created Notebook folder: $NOTEBOOK_FOLDER_ID"
}

# ─────────────────────────────────────────────
# Step 4 — Notebooks (upload from /notebooks/*.ipynb)
# ─────────────────────────────────────────────
Write-Step "Step 4: Upload notebooks"

function Convert-NotebookToIpynb {
    param([string]$VscPath)
    $lines = Get-Content $VscPath -Raw
    $cells = @()
    # Split on VSCode.Cell boundaries
    $cellBlocks = [regex]::Split($lines, '(?=<VSCode\.Cell )')
    foreach ($block in $cellBlocks) {
        $block = $block.Trim()
        if (-not $block) { continue }
        $headerMatch = [regex]::Match($block, '<VSCode\.Cell id="[^"]*" language="([^"]+)">')
        if (-not $headerMatch.Success) { continue }
        $lang = $headerMatch.Groups[1].Value
        $cellType = if ($lang -eq "markdown") { "markdown" } else { "code" }
        $content = $block -replace '<VSCode\.Cell[^>]*>', '' -replace '</VSCode\.Cell>', ''
        $content = $content.Trim()
        $sourceLines = ($content -split "`n") | ForEach-Object { "$_`n" }
        if ($sourceLines.Count -gt 0) {
            $sourceLines[-1] = $sourceLines[-1].TrimEnd("`n")
        }
        $cell = @{
            cell_type = $cellType
            source    = @($sourceLines)
            metadata  = @{}
        }
        if ($cellType -eq "code") {
            $cell["outputs"] = @()
            $cell["execution_count"] = $null
        }
        $cells += $cell
    }
    return @{
        nbformat       = 4
        nbformat_minor = 5
        metadata       = @{
            language_info = @{ name = "python" }
            kernel_info   = @{ name = "synapse_pyspark" }
        }
        cells          = $cells
    }
}

$notebookIds = @{}
$notebookFiles = @(
    @{ name = "01_bronze_ingest";     file = "01_bronze_ingest.ipynb" }
    @{ name = "02_silver_transform";  file = "02_silver_transform.ipynb" }
    @{ name = "03_gold_aggregate";    file = "03_gold_aggregate.ipynb" }
)

foreach ($nb in $notebookFiles) {
    $existing = Find-ItemId $WS_ID "Notebook" $nb.name
    if ($existing) {
        Write-Host "  $($nb.name) already exists: $existing"
        $notebookIds[$nb.name] = $existing
        continue
    }

    $vscPath = Join-Path $RepoRoot "notebooks" $nb.file
    if (-not (Test-Path $vscPath)) { throw "Notebook not found: $vscPath" }

    # Read notebook source and patch lakehouse references
    $nbContent = Get-Content $vscPath -Raw

    # Replace hardcoded lakehouse paths with dynamic ones
    # Silver reads from Bronze
    $nbContent = $nbContent -replace 'abfss://[a-f0-9-]+@onelake\.dfs\.fabric\.microsoft\.com/[a-f0-9-]+(?=/Tables/raw_)',
        "abfss://$WS_ID@onelake.dfs.fabric.microsoft.com/$BRONZE_LH"
    # Gold reads from Silver
    $nbContent = $nbContent -replace 'abfss://[a-f0-9-]+@onelake\.dfs\.fabric\.microsoft\.com/[a-f0-9-]+(?=/Tables/(dim_|fact_))',
        "abfss://$WS_ID@onelake.dfs.fabric.microsoft.com/$SILVER_LH"

    # Write patched content to temp file and convert
    $patchedPath = "$env:TEMP\_nb_patched.ipynb"
    [System.IO.File]::WriteAllText($patchedPath, $nbContent, [System.Text.Encoding]::UTF8)

    $ipynb = Convert-NotebookToIpynb -VscPath $patchedPath
    $ipynbJson = $ipynb | ConvertTo-Json -Depth 10 -Compress
    $ipynbB64 = B64 $ipynbJson

    $body = @{
        displayName = $nb.name
        type        = "Notebook"
        definition  = @{
            format = "ipynb"
            parts  = @(
                @{ path = "notebook-content.ipynb"; payload = $ipynbB64; payloadType = "InlineBase64" }
            )
        }
    } | ConvertTo-Json -Depth 10 -Compress

    [System.IO.File]::WriteAllText("$env:TEMP\_nb_body.json", $body, [System.Text.Encoding]::UTF8)

    $v = Invoke-FabricApiRaw -Method POST -Url "/v1/workspaces/$WS_ID/items" -BodyFile "$env:TEMP\_nb_body.json"
    Wait-FabricLro $v -PollSeconds 10 | Out-Null

    $nbId = Find-ItemId $WS_ID "Notebook" $nb.name
    $notebookIds[$nb.name] = $nbId
    Write-Host "  Created $($nb.name): $nbId"

    # Move into the Notebook folder
    $moveBody = @{ folderId = $NOTEBOOK_FOLDER_ID } | ConvertTo-Json -Compress
    [System.IO.File]::WriteAllText("$env:TEMP\_nb_move.json", $moveBody)
    Invoke-FabricApi -Method PATCH -Url "/v1/workspaces/$WS_ID/items/$nbId" -BodyFile "$env:TEMP\_nb_move.json" | Out-Null
    Write-Host "  Moved $($nb.name) to Notebook folder"
}

$BRONZE_NB = $notebookIds["01_bronze_ingest"]
$SILVER_NB = $notebookIds["02_silver_transform"]
$GOLD_NB   = $notebookIds["03_gold_aggregate"]

# ─────────────────────────────────────────────
# Step 5 — Semantic Model (Import Mode on Silver + Gold)
# ─────────────────────────────────────────────
Write-Step "Step 5: Create semantic model"
$SM_NAME = "Automotive Predictive Maintenance"
$SM_ID = Find-ItemId $WS_ID "SemanticModel" $SM_NAME

if ($SM_ID) {
    Write-Host "  Semantic model already exists: $SM_ID"
} else {
    # Read TMDL files from repo and patch the expression
    $smDir = Join-Path $RepoRoot "fabric" "semantic-model"
    $pbism  = Get-Content (Join-Path $smDir "definition.pbism") -Raw
    $dbTmdl = Get-Content (Join-Path $smDir "definition" "database.tmdl") -Raw

    # Patch model.tmdl — replace ref table lines to match our table files
    $modelTmdl = Get-Content (Join-Path $smDir "definition" "model.tmdl") -Raw

    # Build expressions.tmdl with dynamic SQL endpoint parameters.
    $exprTmdl = @"
expression Server = "$SqlEndpointServer" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]

expression SilverDatabase = "automotive_silver" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]

expression GoldDatabase = "automotive_gold" meta [IsParameterQuery=true, Type="Text", IsParameterQueryRequired=true]

"@

    # Read table TMDL files as-is
    $tableDir = Join-Path $smDir "definition" "tables"
    $tableParts = @()
    foreach ($tFile in (Get-ChildItem $tableDir -Filter "*.tmdl")) {
        $content = Get-Content $tFile.FullName -Raw
        $tableParts += @{
            path        = "definition/tables/$($tFile.Name)"
            payload     = (B64 $content)
            payloadType = "InlineBase64"
        }
    }

    $parts = @(
        @{ path = "definition.pbism";             payload = (B64 $pbism);     payloadType = "InlineBase64" }
        @{ path = "definition/database.tmdl";     payload = (B64 $dbTmdl);    payloadType = "InlineBase64" }
        @{ path = "definition/model.tmdl";        payload = (B64 $modelTmdl); payloadType = "InlineBase64" }
        @{ path = "definition/expressions.tmdl";  payload = (B64 $exprTmdl);  payloadType = "InlineBase64" }
    ) + $tableParts

    $body = @{
        displayName = $SM_NAME
        definition  = @{ format = "TMDL"; parts = $parts }
    } | ConvertTo-Json -Depth 10 -Compress

    [System.IO.File]::WriteAllText("$env:TEMP\_sm_body.json", $body, [System.Text.Encoding]::UTF8)

    $v = Invoke-FabricApiRaw -Method POST -Url "/v1/workspaces/$WS_ID/semanticModels" -BodyFile "$env:TEMP\_sm_body.json"
    Wait-FabricLro $v | Out-Null

    $SM_ID = Find-ItemId $WS_ID "SemanticModel" $SM_NAME
    Write-Host "  Created semantic model: $SM_ID"
}

# ─────────────────────────────────────────────
# Step 6 — Power BI Report (PBIR format)
# ─────────────────────────────────────────────
Write-Step "Step 6: Create Power BI report"
$RPT_NAME = "Automotive Predictive Maintenance"
$RPT_ID = Find-ItemId $WS_ID "Report" $RPT_NAME

if ($RPT_ID) {
    Write-Host "  Report already exists: $RPT_ID"
} else {
    # Use build_report.ps1 to generate the payload, but feed it dynamic IDs
    $reportScript = Join-Path $ScriptRoot "build_report.ps1"
    $reportOut = "$env:TEMP\_rpt_body.json"
    & $reportScript -SM_ID $SM_ID -WS_ID $WS_ID -OutFile $reportOut
    Write-Host "  Report payload built ($((Get-Item $reportOut).Length) bytes)"

    $v = Invoke-FabricApiRaw -Method POST -Url "/v1/workspaces/$WS_ID/reports" -BodyFile $reportOut
    Wait-FabricLro $v | Out-Null

    $RPT_ID = Find-ItemId $WS_ID "Report" $RPT_NAME
    Write-Host "  Created report: $RPT_ID"
}

# ─────────────────────────────────────────────
# Step 7 — Data Pipeline (Medallion ETL)
# ─────────────────────────────────────────────
Write-Step "Step 7: Create pipeline"
$PIPE_NAME = "Medallion ETL Pipeline"
$PIPE_ID = Find-ItemId $WS_ID "DataPipeline" $PIPE_NAME

if ($PIPE_ID) {
    Write-Host "  Pipeline already exists: $PIPE_ID"
} else {
    $pipelineContent = @{
        properties = @{
            description = "End-to-end Medallion Architecture pipeline: Bronze -> Silver -> Gold"
            activities  = @(
                @{
                    name       = "Bronze - Ingest Raw Data"
                    type       = "TridentNotebook"
                    dependsOn  = @()
                    policy     = @{ timeout = "0.01:00:00"; retry = 0; retryIntervalInSeconds = 30 }
                    typeProperties = @{ notebookId = $BRONZE_NB; workspaceId = $WS_ID }
                },
                @{
                    name       = "Silver - Transform and Validate"
                    type       = "TridentNotebook"
                    dependsOn  = @( @{ activity = "Bronze - Ingest Raw Data"; dependencyConditions = @("Succeeded") } )
                    policy     = @{ timeout = "0.01:00:00"; retry = 0; retryIntervalInSeconds = 30 }
                    typeProperties = @{ notebookId = $SILVER_NB; workspaceId = $WS_ID }
                },
                @{
                    name       = "Gold - Business Aggregates"
                    type       = "TridentNotebook"
                    dependsOn  = @( @{ activity = "Silver - Transform and Validate"; dependencyConditions = @("Succeeded") } )
                    policy     = @{ timeout = "0.01:00:00"; retry = 0; retryIntervalInSeconds = 30 }
                    typeProperties = @{ notebookId = $GOLD_NB; workspaceId = $WS_ID }
                }
            )
        }
    }

    $pipelineB64 = B64 ($pipelineContent | ConvertTo-Json -Depth 10 -Compress)
    $body = @{
        displayName = $PIPE_NAME
        type        = "DataPipeline"
        definition  = @{
            parts = @(
                @{ path = "pipeline-content.json"; payload = $pipelineB64; payloadType = "InlineBase64" }
            )
        }
    } | ConvertTo-Json -Depth 10 -Compress

    [System.IO.File]::WriteAllText("$env:TEMP\_pipe_body.json", $body, [System.Text.Encoding]::UTF8)
    Invoke-FabricApi -Method POST -Url "/v1/workspaces/$WS_ID/items" -BodyFile "$env:TEMP\_pipe_body.json" | Out-Null

    $PIPE_ID = Find-ItemId $WS_ID "DataPipeline" $PIPE_NAME
    Write-Host "  Created pipeline: $PIPE_ID"
}

# ─────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────
Write-Step "Deployment complete!"
Write-Host ""
Write-Host "  Workspace    : $WorkspaceName ($WS_ID)"
Write-Host "  Bronze LH    : $BRONZE_LH"
Write-Host "  Silver LH    : $SILVER_LH"
Write-Host "  Gold LH      : $GOLD_LH"
Write-Host "  Bronze NB    : $BRONZE_NB"
Write-Host "  Silver NB    : $SILVER_NB"
Write-Host "  Gold NB      : $GOLD_NB"
Write-Host "  Semantic Model: $SM_ID"
Write-Host "  Report       : $RPT_ID"
Write-Host "  Pipeline     : $PIPE_ID"
Write-Host ""
Write-Host "  MANUAL STEPS REQUIRED:" -ForegroundColor Yellow
Write-Host "  1. Open Fabric portal -> Workspace -> each notebook" -ForegroundColor Yellow
Write-Host "  2. Attach the corresponding lakehouse (Bronze/Silver/Gold)" -ForegroundColor Yellow
Write-Host "  3. Run notebooks in order: Bronze -> Silver -> Gold" -ForegroundColor Yellow
Write-Host "  4. After data is populated, open the semantic model and verify tables load" -ForegroundColor Yellow
