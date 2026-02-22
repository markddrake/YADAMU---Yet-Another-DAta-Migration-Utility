<#
.SYNOPSIS
    YADAMU deployment script - copies development files to repo staging folder
    using git's .gitignore engine to determine which files to include.

.DESCRIPTION
    Replaces the old 559-line deploy.bat with a .gitignore-driven approach.
    
    Phase 1: Uses git ls-files with a temporary GIT_DIR to list all non-ignored
             files in the development tree, then copies them to the destination.
    Phase 2: Copies Docker build files from C:\Docker\Build\YADAMU with path
             remapping into docker/rdbms/*/docker/

    The .gitignore file is the single source of truth for what gets deployed.
    When you're ready to work directly in the repo, the same .gitignore works
    as-is.

.PARAMETER Source
    Development source folder (default: C:\Development\YADAMU)

.PARAMETER Destination  
    Deployment target folder (default: C:\Deployment\YADAMU2)

.PARAMETER DockerBuildSource
    Docker build files source (default: C:\Docker\Build\YADAMU)

.PARAMETER GitIgnoreFile
    Path to .gitignore file (default: .gitignore next to this script)

.PARAMETER Clean
    Remove existing files in destination before copying (preserves .git)

.PARAMETER DryRun
    List files that would be copied without actually copying

.EXAMPLE
    # Test run - deploy to YADAMU2 for comparison
    .\deploy.ps1

    # Deploy to real repo after validation
    .\deploy.ps1 -Destination "C:\Deployment\YADAMU"

    # See what would be copied without copying
    .\deploy.ps1 -DryRun

    # Full clean deploy
    .\deploy.ps1 -Clean
#>

param(
    [string]$Source          = "C:\Development\YADAMU",
    [string]$Destination     = "C:\Deployment\YADAMU2",
    [string]$DockerBuildSource = "C:\Docker\Build\YADAMU",
    [string]$GitIgnoreFile   = (Join-Path $PSScriptRoot ".gitignore"),
    [switch]$Clean,
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Validate inputs
# ---------------------------------------------------------------------------

if (-not (Test-Path $Source)) {
    Write-Error "Source folder not found: $Source"
    exit 1
}

if (-not (Test-Path $GitIgnoreFile)) {
    Write-Error ".gitignore not found: $GitIgnoreFile"
    exit 1
}

# Check git is available
try { git --version | Out-Null } catch {
    Write-Error "git is not available on PATH"
    exit 1
}

# ---------------------------------------------------------------------------
# Phase 0: Clean destination (preserve .git if present)
# ---------------------------------------------------------------------------

if ($Clean -and (Test-Path $Destination) -and -not $DryRun) {
    Write-Host "Cleaning $Destination (preserving .git)..." -ForegroundColor Yellow
    Get-ChildItem -Path $Destination -Exclude ".git" | Remove-Item -Recurse -Force
}

if (-not (Test-Path $Destination) -and -not $DryRun) {
    New-Item -ItemType Directory -Path $Destination -Force | Out-Null
}

# ---------------------------------------------------------------------------
# Phase 1: Copy development tree (filtered by .gitignore)
# ---------------------------------------------------------------------------
# Uses --exclude-from (not --exclude-standard) so we apply ONLY our own
# .gitignore rules, ignoring any stray .gitignore files that may exist
# inside the development tree.
# ---------------------------------------------------------------------------

Write-Host "Phase 1: Scanning $Source using .gitignore rules..." -ForegroundColor Cyan

$TempGit = Join-Path $env:TEMP "yadamu-deploy-git-$(Get-Random)"
$origGitDir = $env:GIT_DIR
$origWorkTree = $env:GIT_WORK_TREE

try {
    $env:GIT_DIR = $TempGit
    $env:GIT_WORK_TREE = $Source

    # Initialize temp repo
    git init --quiet 2>$null

    # Get all files not matched by our .gitignore
    # --others = untracked (all files, since repo has no commits)
    # --exclude-from = apply only our rules, not any in-tree .gitignore files
    $files = git ls-files --others --exclude-from="$GitIgnoreFile" | Where-Object { $_ -ne "" }

    Write-Host "  Found $($files.Count) files to deploy" -ForegroundColor Green

    $copied = 0
    foreach ($file in $files) {
        $srcPath = Join-Path $Source $file
        $dstPath = Join-Path $Destination $file
        $dstDir  = Split-Path $dstPath -Parent

        if ($DryRun) {
            Write-Host "  [DRY RUN] $file"
        } else {
            if (-not (Test-Path $dstDir)) {
                New-Item -ItemType Directory -Path $dstDir -Force | Out-Null
            }
            Copy-Item $srcPath $dstPath -Force
        }
        $copied++
    }

    Write-Host "  Phase 1 complete: $copied files" -ForegroundColor Green

} finally {
    if ($origGitDir) { $env:GIT_DIR = $origGitDir } else { Remove-Item Env:\GIT_DIR -ErrorAction SilentlyContinue }
    if ($origWorkTree) { $env:GIT_WORK_TREE = $origWorkTree } else { Remove-Item Env:\GIT_WORK_TREE -ErrorAction SilentlyContinue }
    if (Test-Path $TempGit) { Remove-Item -Recurse -Force $TempGit }
}

# ---------------------------------------------------------------------------
# Phase 2: Docker build files (path remapping from C:\Docker\Build\YADAMU)
# ---------------------------------------------------------------------------
# These files live outside the development tree and get remapped into
# docker/rdbms/{db}/docker/{platform}/{version}/ in the repo.
#
# If you later move these into C:\Development\YADAMU\docker, you can
# delete this entire section — Phase 1 will pick them up automatically.
# ---------------------------------------------------------------------------

Write-Host "Phase 2: Docker build files from $DockerBuildSource..." -ForegroundColor Cyan

if (-not (Test-Path $DockerBuildSource)) {
    Write-Host "  WARNING: Docker build source not found, skipping Phase 2" -ForegroundColor Yellow
} else {

    # Mapping table: source subfolder -> destination subfolder (under docker/)
    $dockerMappings = @{
        "windows"              = "windows"
        "mariadb\windows\10"   = "rdbms\mariadb\docker\windows\10"
        "mariadb\windows\11"   = "rdbms\mariadb\docker\windows\11"
        "mongodb\windows\05"   = "rdbms\mongodb\docker\windows\05"
        "mongodb\windows\07"   = "rdbms\mongodb\docker\windows\07"
        "MsSQL\windows\2014"   = "rdbms\mssql\docker\windows\2014"
        "MsSQL\windows\2017"   = "rdbms\mssql\docker\windows\2017"
        "MsSQL\windows\2019"   = "rdbms\mssql\docker\windows\2019"
        "MsSQL\windows\2022"   = "rdbms\mssql\docker\windows\2022"
        "mysql\windows\8.0"    = "rdbms\mysql\docker\windows\8.0"
        "mysql\windows\8.1"    = "rdbms\mysql\docker\windows\8.1"
        "teradata\linux\17"    = "rdbms\teradata\docker\linux\17"
        "oracle\windows\11.2.0.4" = "rdbms\oracle\docker\windows\11.2"
        "oracle\windows\12.2"     = "rdbms\oracle\docker\windows\12.2"
        "oracle\windows\18.3"     = "rdbms\oracle\docker\windows\18.3"
        "oracle\windows\19.3"     = "rdbms\oracle\docker\windows\19.3"
        "oracle\windows\21.3"     = "rdbms\oracle\docker\windows\21.3"
        "postgres\windows\14"     = "rdbms\postgres\docker\windows\14"
        "postgres\windows\16"     = "rdbms\postgres\docker\windows\16"
    }

    $dockerCopied = 0
    foreach ($mapping in $dockerMappings.GetEnumerator()) {
        $srcDir = Join-Path $DockerBuildSource $mapping.Key
        $dstDir = Join-Path $Destination "docker\$($mapping.Value)"

        if (-not (Test-Path $srcDir)) {
            Write-Host "  SKIP (not found): $srcDir" -ForegroundColor DarkYellow
            continue
        }

        $dockerFiles = Get-ChildItem -Path $srcDir -File
        foreach ($f in $dockerFiles) {
            if ($DryRun) {
                Write-Host "  [DRY RUN] docker\$($mapping.Value)\$($f.Name)"
            } else {
                if (-not (Test-Path $dstDir)) {
                    New-Item -ItemType Directory -Path $dstDir -Force | Out-Null
                }
                Copy-Item $f.FullName (Join-Path $dstDir $f.Name) -Force
            }
            $dockerCopied++
        }
    }

    Write-Host "  Phase 2 complete: $dockerCopied files" -ForegroundColor Green
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

Write-Host ""
if ($DryRun) {
    Write-Host "DRY RUN complete. No files were copied." -ForegroundColor Yellow
    Write-Host "Run without -DryRun to perform the actual deployment." 
} else {
    Write-Host "Deployment complete to $Destination" -ForegroundColor Green
    Write-Host "Next step: compare with known-good deployment:"
    Write-Host '  # Files only in YADAMU2 (extras):'
    Write-Host '  $old = Get-ChildItem C:\Deployment\YADAMU -Recurse -File | ForEach-Object { $_.FullName.Replace("C:\Deployment\YADAMU\","") }'
    Write-Host '  $new = Get-ChildItem C:\Deployment\YADAMU2 -Recurse -File | ForEach-Object { $_.FullName.Replace("C:\Deployment\YADAMU2\","") }'
    Write-Host '  Compare-Object $old $new'
}
