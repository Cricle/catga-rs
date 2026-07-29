<#
.SYNOPSIS
    Local podman performance harness for Catga storage benchmarks.

.DESCRIPTION
    Starts the required database services as podman containers, waits for them to
    become healthy, exports the matching CATGA_*_URL variables, and runs the manual
    release benchmarks. This is the local counterpart of `scripts/performance.sh`
    (which targets Docker Compose on a CI runner): it runs directly against podman
    on a developer machine without needing Docker Desktop.

    SQLite needs no service and always runs. Each network backend is measured only
    when its container is started; the benchmark skips backends whose URL is absent.

    Port notes: MySQL and SQL Server are bound to non-standard host ports (13306
    and 11433) because the standard 3306/1433 ports are frequently blocked or held
    by local services on Windows, which silently breaks podman port forwarding.

.PARAMETER Backends
    Which services to start and benchmark: sqlite, redis, postgres, mysql, mssql,
    or 'all'. Comma-separated or repeated. Default is 'all'.

.PARAMETER RelaxedDurability
    Start MySQL and PostgreSQL with relaxed durability (MySQL
    --innodb-flush-log-at-trx-commit=2 --sync-binlog=0, PostgreSQL
    -c synchronous_commit=off). This demonstrates how much of the FlowStore latency
    is per-commit disk fsync rather than client overhead. Measurements are NOT
    comparable to a durable deployment; use only to isolate the fsync cost.

.PARAMETER KeepContainers
    Leave the containers running after the benchmarks finish instead of removing
    them. Useful for iterating quickly or running other ignored E2E tests.

.PARAMETER InProcess
    Also run the in-process benchmarks (critical path, mediator, flow) that need
    no external service, alongside the storage benchmark.

.EXAMPLE
    ./scripts/performance-local.ps1
    Starts every backend, runs the storage benchmark, and removes the containers.

.EXAMPLE
    ./scripts/performance-local.ps1 -Backends sqlite,redis -KeepContainers
    Measures only the local SQLite store and a Redis container, leaving it running.

.EXAMPLE
    ./scripts/performance-local.ps1 -Backends postgres,mysql -RelaxedDurability
    Shows the throughput gained when the per-commit fsync is disabled.
#>
[CmdletBinding()]
param(
    [string[]]$Backends = @('all'),
    [switch]$RelaxedDurability,
    [switch]$KeepContainers,
    [switch]$InProcess
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ContainerPrefix = 'catga-perf'

if ($Backends -contains 'all') {
    $Backends = @('sqlite', 'redis', 'postgres', 'mysql', 'mssql')
}

$Wanted = @{}
foreach ($backend in $Backends) {
    $Wanted[$backend.ToLowerInvariant()] = $true
}

function Test-Tool {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required tool '$Name' is not available on PATH."
    }
}

Test-Tool -Name podman
Test-Tool -Name cargo

# Container definitions. The MySQL and SQL Server durability flags are only added
# when -RelaxedDurability is requested; the default matches a durable deployment.
$Containers = @{
    redis = @{
        Image   = 'docker.io/library/redis:8-alpine'
        Env     = @()
        Ports   = @('127.0.0.1:6379:6379')
        Command = @()
        Health  = 'redis-cli ping'
        UrlVar  = 'CATGA_REDIS_URL'
        Url     = 'redis://127.0.0.1:6379/'
    }
    postgres = @{
        Image   = 'docker.io/library/postgres:17-alpine'
        Env     = @('POSTGRES_DB=catga', 'POSTGRES_USER=catga', 'POSTGRES_PASSWORD=catga_e2e_password')
        Ports   = @('127.0.0.1:5432:5432')
        Command = $(if ($RelaxedDurability) { @('-c', 'synchronous_commit=off') } else { @() })
        Health  = 'pg_isready -U catga -d catga'
        UrlVar  = 'CATGA_POSTGRES_URL'
        Url     = 'postgres://catga:catga_e2e_password@127.0.0.1:5432/catga'
    }
    mysql = @{
        Image   = 'docker.io/library/mysql:8.4'
        Env     = @(
            'MYSQL_DATABASE=catga',
            'MYSQL_USER=catga',
            'MYSQL_PASSWORD=catga_e2e_password',
            'MYSQL_ROOT_PASSWORD=catga_root_e2e_password'
        )
        Ports   = @('127.0.0.1:13306:3306')
        Command = $(if ($RelaxedDurability) { @('--innodb-flush-log-at-trx-commit=2', '--sync-binlog=0') } else { @() })
        Health  = 'mysqladmin ping -h 127.0.0.1 -u root -pcatga_root_e2e_password'
        UrlVar  = 'CATGA_MYSQL_URL'
        Url     = 'mysql://catga:catga_e2e_password@127.0.0.1:13306/catga'
    }
    mssql = @{
        Image   = 'mcr.microsoft.com/azure-sql-edge:latest'
        Env     = @('ACCEPT_EULA=1', 'MSSQL_SA_PASSWORD=Catga_e2e_password_2026!', 'MSSQL_PID=Developer')
        Ports   = @('127.0.0.1:11433:1433')
        Command = @()
        Health  = $null  # sqlcmd is not present in the azure-sql-edge image PATH
        UrlVar  = 'CATGA_MSSQL_URL'
        Url     = 'server=tcp:127.0.0.1,11433;User Id=sa;Password=Catga_e2e_password_2026!;TrustServerCertificate=true;Database=master'
    }
}

$Started = @()

function Stop-BenchmarkContainers {
    foreach ($name in $Started) {
        Write-Verbose "Removing container $name"
        & podman rm -f $name | Out-Null
    }
}

function Wait-ForHealth {
    param([string]$Name, [hashtable]$Definition, [int]$TimeoutSeconds = 180)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $hostPort = ([string]$Definition.Ports[0] -split ':')[1]
    while ((Get-Date) -lt $deadline) {
        $reachable = Test-NetConnection -ComputerName 127.0.0.1 -Port ([int]$hostPort) -InformationLevel Quiet -WarningAction SilentlyContinue
        if ($reachable) {
            if ($null -eq $Definition.Health) {
                return
            }
            $probe = & podman exec $Name @($Definition.Health -split ' ') 2>$null
            if ($LASTEXITCODE -eq 0) {
                return
            }
        }
        Start-Sleep -Seconds 2
    }
    throw "Service '$Name' did not become reachable on port $hostPort within $TimeoutSeconds seconds."
}

try {
    foreach ($backend in @('redis', 'postgres', 'mysql', 'mssql')) {
        if (-not $Wanted.ContainsKey($backend)) { continue }
        $definition = $Containers[$backend]
        $name = "$ContainerPrefix-$backend"

        & podman rm -f $name 2>$null | Out-Null
        $arguments = @('run', '-d', '--name', $name)
        foreach ($pair in $definition.Env) { $arguments += @('-e', $pair) }
        foreach ($mapping in $definition.Ports) { $arguments += @('-p', $mapping) }
        $arguments += $definition.Image
        $arguments += $definition.Command

        Write-Host "Starting $backend container ($name)..."
        & podman @arguments | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "podman failed to start $backend" }
        $Started += $name
    }

    foreach ($backend in @('redis', 'postgres', 'mysql', 'mssql')) {
        if (-not $Started.Contains("$ContainerPrefix-$backend")) { continue }
        Write-Host "Waiting for $backend to become ready..."
        Wait-ForHealth -Name "$ContainerPrefix-$backend" -Definition $Containers[$backend]
    }

    # Clear every benchmark URL first so the harness never measures a stale service.
    foreach ($definition in $Containers.Values) {
        [Environment]::SetEnvironmentVariable($definition.UrlVar, '', 'Process')
    }
    foreach ($backend in @('redis', 'postgres', 'mysql', 'mssql')) {
        if ($Started.Contains("$ContainerPrefix-$backend")) {
            $definition = $Containers[$backend]
            [Environment]::SetEnvironmentVariable($definition.UrlVar, $definition.Url, 'Process')
        }
    }

    if ($InProcess) {
        Write-Host "Running in-process benchmarks (critical path, mediator, flow)..."
        & cargo test --release -p catga-tests --all-features `
            --test critical_path_performance --test mediator_performance --test flow_performance `
            -- --ignored --nocapture
        if ($LASTEXITCODE -ne 0) { throw "in-process benchmarks failed" }
    }

    Write-Host "Running storage benchmark (SQLite always; started services included)..."
    & cargo test --release -p catga-tests --all-features --test storage_performance -- --ignored --nocapture
    if ($LASTEXITCODE -ne 0) { throw "storage benchmark failed" }

    Write-Host "`nPerformance run complete." -ForegroundColor Green
    if ($RelaxedDurability) {
        Write-Host "NOTE: MySQL/PostgreSQL ran with relaxed durability; numbers show the fsync cost, not production behavior." -ForegroundColor Yellow
    }
}
finally {
    if (-not $KeepContainers) {
        Stop-BenchmarkContainers
        Write-Verbose "Removed benchmark containers."
    } else {
        Write-Host "Containers left running: $($Started -join ', ')"
    }
}
