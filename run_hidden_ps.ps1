Param(
    [string]$RepoPath = "$PSScriptRoot",
    [string]$BatchName = 'autorun_cryptonews.bat'
)

$fullBatch = Join-Path -Path $RepoPath -ChildPath $BatchName
if (-not (Test-Path $fullBatch)) {
    Write-Error "Batch file not found: $fullBatch"
    exit 1
}

$log = Join-Path -Path $RepoPath -ChildPath 'autorun_run.log'
"$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') starting $BatchName" | Out-File -FilePath $log -Append

Start-Process -FilePath 'cmd.exe' -ArgumentList '/c', "`"$fullBatch`"" -WindowStyle Hidden -Wait

"$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') finished $BatchName (ExitCode: $($LASTEXITCODE))" | Out-File -FilePath $log -Append
