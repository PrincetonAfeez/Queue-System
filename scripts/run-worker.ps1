# Example long-running consumer for simplequeue.
# Usage:
#   .\scripts\run-worker.ps1
#   .\scripts\run-worker.ps1 -Db queue.db -Queue jobs -Workers 2
param(
    [string]$Db = "queue.db",
    [string]$Queue = "default",
    [int]$Workers = 1,
    [string]$Config = "",
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Extra
)

$argsList = @(
    "consume",
    "--db", $Db,
    "--queue", $Queue,
    "--workers", "$Workers",
    "--mode", "at-least-once",
    "--sweeper"
)

if ($Config) {
    $argsList += @("--config", $Config)
}

if ($Extra) {
    $argsList += $Extra
}

& simplequeue @argsList
