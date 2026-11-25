# Builds redis-java with plain javac (no Maven/Gradle needed).
#   .\build.ps1          compile + build redis-java.jar
#   .\build.ps1 test     compile + run the end-to-end tests
#   .\build.ps1 run      compile + start the server on port 6379
param([string]$Task = "build", [Parameter(ValueFromRemainingArguments)] $Rest)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# Find a JDK: JAVA_HOME, then PATH, then Android Studio's bundled one.
$jdkBin = $null
if ($env:JAVA_HOME -and (Test-Path "$env:JAVA_HOME\bin\javac.exe")) { $jdkBin = "$env:JAVA_HOME\bin" }
elseif (Get-Command javac -ErrorAction SilentlyContinue) { $jdkBin = Split-Path (Get-Command javac).Source }
elseif (Test-Path "C:\Program Files\Android\Android Studio\jbr\bin\javac.exe") { $jdkBin = "C:\Program Files\Android\Android Studio\jbr\bin" }
else { throw "No JDK 21+ found. Install one or set JAVA_HOME." }

$main = Get-ChildItem src\main\java -Recurse -Filter *.java | ForEach-Object FullName
Remove-Item -Recurse -Force out -ErrorAction SilentlyContinue
& "$jdkBin\javac.exe" --release 21 -d out\main $main
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

switch ($Task) {
    "build" {
        & "$jdkBin\jar.exe" --create --file redis-java.jar --main-class redis.Main -C out\main .
        Write-Host "Built redis-java.jar  ->  java -jar redis-java.jar --port 6379"
    }
    "test" {
        $tests = Get-ChildItem src\test\java -Recurse -Filter *.java | ForEach-Object FullName
        & "$jdkBin\javac.exe" --release 21 -cp out\main -d out\test $tests
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        $failed = 0
        foreach ($t in "redis.ServerTest", "redis.RdbTest", "redis.ReplicationTest") {
            Write-Host "== $t"
            & "$jdkBin\java.exe" -cp "out\main;out\test" $t
            if ($LASTEXITCODE -ne 0) { $failed = 1 }
        }
        exit $failed
    }
    "run" { & "$jdkBin\java.exe" -cp out\main redis.Main @Rest }
    default { throw "unknown task: $Task (use build, test or run)" }
}
