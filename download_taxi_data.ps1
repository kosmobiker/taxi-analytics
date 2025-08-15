# =================================================================================
# PowerShell Script to Download NYC Taxi Trip Data from AWS S3
# This script iterates through specified taxi types, years, and months,
# and downloads the corresponding Parquet files.
# =================================================================================

# Base URL for NYC TLC trip data
$baseUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Define taxi types and months/years to download
# You can easily adjust these arrays to download more or less data
$taxiTypes = @("yellow", "green")
$years = @("2020", "2021", "2022", "2023", "2024", "2025")
$months = @("01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12")

# Get the path to the current script's directory and create a 'data' subdirectory
$dataDir = Join-Path $PSScriptRoot "data"
if (-not (Test-Path $dataDir)) {
    New-Item -ItemType Directory -Path $dataDir | Out-Null
    Write-Host "Created directory: $dataDir"
}

# Define the function to handle the download logic for a single file.
# This makes the main loop cleaner and easier to read.
function Download-File {
    param(
        [string]$url,
        [string]$outputPath,
        [string]$fileName
    )
    
    Write-Host "Downloading $fileName..." -NoNewline
    try {
        # The -Headers parameter helps with some S3 configurations, and the `[System.Net.ServicePointManager]`
        # line is a workaround for potential TLS/SSL protocol issues on older PowerShell versions.
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri $url -OutFile $outputPath -Headers @{"Accept-Encoding" = "gzip"}
        Write-Host " -> SUCCESS" -ForegroundColor Green
    }
    catch {
        Write-Host " -> ERROR" -ForegroundColor Red
        Write-Host ("Error downloading $fileName. Message: " + $_.Exception.Message) -ForegroundColor Yellow
    }
}

# Main loop to iterate through all combinations of taxi type, year, and month.
foreach ($taxiType in $taxiTypes) {
    foreach ($year in $years) {
        Write-Host "`nStarting downloads for $taxiType taxi data for $year..."
        
        foreach ($month in $months) {
            # Construct the file name and URL for the specific file
            $fileName = "${taxiType}_tripdata_${year}-${month}.parquet"
            $url = "$baseUrl/$fileName"
            $outputPath = Join-Path $dataDir $fileName
            
            # Check if the file already exists to prevent re-downloading
            if (Test-Path $outputPath) {
                Write-Host "File already exists: $fileName. Skipping..." -ForegroundColor Cyan
                continue
            }

            # Call the helper function to perform the download
            Download-File -url $url -outputPath $outputPath -fileName $fileName
        }
    }
}

Write-Host "`nAll downloads attempted. Check the 'data' folder for files."
