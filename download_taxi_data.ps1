# Base URL for NYC TLC trip data
$baseUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Define taxi types and months to download
$taxiTypes = @("yellow", "green")
$months = @("01", "02", "03") # January to March 2025
$year = "2025"

# Create data directory if it doesn't exist
$dataDir = Join-Path $PSScriptRoot "data"
if (-not (Test-Path $dataDir)) {
    New-Item -ItemType Directory -Path $dataDir
    Write-Host "Created directory: $dataDir"
}

foreach ($taxiType in $taxiTypes) {
    foreach ($month in $months) {
        $fileName = "${taxiType}_tripdata_${year}-${month}.parquet"
        $url = "$baseUrl/$fileName"
        $outputPath = Join-Path $dataDir $fileName
        
        Write-Host "Downloading $fileName..."
        try {
            Invoke-WebRequest -Uri $url -OutFile $outputPath
            Write-Host "Successfully downloaded $fileName"
        }
        catch {
            Write-Host ("Error downloading $fileName. Error: " + $_.Exception.Message)
        }
    }
}

Write-Host "`nDownload complete. Files are in: $dataDir"
Write-Host "Summary of downloaded files:"
Get-ChildItem $dataDir -Filter "*.parquet" | ForEach-Object {
    $size = [math]::Round(($_.Length / 1MB), 2)
    Write-Host "$($_.Name) - $size MB"
}
