# Name or ID of your Hadoop container
$containerName = "namenode"
 
# Local file paths
$localRawFile = ".\data\raw\weather_data.csv"
$localTempFile = ".\data\processed\temperature_results.csv"
$localPrecipFile = ".\data\processed\precipitation_results.csv"
 
# Temporary paths inside container
$containerTempDir = "/tmp"
 
# Get current username
$userName = whoami
 
Write-Output "Copying files to Docker container..."
Start-Process docker -ArgumentList @("cp", $localRawFile, "${containerName}:${containerTempDir}/weather_data.csv") -NoNewWindow -Wait
Start-Process docker -ArgumentList @("cp", $localTempFile, "${containerName}:${containerTempDir}/temperature_results.csv") -NoNewWindow -Wait
Start-Process docker -ArgumentList @("cp", $localPrecipFile, "${containerName}:${containerTempDir}/precipitation_results.csv") -NoNewWindow -Wait
 
Write-Output "Creating directories in HDFS inside container..."
docker exec $containerName hdfs dfs -mkdir -p "/user/$userName/raw_data"
docker exec $containerName hdfs dfs -mkdir -p "/user/$userName/processed_data"
 
Write-Output "Uploading files to HDFS inside container..."
docker exec $containerName hdfs dfs -put -f "${containerTempDir}/weather_data.csv" "/user/$userName/raw_data/"
docker exec $containerName hdfs dfs -put -f "${containerTempDir}/temperature_results.csv" "/user/$userName/processed_data/"
docker exec $containerName hdfs dfs -put -f "${containerTempDir}/precipitation_results.csv" "/user/$userName/processed_data/"
 
Write-Output "Upload to HDFS completed."