# Container name
$containerName = "namenode"
 
# HDFS processed directory
$hdfsProcessedDir = "/user/jzapatso/processed_data"
 
# Local directory to save files
$localProcessedDir = ".\data\processed"
 
Write-Output "Downloading processed files from HDFS..."
 
# Download files inside container
docker exec $containerName bash -c "hdfs dfs -get -f $hdfsProcessedDir/temperature_results.csv /tmp/temperature_results.csv"
docker exec $containerName bash -c "hdfs dfs -get -f $hdfsProcessedDir/precipitation_results.csv /tmp/precipitation_results.csv"
 
# Copy files from container to local
docker cp "${containerName}:/tmp/temperature_results.csv" "$localProcessedDir\temperature_results.csv"
docker cp "${containerName}:/tmp/precipitation_results.csv" "$localProcessedDir\precipitation_results.csv"
 
Write-Output "Download completed."