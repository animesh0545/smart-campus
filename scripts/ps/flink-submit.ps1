# PowerShell version of scripts/flink-submit.sh
#   .\scripts\ps\flink-submit.ps1           # build if needed, upload, submit as EMR step
#   .\scripts\ps\flink-submit.ps1 -Status
#   .\scripts\ps\flink-submit.ps1 -Rebuild
param([switch]$Status, [switch]$Rebuild)
. "$PSScriptRoot\common.ps1"
Need @('aws')

$Cluster = TfOut 'emr_cluster_id'
$Brokers = TfOut 'msk_bootstrap_brokers'
$Bucket  = TfOut 'artifacts_bucket'
$Region  = Region
$Jar     = Join-Path $Root 'analytics-flink\target\usage-analytics.jar'
if (-not $Cluster) { Die 'EMR is off. Run .\scripts\ps\emr.ps1 up' }
if (-not $Brokers) { Die 'MSK is off' }

if ($Status) {
  aws emr list-steps --cluster-id $Cluster --region $Region --query 'Steps[].[Name,Status.State,Status.Timeline.StartDateTime]' --output table
  exit 0
}
if ($Rebuild -or -not (Test-Path $Jar)) {
  Need @('mvn'); Info 'building Flink job'
  Exec { mvn -B -q -f "$Root\analytics-flink\pom.xml" package }
}
Exec { aws s3 cp $Jar "s3://$Bucket/flink/usage-analytics.jar" --region $Region }

$cmd = "aws s3 cp s3://$Bucket/flink/usage-analytics.jar /home/hadoop/usage-analytics.jar && flink run -m yarn-cluster -d -ynm usage-analytics /home/hadoop/usage-analytics.jar --bootstrap $Brokers --checkpoint-dir s3://$Bucket/flink-checkpoints --window-seconds 60"
$steps = @(@{ Name = 'usage-analytics-flink'; ActionOnFailure = 'CONTINUE'; Type = 'CUSTOM_JAR'; Jar = 'command-runner.jar'; Args = @('bash', '-c', $cmd) })
$file = New-TemporaryFile
ConvertTo-Json -InputObject $steps -Depth 5 | Set-Content -Encoding ascii $file
$stepId = aws emr add-steps --cluster-id $Cluster --region $Region --steps "file://$file" --query 'StepIds[0]' --output text
Remove-Item $file
Info "step $stepId submitted; waiting"
aws emr wait step-complete --cluster-id $Cluster --step-id $stepId --region $Region
if ($LASTEXITCODE -eq 0) { Ok 'Flink job running on YARN (detached)' } else { Warn "step failed: check CloudWatch group $(TfOut 'flink_log_group')" }
