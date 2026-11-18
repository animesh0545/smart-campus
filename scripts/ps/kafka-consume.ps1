# PowerShell version of scripts/kafka-consume.sh (throw-away client pod; MSK has no public endpoint)
#   .\scripts\ps\kafka-consume.ps1 analytics-results
param([string]$Topic = 'analytics-results')
. "$PSScriptRoot\common.ps1"
Need @('kubectl')
$Brokers = TfOut 'msk_bootstrap_brokers'
if (-not $Brokers) { Die 'MSK is off' }
kubectl -n $NS run "kafka-cli-$(Get-Random)" --rm -i --tty --restart=Never --image=apache/kafka:3.9.0 -- `
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server $Brokers --topic $Topic --from-beginning --property print.timestamp=true
