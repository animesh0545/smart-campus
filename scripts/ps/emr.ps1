# PowerShell version of scripts/emr.sh:   .\scripts\ps\emr.ps1 up | down
param([Parameter(Mandatory)][ValidateSet('up', 'down')][string]$Action)
. "$PSScriptRoot\common.ps1"
Need @('terraform')
$flag = if ($Action -eq 'up') { 'true' } else { 'false' }
Exec { terraform "-chdir=$Infra" apply -input=false -auto-approve -var "enable_emr=$flag" }
if ($Action -eq 'up') { Ok "EMR cluster: $(TfOut 'emr_cluster_id'). Next: .\scripts\ps\flink-submit.ps1" } else { Ok 'EMR destroyed' }
Warn 'set enable_emr in terraform.tfvars too, or a later plain apply reverts this'
