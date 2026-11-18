#!/bin/bash
# EMR bootstrap action: ship Flink (YARN container) logs and EMR step logs to
# CloudWatch Logs group ${log_group}, so they show up in Grafana next to the
# Loki logs of the EKS services (centralized logging, requirement h).
set -euxo pipefail

if command -v dnf >/dev/null 2>&1; then
  sudo dnf install -y amazon-cloudwatch-agent
else
  sudo yum install -y amazon-cloudwatch-agent
fi

sudo mkdir -p /opt/aws/amazon-cloudwatch-agent/etc
cat <<'EOF' | sudo tee /opt/aws/amazon-cloudwatch-agent/etc/flink-logs.json
{
  "agent": { "run_as_user": "root" },
  "logs": {
    "logs_collected": {
      "files": {
        "collect_list": [
          {
            "file_path": "/var/log/hadoop-yarn/containers/application_*/container_*/jobmanager.log",
            "log_group_name": "${log_group}",
            "log_stream_name": "{instance_id}/jobmanager",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/hadoop-yarn/containers/application_*/container_*/taskmanager.log",
            "log_group_name": "${log_group}",
            "log_stream_name": "{instance_id}/taskmanager",
            "timezone": "UTC"
          },
          {
            "file_path": "/var/log/hadoop-yarn/containers/application_*/container_*/taskmanager.out",
            "log_group_name": "${log_group}",
            "log_stream_name": "{instance_id}/taskmanager-stdout",
            "timezone": "UTC"
          },
          {
            "file_path": "/mnt/var/log/hadoop/steps/*/stdout",
            "log_group_name": "${log_group}",
            "log_stream_name": "{instance_id}/emr-steps",
            "timezone": "UTC"
          },
          {
            "file_path": "/mnt/var/log/hadoop/steps/*/stderr",
            "log_group_name": "${log_group}",
            "log_stream_name": "{instance_id}/emr-steps-stderr",
            "timezone": "UTC"
          }
        ]
      }
    }
  }
}
EOF

sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config -m ec2 -s \
  -c file:/opt/aws/amazon-cloudwatch-agent/etc/flink-logs.json
