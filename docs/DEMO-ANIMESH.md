# Demo script: Animesh Bhargava (2026PHXP0098P)

**Tasks:** a (IaC), c (EKS + HPA + k6), d (GitOps), h (observability)
**Services:** User Service, Resource Registry, Room Booking, Availability
**Time:** about 10 minutes. Have the stack up 45 min before (EMR is Shreyansh's; it is not needed for this part).

### Before the examiner arrives
```bash
./scripts/status.sh                    # all pods Running, URL printed
export URL=$(./scripts/status.sh --url)
./scripts/port-forward.sh &            # ArgoCD :8081, Grafana :3000, Prometheus :9090
python3 scripts/seed_bookings.py --url $URL --count 150
```
Open tabs: ArgoCD (Applications), Grafana dashboard "Smart Campus - Services & Cluster", terminal with two panes.

---

## 1. (a) Infrastructure as Code: 2 min
1. Show `infra/main.tf`: the module list (network, eks, cicd, data, msk, emr, lambda, argocd, observability).
2. `terraform -chdir=infra plan` → **"No changes"**: the live infrastructure matches the code.
3. Point out:
   * `modules/network`: public/private subnets, **no NAT** (cost), S3/DynamoDB gateway endpoints.
   * `modules/data`: RDS SG accepts 5432 **only from the EKS node SG** (SG-to-SG rule).
   * `modules/emr`: EMR roles created in Terraform instead of `aws emr create-default-roles` (no manual IAM step).
   * `enable_emr` / `enable_msk` switches + AWS Budgets alert in `modules/observability`.
4. `aws resourcegroupstaggingapi get-resources --tag-filters Key=ManagedBy,Values=terraform --query 'length(ResourceTagMappingList)'`: everything is tagged as Terraform-managed.

## 2. (d) GitOps: 2 min
1. ArgoCD UI: root app `smart-campus-root` → children (metrics-server, kube-prometheus-stack, loki, alloy, observability-config, smart-campus). All Synced/Healthy.
2. Explain: Terraform installs only ArgoCD + one root app; Git is the source of truth for workloads; CI only builds images and commits tags.
3. **Live change:** edit something visible, e.g. the gateway's `/` route text in `gitops/apps/smart-campus/gateway-nginx.conf`, then `git commit -am "demo: change gateway banner" && git push`. In ArgoCD click *Refresh* → the ConfigMap hash changes → gateway pods roll → `curl $URL/` shows the change.
   (Code change path: change a service → push → GitHub Actions → ECR → tag-bump commit → ArgoCD.)
4. **Self-heal:** `kubectl -n smart-campus scale deploy user-service --replicas=3` → ArgoCD reverts to 1 within seconds ("drift corrected").

## 3. (c) EKS + HPA + load test: 3 min
Pane 1:
```bash
kubectl -n smart-campus get hpa -w
```
Pane 2:
```bash
k6 run -e BASE_URL=$URL -e DURATION=4m loadtest/k6-load.js
```
* Within 1–2 minutes: `availability` and `room-booking` TARGETS rise above 60 %, REPLICAS 1 → 3 → 5/6.
* `kubectl -n smart-campus get pods -l app=availability -o wide`: new pods on several nodes.
* Grafana: "HPA replicas" panel steps up; RPS rises; p95 latency stays bounded.
* After k6 ends: replicas go back down after the 2-minute stabilisation window.

## 4. (h) Observability: 2 min
1. Grafana dashboard "Smart Campus - Services & Cluster": RPS / error rate / p95 per service, gRPC panels, HPA, node + pod health, Loki log panel.
2. Logs: set "service" = `room-booking`, search `booking created`; open one line and copy its `cid`; search that cid → the same request in gateway, room-booking, availability (gRPC) and partition-manager.
3. Second dashboard "Serverless & Flink logs (CloudWatch)": Lambda logs (and Flink when EMR is on): **all** services' logs are reachable from Grafana.
4. Prometheus → Alerts: `SmartCampusHighErrorRate`, `SmartCampusHPAAtMax` rules loaded.

## 5. My services: 1 min
```bash
curl -s $URL/api/users/U001 | python3 -m json.tool
curl -s "$URL/api/resources?type=LAB" | python3 -m json.tool | head -20
curl -s "$URL/api/availability/ROOM-001/$(date +%F)/schedule" | python3 -m json.tool
curl -s -X POST $URL/api/rooms/bookings -H 'Content-Type: application/json' \
  -d "{\"resourceId\":\"ROOM-010\",\"userId\":\"U003\",\"date\":\"$(date +%F)\",\"startTime\":\"16:00\",\"endTime\":\"17:00\"}"
```

---

## Likely questions

**Why Terraform rather than boto3?** Declarative and idempotent: `plan` shows a diff against the state file; boto3 is imperative, so you code the *how* and track what exists yourself.

**Where is the Terraform state? What if two of us apply at once?** In S3 (versioned) with a DynamoDB lock table, created once by `bootstrap.sh`; a second apply waits for the lock.

**Is anything created by hand?** No infrastructure. Two things are created *by Kubernetes from Git*: the gateway's AWS load balancer (Service type LoadBalancer) and its security group. The only console action is the account's Paid-plan upgrade.

**How does a pod get AWS permissions?** EKS Pod Identity: Terraform maps the `smart-campus-app` service account to an IAM role with only DynamoDB/SQS/S3 on our resources. No keys in the cluster or in Git.

**How does HPA decide?** Every 15 s it reads average CPU from metrics-server, as a percentage of the *request* (100m). `desired = ceil(current × actual / target)`. We allow +2 pods per 30 s up and wait 2 min before scaling down (avoids flapping).

**Why does gRPC traffic reach the new pods?** Clients resolve a headless Service (all pod IPs) with round-robin, and servers set `max_connection_age` = 30 s so clients reconnect and re-resolve. A normal ClusterIP would pin one long HTTP/2 connection to one pod.

**HPA vs VPA vs Cluster Autoscaler?** HPA adds pods, VPA resizes pod requests, CA adds nodes. We use HPA; the node group has min 2 / max 4 (CA not installed, so nodes stay at desired size).

**What happens if someone `kubectl apply`s a change?** ArgoCD self-heal reverts it to what Git says. Rollback = `git revert`.

**How does CI authenticate to AWS?** GitHub OIDC → `sts:AssumeRoleWithWebIdentity` into a role limited to ECR push and the Flink artifact path, and only for this repository.

**Why Loki and not EFK?** Loki indexes only labels, so it is light enough for a 3-node cluster, and Grafana queries it next to Prometheus. Alloy tails every pod's stdout and pushes to Loki.

**RPS / error rate / latency queries?** `sum by (service)(rate(http_requests_total[1m]))`; 5xx rate / total rate; `histogram_quantile(0.95, sum by (service, le)(rate(http_request_duration_seconds_bucket[1m])))`.

**Why no NAT gateway, and is that safe?** It saves cost. Nodes have public IPs but their SG only admits node-to-node, control-plane and LB NodePort traffic; RDS and MSK are in private subnets with no internet route at all.

**Why is 409 not counted as an error in k6?** A taken slot is a correct business answer; we mark 409 as an expected status so `http_req_failed` reflects real failures.
