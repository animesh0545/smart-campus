# Smart Campus: room & equipment booking on AWS
**CS/SS G527 Cloud Computing assignment · Scenario S4 · Concept: data partitioning with consistent hashing**

| Member | ID | Tasks | Services |
|---|---|---|---|
| Animesh Bhargava | 2026PHXP0098P | a IaC · c EKS + HPA + k6 · d GitOps · h observability | User Service, Resource Registry, Room Booking, Availability |
| Shreyansh Kedawat | 2022HS120897P | b serverless + REST/gRPC/MQ · e Flink · f consistent hashing · g storage | Partition Manager, Equipment Booking, Notification (Lambda), Usage Analytics (Flink) |

Other documents: [`docs/DESIGN.md`](docs/DESIGN.md) (design document) · [`docs/DEMO-ANIMESH.md`](docs/DEMO-ANIMESH.md) · [`docs/DEMO-SHREYANSH.md`](docs/DEMO-SHREYANSH.md) · [`task_divison.txt`](task_divison.txt)

**Contents**
[1 Repository layout](#1-repository-layout) · [2 Command cheat-sheet](#2-command-cheat-sheet) · [3 Prerequisites](#3-prerequisites) · [4 Run and test locally](#4-run-and-test-locally-no-aws) · [5 Deploy to AWS](#5-deploy-to-aws-step-by-step) · [6 Test on AWS](#6-test-on-aws) · [7 Demo day runbook](#7-demo-day-runbook) · [8 Daily workflow and cost](#8-daily-workflow-stop-resume-change-cost) · [9 Submission checklist](#9-submission-checklist) · [10 Troubleshooting](#10-troubleshooting) · [11 Bash / PowerShell map](#11-bash--powershell-map)

---

## 1. Repository layout

```
infra/                      Terraform (requirement a)
  main.tf variables.tf …    root module, one S3 remote state
  modules/network           VPC, public/private subnets, IGW, S3/DynamoDB endpoints (no NAT)
  modules/eks               EKS 1.35, node group, add-ons, Pod Identity role for app pods
  modules/cicd              ECR repos, GitHub OIDC role for CI
  modules/data              RDS PostgreSQL, DynamoDB tables, S3 buckets, SQS + DLQ, SNS
  modules/msk               Kafka (MSK) cluster
  modules/emr               Flink-on-EMR cluster, EMR IAM roles, CloudWatch-agent bootstrap
  modules/lambda            Notification Lambda + SQS and S3 triggers
  modules/argocd            namespaces, platform ConfigMap/Secret, ArgoCD, root app
  modules/observability     Flink log group, Grafana CloudWatch IAM, AWS Budgets alert
services/                   Python microservices (one image each, built from services/Dockerfile)
  common/                   shared library: config, logging/metrics, gRPC, Kafka/SQS, hash ring, booking core
  user-service/ resource-registry/ room-booking/ equipment-booking/
  availability/ partition-manager/ usage-analytics/      app.py per service
  notification-lambda/      Lambda handler (deployed by Terraform, not a container)
  gateway/nginx.local.conf  gateway config for docker compose
  tests/                    pytest suite (unit + in-process integration)
proto/campus.proto          gRPC contract (Availability, Partition Manager)
gitops/                     everything ArgoCD deploys (requirement d)
  root/                     app-of-apps Helm chart (metrics-server, monitoring, our apps)
  apps/smart-campus/        Deployments, Services, HPAs, gateway, ZooKeeper, kustomization (image tags)
  observability/            PodMonitor, alert rules, 2 Grafana dashboards
analytics-flink/            Flink 1.18 job (Java/Maven) for EMR (requirement e)
loadtest/                   k6-smoke.js, k6-load.js
samples/                    resources-import.csv (bulk import demo)
scripts/                    bash helpers; scripts/ps/ = PowerShell equivalents
.github/workflows/          ci.yml (test, build, push, bump tags), terraform.yml (fmt + validate)
docker-compose.yml          local stack
```

## 2. Command cheat-sheet

| Goal | Command |
|---|---|
| Unit + integration tests | `pytest -q services/tests` |
| Local stack up / check / down | `./scripts/local.sh up` · `./scripts/local.sh check` · `./scripts/local.sh down` |
| Provision AWS | `./scripts/bootstrap.sh` |
| Build + deploy images (without CI) | `./scripts/build-push.sh [service …]` |
| Status, public URL, smoke test | `./scripts/status.sh` · `./scripts/status.sh --url` |
| Full functional check on AWS | `python3 scripts/e2e_check.py --url $(./scripts/status.sh --url) --rebalance` |
| ArgoCD / Grafana / Prometheus UIs | `./scripts/port-forward.sh` → :8081 / :3000 / :9090 |
| HPA load test | `k6 run -e BASE_URL=$URL loadtest/k6-load.js` + `kubectl -n smart-campus get hpa -w` |
| EMR on/off | `./scripts/emr.sh up` · `./scripts/emr.sh down` |
| Run the Flink job | `./scripts/flink-submit.sh` |
| Read a Kafka topic | `./scripts/kafka-consume.sh analytics-results` |
| Consistent-hashing demo | `./scripts/demo-hashing.sh simulate|ring|lookup|join|leave|zk` |
| Events / Lambda demo | `./scripts/demo-events.sh` · `./scripts/demo-events.sh upload` |
| Seed data | `python3 scripts/seed_bookings.py --url $URL --count 300` |
| Destroy everything | `./scripts/teardown.sh` |

---

## 3. Prerequisites

### 3.1 Tools (each member's laptop)

| Tool | Version | Check | Needed for |
|---|---|---|---|
| Git | any | `git --version` | everything |
| Python | 3.10+ | `python3 --version` | tests, helper scripts |
| Docker Desktop / Engine with buildx | recent | `docker buildx version` | local stack, image builds |
| AWS CLI | v2 | `aws --version` | AWS |
| Terraform | ≥ 1.6 | `terraform version` | AWS |
| kubectl | ≥ 1.30 | `kubectl version --client` | AWS |
| k6 | any | `k6 version` | load tests |
| Java + Maven | JDK 11+ / Maven 3.8+ | `mvn -v` | Flink jar (CI can build it instead) |
| GitHub CLI (optional) | any | `gh --version` | creating the repo |

macOS: `brew install awscli terraform kubectl k6 maven gh` (+ Docker Desktop). WSL/Ubuntu: use the vendors' apt instructions. Windows-only users: same tools, then use `scripts\ps\*.ps1` (section 11).

### 3.2 AWS account (do this first: these can block you)
1. `aws configure` (region `us-east-1`), then `aws sts get-caller-identity`.
2. The account must be on the **Paid plan**: MSK and EMR are not available on the Free plan; free credits still apply. This is the **only console action** in the whole project.
3. **EC2 vCPU quota** ≥ 16 (EKS 3 × 2 vCPU + EMR 2 × 4 vCPU):
   ```bash
   aws service-quotas get-service-quota --service-code ec2 --quota-code L-1216C47A --query Quota.Value
   # if < 16:
   aws service-quotas request-service-quota-increase --service-code ec2 --quota-code L-1216C47A --desired-value 32
   ```
4. Teammate access: both members can share one AWS account. Put the second member's IAM user ARN in `extra_admin_arns` (section 5.2) so they get `kubectl` access too.

---

## 4. Run and test locally (no AWS)

Use this while AWS provisions (MSK takes ~25 min), and to develop.

### 4.1 Automated tests (≈ 20 s)
```bash
python3 -m venv .venv && source .venv/bin/activate         # Windows: .venv\Scripts\Activate.ps1
pip install -r services/requirements.txt pytest moto
pytest -q services/tests
```
**Expected:** `22 passed`. The suite covers:
- the hash ring maths (k/n movement, vnode balance);
- the booking flow over real gRPC with moto DynamoDB (create, 409 conflict, modify across partitions, cancel, scatter/gather);
- the Partition Manager API, with an in-memory ZooKeeper (seeding, watch, lock, join/leave without losing data);
- the Lambda handler (SQS, S3, partial-batch failure);
- the analytics sink.

### 4.2 Full local stack
```bash
./scripts/local.sh up          # docker compose: Postgres, ZooKeeper, Kafka, DynamoDB Local, 7 services, nginx gateway
curl http://localhost:8080/    # route list
./scripts/local.sh check       # end-to-end check through the gateway, includes node-3 join + leave
```
**Expected:** `… checks passed` with no FAIL lines. Other things to try:
```bash
./scripts/local.sh seed 200                    # bookings spread over node-0..2
curl -s localhost:8080/api/partitions/ring | python3 -m json.tool
curl -s "localhost:8080/api/partitions/simulate?keys=10000&nodes=3" | python3 -m json.tool
./scripts/local.sh load 1m                     # k6 against the local stack
./scripts/local.sh logs room-booking           # JSON logs with correlation ids
```

### 4.3 Flink locally (optional, to test requirement e without EMR)
```bash
./scripts/local.sh flink       # builds the jar (Maven), starts Flink 1.18, runs the job with 20 s windows
./scripts/local.sh seed 50
./scripts/local.sh results     # JSON window results appear on analytics-results
curl -s "localhost:8080/api/analytics/summary?minutes=30" | python3 -m json.tool
```
Flink UI: http://localhost:8081.

### 4.4 Stop
```bash
./scripts/local.sh down
```
Not available locally (AWS-only): SQS, the Lambda and S3. Publishing to them is skipped and logged as `skipped` in the `events_published_total` metric.

---

## 5. Deploy to AWS (step by step)

Total ≈ 45–60 min, mostly waiting. Start step 5.3 within your first hour.

### 5.1 Create the GitHub repo (public, so ArgoCD can read it)
```bash
git init && git add -A && git commit -m "Smart Campus S4"
gh repo create smart-campus --public --source . --push       # or create on github.com, then: git remote add origin … && git push -u origin main
```

### 5.2 Configure Terraform
```bash
cp infra/terraform.tfvars.example infra/terraform.tfvars
```
Edit `infra/terraform.tfvars`:
```hcl
github_repo      = "<your-github-user>/smart-campus"
budget_email     = "<email for budget alerts>"
enable_msk       = true
enable_emr       = false                                         # on only when testing (e) / demo
extra_admin_arns = ["arn:aws:iam::<account>:user/<teammate>"]   # optional
# notification_email = "<email>"   # optional: real booking emails via SNS (confirm the subscription mail)
```
Optional sanity check (needs Terraform):
```bash
terraform -chdir=infra fmt -recursive && terraform -chdir=infra init -backend=false && terraform -chdir=infra validate
```

### 5.3 Provision
```bash
./scripts/bootstrap.sh
```
The script:
1. creates the S3 state bucket and DynamoDB lock table, and writes `infra/backend.hcl`;
2. runs **phase 1** (VPC + MSK + EKS in parallel, ≈ 25 min);
3. runs **phase 2** (RDS, DynamoDB, S3, SQS/SNS, Lambda, ECR, CI role, ArgoCD + root app, Grafana IAM, budget);
4. configures `kubectl`.

You can split it: `./scripts/bootstrap.sh --phase1` now and `./scripts/bootstrap.sh --phase2` later.

**Verify**
```bash
kubectl get nodes                          # 3 × Ready
kubectl -n argocd get applications         # smart-campus-root + children (smart-campus is Degraded until images exist: normal)
terraform -chdir=infra output              # endpoints, github_actions_role_arn, buckets
```

### 5.4 Connect CI (once)
GitHub → repo → **Settings → Secrets and variables → Actions → Variables** → New repository variable:

| Name | Value |
|---|---|
| `AWS_ROLE_ARN` | `terraform -chdir=infra output -raw github_actions_role_arn` |
| `ARTIFACTS_BUCKET` | `terraform -chdir=infra output -raw artifacts_bucket` |

Also check Settings → Actions → General → Workflow permissions = **Read and write** (CI commits the image-tag bump).

### 5.5 First deploy (GitOps)
Option A, CI: `git commit --allow-empty -m "deploy" && git push`. GitHub Actions runs the tests, builds the 7 images, pushes them to ECR, and commits the new tags into `gitops/apps/smart-campus/kustomization.yaml`.
Option B, from your laptop: `./scripts/build-push.sh` does the same thing; it cross-builds amd64, so it works on Apple Silicon.

ArgoCD notices the commit within about 1 minute and deploys. **Nobody runs `kubectl apply`.**

**Verify**
```bash
git pull                                       # fetch CI's tag-bump commit
./scripts/status.sh                            # all pods Running, public URL, smoke test OK
export URL=$(./scripts/status.sh --url)        # http://<elb>.us-east-1.elb.amazonaws.com (DNS can take 2-3 min)
python3 scripts/e2e_check.py --url $URL        # expect: all checks passed
```

### 5.6 UIs
```bash
./scripts/port-forward.sh
```
| UI | URL | Login |
|---|---|---|
| ArgoCD | http://localhost:8081 | `admin` / password printed by the script |
| Grafana | http://localhost:3000 | `admin` / `lab123` (set in `gitops/root/values.yaml`) |
| Prometheus | http://localhost:9090 | none |

Grafana → Dashboards: **Smart Campus - Services & Cluster** and **Smart Campus - Serverless & Flink logs (CloudWatch)**.

### 5.7 Flink on EMR (requirement e)
```bash
./scripts/emr.sh up                         # ≈ 10 min, ≈ $0.48/hr
mvn -B -f analytics-flink/pom.xml package   # or download the "usage-analytics-jar" artifact from the CI run into analytics-flink/target/
./scripts/flink-submit.sh                   # uploads the jar to S3, submits an EMR step (Flink on YARN, detached)
```
**Verify**
```bash
./scripts/flink-submit.sh --status                         # step COMPLETED (= job submitted and running)
python3 scripts/seed_bookings.py --url $URL --count 100
./scripts/kafka-consume.sh analytics-results               # one JSON per active resource per minute (Ctrl+C to exit)
curl -s "$URL/api/analytics/summary?minutes=30" | python3 -m json.tool
```
In Grafana, the "Serverless & Flink logs" dashboard shows `WINDOW_RESULT` lines from the Flink job.

When finished: `./scripts/emr.sh down`.

---

## 6. Test on AWS

| What | How | Pass criteria |
|---|---|---|
| Functional, all services | `python3 scripts/e2e_check.py --url $URL --rebalance` | all checks passed |
| Smoke (k6) | `k6 run -e BASE_URL=$URL loadtest/k6-smoke.js` | checks 100 % |
| Autoscaling (c) | terminal 1: `kubectl -n smart-campus get hpa -w`; terminal 2: `k6 run -e BASE_URL=$URL loadtest/k6-load.js` (≈ 7 min) | `availability` and `room-booking` go from 1 to ≥ 3 replicas; `http_req_failed` < 5 % |
| GitOps (d) | change e.g. the `/` text in `gitops/apps/smart-campus/gateway-nginx.conf`, push; ArgoCD → Refresh | gateway pods roll, `curl $URL/` shows the change |
| Self-heal (d) | `kubectl -n smart-campus scale deploy user-service --replicas=3` | ArgoCD resets it to 1 within seconds |
| Events + Lambda (b) | `./scripts/demo-events.sh`, `./scripts/demo-events.sh upload` | Lambda log lines + notification text printed |
| Consistent hashing (f) | `./scripts/demo-hashing.sh join` then `leave` | flows only `→ node-3`, then `moved_only_from_leaving_node: true` |
| Flink (e) | section 5.7 | results on `analytics-results` every minute |
| Logs (h) | Grafana Loki panel, search a correlation id; CloudWatch dashboard | the same request visible across services; Lambda/Flink logs visible |
| Storage (g) | `aws dynamodb scan --table-name smart-campus-bookings-node-0 --select COUNT`; `aws s3 ls s3://$(terraform -chdir=infra output -raw uploads_bucket)/imports/` | items and CSV objects exist |

---

## 7. Demo day runbook

### 7.1 Before the slot

| When | Who | Action |
|---|---|---|
| T−60 min | Animesh | `./scripts/bootstrap.sh` if the stack was torn down (≈ 45 min), then `./scripts/status.sh` |
| T−25 min | Shreyansh | `./scripts/emr.sh up`, then `./scripts/flink-submit.sh` |
| T−10 min | either | `export URL=$(./scripts/status.sh --url)`; `python3 scripts/e2e_check.py --url $URL`; `python3 scripts/seed_bookings.py --url $URL --count 200`; `./scripts/demo-hashing.sh leave node-3 \|\| true` (start from 3 nodes) |
| T−5 min | both | `./scripts/port-forward.sh` (ArgoCD, Grafana tabs open); terminal with 2 panes; open `docs/DESIGN.md` diagrams |

### 7.2 Demo order (≈ 20–25 min total)

| # | Req | Presenter | Command / screen | What to point out |
|---|---|---|---|---|
| 1 | overview | both | `docs/DESIGN.md` diagrams, `curl $URL/` | 8 services, public URL, mechanisms |
| 2 | a | Animesh | `infra/main.tf`, `terraform -chdir=infra plan` | "No changes"; modules; no NAT; SG-to-SG; EMR roles in TF |
| 3 | d | Animesh | ArgoCD UI; push a change; scale-and-revert | Git is the source of truth; CI only bumps tags |
| 4 | c | Animesh | `kubectl get hpa -w` + `k6 run … k6-load.js` | replicas 1 → 5/6; Grafana HPA panel |
| 5 | h | Animesh | Grafana dashboards; Loki correlation-id search; CloudWatch dashboard | RPS / errors / latency / cluster health; all logs in one place |
| 6 | f | Shreyansh | `demo-hashing.sh simulate`, `ring`, `lookup`, `zk`, `join`, `leave` | ≈ 25 % vs ≈ 75 % moved; vnodes balance; ZooKeeper watch |
| 7 | b | Shreyansh | `demo-events.sh`, `demo-events.sh upload` | REST → gRPC → Kafka + SQS → Lambda; S3 trigger |
| 8 | e | Shreyansh | EMR step, `kafka-consume.sh analytics-results`, `/api/analytics/summary` | stateful 1-min window, checkpoints to S3 |
| 9 | g | Shreyansh | RDS via API, DynamoDB tables, S3 buckets | why 3 different stores |

The detailed scripts and examiner Q&A are in `docs/DEMO-ANIMESH.md` and `docs/DEMO-SHREYANSH.md`.

### 7.3 After the demo
```bash
./scripts/emr.sh down        # at least this
./scripts/teardown.sh        # everything, once grading is done
```

---

## 8. Daily workflow (stop, resume, change, cost)

* **Stop working:** `./scripts/teardown.sh`. Do not use plain `terraform destroy`: the gateway's load balancer is created by Kubernetes, and the script removes it first so the VPC can be deleted.
* **Resume:** `./scripts/bootstrap.sh`, then `git commit --allow-empty -m redeploy && git push` (or `./scripts/build-push.sh`). The ECR repositories are recreated empty, so images must be pushed again.
* **Change a service:** edit `services/<name>/app.py`, run `pytest -q services/tests`, then push. CI deploys it.
* **Change Kubernetes config:** edit `gitops/...` and push; ArgoCD applies it.
* **Change infrastructure:** edit `infra/...`, then `./scripts/bootstrap.sh --phase2` (a plain apply).
* **Don't run `terraform apply` from both laptops at once.** The DynamoDB lock blocks the second one, but wait for the first to finish.

**Cost** (us-east-1 list prices, approximate):

| Component | $/hr |
|---|---|
| EKS control plane | 0.10 |
| 3 × t3.large nodes | 0.25 |
| MSK 2 × kafka.t3.small | ~0.10 |
| RDS db.t4g.micro | 0.016 |
| Load balancer | 0.025 |
| **Running without EMR** | **≈ 0.49** |
| EMR 2 × m5.xlarge | +≈ 0.48 |
| DynamoDB, S3, SQS, Lambda, CloudWatch | cents |

The Budgets alert emails at 50 % and 80 % of `budget_limit_usd` (default $40) actual spend, and at 100 % forecast. **The main risk is leaving EMR or the stack running overnight.**

---

## 9. Submission checklist

- [ ] GitHub repo is public and up to date (`git push`), and the link is copied for Nalanda
- [ ] `task_divison.txt` at the repo root (names, IDs, tasks, services). Check the two IDs.
- [ ] Design document: `docs/DESIGN.md`. If a PDF is required: `pandoc docs/DESIGN.md -o DESIGN.pdf`, or open the file on GitHub and print to PDF (the Mermaid diagrams render there).
- [ ] Repo contains microservice code, Terraform, Kubernetes manifests and GitOps config (all present)
- [ ] One submission per group on Nalanda
- [ ] Each member has rehearsed their demo script (`docs/DEMO-*.md`)

---

## 10. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `bootstrap.sh` phase 2 fails with helm/kubernetes "unauthorized" or "no such host" | EKS was still settling. Re-run `./scripts/bootstrap.sh --phase2`. |
| MSK create fails: AZ not supported | Rare in us-east-1. Change `exclude_zone_ids` in `infra/modules/network/main.tf` and re-apply. |
| EMR: `VcpuLimitExceeded` | Quota too low (section 3.2). |
| Pods `ImagePullBackOff` | Images not pushed yet: section 5.5. Check that `kustomization.yaml` has your account id (`git pull` after CI). |
| CI fails at "configure-aws-credentials" | `AWS_ROLE_ARN` variable missing or wrong; the repo name must match `github_repo` in tfvars. |
| CI bump job: "permission denied to push" | Workflow permissions must be Read and write (section 5.4). |
| `observability-config` OutOfSync for a few minutes | It waits for the kube-prometheus-stack CRDs and retries automatically. |
| Grafana CloudWatch panels: "no credentials" | `kubectl -n monitoring rollout restart deploy kps-grafana` (Pod Identity injects credentials at pod start). |
| `status.sh` shows no URL | The load balancer is still provisioning (2–3 min); `kubectl -n smart-campus get svc gateway`. |
| Partition Manager not Ready | It waits for ZooKeeper: `kubectl -n smart-campus logs deploy/partition-manager`. |
| `flink-submit.sh` step FAILED | CloudWatch log group `/smart-campus/flink`, stream `*/emr-steps-stderr`; or `aws emr describe-step`. Check that MSK is up. |
| No rows in `analytics-results` | The Flink job reads only new events (latest offsets): seed bookings *after* submitting the job. |
| `teardown.sh` leaves the VPC | It retries after deleting `k8s-*` security groups. If it still fails, delete leftover ELBs in the VPC, then run it again. |
| Port-forward "address already in use" | Another forward is running: `pkill -f port-forward`. |
| Apple Silicon: "exec format error" in pods | The image was built for arm64. Use `scripts/build-push.sh`, which builds `--platform linux/amd64`. |

---

## 11. Bash / PowerShell map

Run the PowerShell versions from the repo root in PowerShell 7 (`pwsh`). If a script is blocked, run `Set-ExecutionPolicy -Scope Process Bypass` first.

| Bash (macOS / Linux / WSL) | PowerShell |
|---|---|
| `./scripts/local.sh up\|check\|seed\|load\|flink\|results\|logs\|down` | `.\scripts\ps\local.ps1 up\|check\|…` |
| `./scripts/bootstrap.sh [--phase1\|--phase2]` | `.\scripts\ps\bootstrap.ps1 [-Phase phase1\|phase2]` |
| `./scripts/build-push.sh [svc …]` | `.\scripts\ps\build-push.ps1 [-Services a,b] [-NoGit]` |
| `./scripts/status.sh [--url]` | `.\scripts\ps\status.ps1 [-Url]` |
| `./scripts/port-forward.sh` | `.\scripts\ps\port-forward.ps1` |
| `./scripts/emr.sh up\|down` | `.\scripts\ps\emr.ps1 up\|down` |
| `./scripts/flink-submit.sh [--status]` | `.\scripts\ps\flink-submit.ps1 [-Status] [-Rebuild]` |
| `./scripts/kafka-consume.sh <topic>` | `.\scripts\ps\kafka-consume.ps1 <topic>` |
| `./scripts/demo-hashing.sh <action>` | `.\scripts\ps\demo-hashing.ps1 <action>` |
| `./scripts/demo-events.sh [upload]` | `.\scripts\ps\demo-events.ps1 [-Upload]` |
| `./scripts/teardown.sh` | `.\scripts\ps\teardown.ps1` |
| `python3 scripts/e2e_check.py …`, `seed_bookings.py`, `set_image_tags.py` | same with `python` (standard library only) |
