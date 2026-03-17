# Handoff: Migrate Jenkins Agents from EKS K8s Pods to ECS Fargate

## Who Is This For
A new setup session focused purely on AWS + Jenkins infrastructure migration.
The user (or another AI chat) should complete this setup end-to-end.

---

## Background — What This Project Is

This is an **ETL validation test suite** (`etl-validator-github`) that runs automated tests against a banking data pipeline. The pipeline ingests bank files, transforms them, and loads records into a database. The test suite validates that invalid field values (bad account numbers, bad payment modes, bad org codes, etc.) are correctly rejected by the pipeline.

- **Framework:** Python 3.9, pytest, allure-pytest
- **CI/CD:** Jenkins (CloudBees) running on EKS
- **AWS services used:** S3 (checkpoints, test data), RDS/MSSQL (DB validation), AWS assume-role via IRSA
- **Test run time:** ~3–4 hours for a full suite (44 test functions)

---

## The Problem Being Solved

### Root Cause: 1-Hour K8s Agent Timeout
The Jenkins pipeline currently uses **Kubernetes pod agents** (via the Kubernetes plugin). The pod spec is:

```yaml
containers:
  - name: python
    image: "python:3.9"
  - name: awscli
    image: "amazon/aws-cli:latest"
  - name: java
    image: "eclipse-temurin:17-jre"
```

K8s pod agents have a hard **1-hour timeout limit** enforced by the Kubernetes plugin and/or cluster policy. Because the full test suite takes 3–4 hours, the pipeline cannot complete in a single build.

### Current Workaround (Checkpoint Chaining)
A checkpoint system was built as a workaround:
- Tests write progress to S3 after each test completes
- When the agent times out, Jenkins auto-triggers a new build with `CHECKPOINT_ID` and `RESUME_COUNT` parameters
- The next build reads the checkpoint, skips already-completed tests, and continues from where it left off
- `MAX_RESUME_COUNT = 6` (maximum 6 auto-resumes allowed per full run)

This works but adds complexity, risks losing progress on pod evictions (the pod was evicted mid-run in build 241 due to node memory pressure), and limits reliability.

### Why ECS Fargate Fixes This
- **No agent timeout** — ECS Fargate tasks run until the job finishes, no 1-hour limit
- **No pod evictions** — Fargate is serverless; there are no nodes that can run out of memory and evict containers
- **No node management** — no EKS node groups to maintain
- **On-demand billing** — cost only for actual build time, no idle node cost

---

## Current Jenkins Infrastructure

| Component | Current State |
|---|---|
| Jenkins controller | CloudBees Jenkins on EKS |
| Agent type | Kubernetes pod (via K8s plugin) |
| Agent image | `python:3.9` + `amazon/aws-cli` + `eclipse-temurin:17-jre` |
| AWS auth | IRSA (pod service account `jenkins-role`) |
| Region | `us-east-1` |
| AWS Account | `448049811908` |
| IAM role used | `arn:aws:iam::448049811908:role/mtfpm-test-automation-execution-role` |

---

## What Needs to Be Built

### 1. Custom Docker Image (ECR)
A single container image that replaces the 3-container K8s pod setup. Must include:
- Jenkins JNLP inbound agent (base: `jenkins/inbound-agent:latest-jdk17`)
- Python 3.9
- pip packages from `requirements.txt`:
  ```
  pandas, faker, pyarrow, boto3, requests, tqdm, pyodbc, openpyxl, pytest, allure-pytest, tabulate
  ```
- AWS CLI v2
- Java 17 JRE (for Allure report generation)

**ECR repo name suggestion:** `jenkins-agent-etl`

### 2. ECS Fargate Cluster
- Cluster name: `jenkins-agents`
- Fargate (serverless) only — no EC2 instances

### 3. ECS Task Definition
- Family name: `jenkins-agent`
- Launch type: Fargate
- CPU: 1 vCPU minimum (recommend 2 vCPU for pipeline workload)
- Memory: 3–4 GB
- Container: the ECR image above
- IAM: task execution role with ECR pull permissions

### 4. IAM — Critical Requirement
The Fargate task needs the **same AWS permissions** the K8s pod currently gets via IRSA:
- Ability to assume role `arn:aws:iam::448049811908:role/mtfpm-test-automation-execution-role`
- S3 read/write (for checkpoints)
- SecretsManager or SSM access if used for DB credentials

Current K8s approach uses a pod service account (`jenkins-role`) with IRSA.
Fargate equivalent: attach an **ECS Task Role** (separate from execution role) with the same permissions.

### 5. Jenkins ECS Plugin Configuration
- Plugin: **Amazon Elastic Container Service (ECS) / Fargate**
- Cloud name: `fargate-cloud`
- ECS Cluster: `jenkins-agents`
- Agent label: `fargate-agent`
- No timeout configuration needed (that's the whole point)

### 6. Jenkinsfile Change
Replace the `agent` block:

```groovy
// REMOVE THIS:
agent {
    kubernetes {
        yaml '''
apiVersion: v1
kind: Pod
spec:
    serviceAccountName: jenkins-role
    restartPolicy: Never
    containers:
        - name: python
          image: "python:3.9"
          ...
        - name: awscli
          ...
        - name: java
          ...
'''
    }
}

// REPLACE WITH:
agent {
    label 'fargate-agent'
}
```

Also: once Fargate is confirmed working with a full uninterrupted run, the checkpoint chaining logic (CHECKPOINT_ID, RESUME_COUNT, MAX_RESUME_COUNT parameters and associated stage logic) can optionally be simplified or removed — it will no longer be needed.

---

## Decisions Already Made

| Decision | Choice |
|---|---|
| ECS Fargate vs EKS | **ECS Fargate** (serverless, no node mgmt) |
| Provisioning method | **AWS Console** (manual, step-by-step) |
| Base image | `jenkins/inbound-agent` + Python 3.9 layered on top |
| Existing K8s cluster | Keep running (Jenkins controller is still on EKS) — only agents move to Fargate |

---

## Files of Interest

| File | Purpose |
|---|---|
| `Jenkinsfile` | Full pipeline definition — agent block needs updating |
| `requirements.txt` | Python deps to bake into Docker image |
| `checkpoint_manager.py` | S3-backed checkpoint logic (can be simplified post-migration) |
| `checkpoint_helper.py` | Checkpoint helper utilities |
| `tests/conftest.py` | pytest session config, checkpoint integration |
| `DM_bankfile_validate_pipeline.py` | Main pipeline runner called by each test |

---

## Success Criteria

1. Jenkins build using `fargate-agent` label spins up an ECS Fargate task
2. Task connects back to Jenkins controller via JNLP
3. Full test suite completes in a single build without hitting any timeout
4. AWS assume-role (`mtfpm-test-automation-execution-role`) works from within the Fargate task
5. S3 checkpoint reads/writes work from the Fargate task
6. Allure reports generate correctly (requires Java 17 in the container)

---

## Questions the Setup Session Should Answer

1. What subnets should the Fargate tasks run in? (private subnets with NAT Gateway recommended — tasks need outbound internet to reach Jenkins controller and AWS APIs)
2. Does the Jenkins controller have a public or private endpoint? (affects whether tasks can reach it)
3. Will IRSA still apply, or switch to ECS Task Role with explicit IAM policy? (ECS Fargate supports IRSA if EKS is the cluster — but since agents are moving to ECS, a native ECS Task Role is simpler)
4. Is there a VPC already in use that the Fargate tasks should join?
