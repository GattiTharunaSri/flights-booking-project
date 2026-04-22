# ✈️ Flight Booking Analytics Pipeline

A production-grade data engineering pipeline on **Google Cloud Platform** that processes flight booking data using **Apache Airflow (Cloud Composer)**, **Dataproc Serverless (PySpark)**, and **BigQuery**. The entire CI/CD workflow is automated via **GitHub Actions** with **Workload Identity Federation (WIF)** for secure, keyless authentication.

> 📌 **Note:** All GCP infrastructure for this project was set up using the **Google Cloud Console (UI)** — no command-line tools required. Instructions below reflect the UI-based workflow.

---

## 📋 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Pipeline Workflow](#-pipeline-workflow)
- [Data Transformations](#-data-transformations)
- [Prerequisites](#-prerequisites)
- [GCP Setup (via Console)](#-gcp-setup-via-console)
- [GitHub Setup](#-github-setup)
- [Deployment](#-deployment)
- [Environments](#-environments)
- [Running the Pipeline](#-running-the-pipeline)
- [Monitoring & Troubleshooting](#-monitoring--troubleshooting)
- [Security Best Practices](#-security-best-practices)
- [Future Enhancements](#-future-enhancements)

---

## 🏗️ Architecture Overview

```
┌──────────────────┐       ┌─────────────────┐       ┌──────────────────┐
│   GitHub Repo    │──────▶│  GitHub Actions │──────▶│   GCP Services   │
│  (Source Code)   │       │   (CI/CD via    │  WIF  │                  │
└──────────────────┘       │      WIF)       │       │  ┌────────────┐  │
                           └─────────────────┘       │  │    GCS     │  │
                                                     │  │  (Buckets) │  │
                                                     │  └─────┬──────┘  │
                                                     │        │         │
                                                     │        ▼         │
                                                     │  ┌────────────┐  │
                                                     │  │   Cloud    │  │
                                                     │  │  Composer  │  │
                                                     │  │ (Airflow)  │  │
                                                     │  └─────┬──────┘  │
                                                     │        │         │
                                                     │        ▼         │
                                                     │  ┌────────────┐  │
                                                     │  │  Dataproc  │  │
                                                     │  │ Serverless │  │
                                                     │  │  (Spark)   │  │
                                                     │  └─────┬──────┘  │
                                                     │        │         │
                                                     │        ▼         │
                                                     │  ┌────────────┐  │
                                                     │  │  BigQuery  │  │
                                                     │  │ (Analytics)│  │
                                                     │  └────────────┘  │
                                                     └──────────────────┘
```

---

## 🛠️ Tech Stack

| Category | Technology |
|---|---|
| **Orchestration** | Apache Airflow (via Cloud Composer) |
| **Processing** | Dataproc Serverless (PySpark 3.x) |
| **Data Warehouse** | BigQuery |
| **Storage** | Google Cloud Storage (GCS) |
| **CI/CD** | GitHub Actions |
| **Authentication** | Workload Identity Federation (WIF) |
| **Language** | Python 3.11 |
| **Version Control** | Git + GitHub |

---

## 📁 Project Structure

```
flights-booking-project/
│
├── .github/
│   └── workflows/
│       └── ci-cd.yaml                 # GitHub Actions CI/CD pipeline
│
├── airflow_job/
│   └── airflow_job.py                 # Airflow DAG definition
│
├── spark_job/
│   └── spark_transformation_job.py    # PySpark transformation logic
│
├── variables/
│   ├── dev/
│   │   └── variables.json             # Dev environment variables
│   └── prod/
│       └── variables.json             # Prod environment variables
│
├── data/
│   └── flight_booking.csv             # Sample dataset
│
└── README.md                          # You are here
```

---

## 🔄 Pipeline Workflow

### 1. Developer pushes code to GitHub
The workflow triggers on pushes to either the `dev` or `main` branch.

### 2. GitHub Actions CI/CD runs
- **Authenticates to GCP** via Workload Identity Federation (no keys!)
- **Uploads `variables.json`** to the Composer GCS bucket
- **Imports variables** into the Airflow environment
- **Uploads the Spark job** to a dedicated GCS bucket
- **Deploys the DAG** to Cloud Composer

### 3. Airflow DAG execution
- **GCSObjectExistenceSensor** waits for the input file `flight_booking.csv` in GCS
- Once detected, submits a **Dataproc Serverless batch job**

### 4. PySpark job processes the data
- Reads the CSV from GCS
- Applies transformations (derived columns, aggregations)
- Writes results to **three BigQuery tables**

### 5. Analytics available in BigQuery
Ready for downstream consumption by dashboards, ML models, or reports.

---

## 🔄 Data Transformations

The Spark job produces **three BigQuery tables**:

### 1. `transformed_table` — Enriched booking data
Adds the following derived columns to the raw data:

| Column | Logic |
|---|---|
| `is_weekend` | 1 if `flight_day` is Sat/Sun, else 0 |
| `lead_time_category` | `Last-Minute` (< 7 days), `Short-Term` (7-29 days), `Long-Term` (≥ 30 days) |
| `booking_success_rate` | `booking_complete / num_passengers` |

### 2. `route_insights_table` — Route-level aggregations
Grouped by `route`, contains:
- `total_bookings` — total count per route
- `avg_flight_duration` — average flight duration
- `avg_stay_length` — average length of stay

### 3. `origin_insights_table` — Booking origin insights
Grouped by `booking_origin`, contains:
- `total_bookings` — total count per origin country
- `success_rate` — average booking success rate
- `avg_purchase_lead` — average days between booking and flight

---

## ✅ Prerequisites

Before getting started, make sure you have:

- A **GCP Project** with billing enabled
- **Owner** or **IAM Admin** role on the project
- A **GitHub repository** (this one!)
- The following **GCP APIs enabled** (via `APIs & Services → Library`):
  - Cloud Composer API
  - Dataproc API
  - BigQuery API
  - Cloud Storage API
  - IAM Service Account Credentials API
  - Security Token Service API

---

## ☁️ GCP Setup (via Console)

All infrastructure was provisioned through the **Google Cloud Console (UI)**. Follow these steps in order.

### Step 1: Create GCS Buckets

1. Navigate to **Cloud Storage → Buckets**
2. Click **+ CREATE**
3. Create the following bucket:

| Bucket Name | Location | Storage Class |
|---|---|---|
| `aiflow-sample-project-dev` | `us-central1` | Standard |

4. Inside the bucket, create the following folder structure using **CREATE FOLDER**:
   ```
   flight-booking-analysis/
   ├── source-dev/
   ├── source-prod/
   └── spark-job/
   ```

### Step 2: Create BigQuery Datasets

1. Navigate to **BigQuery → SQL Workspace**
2. In the Explorer panel, click on your project → **⋮ (three dots)** → **Create dataset**
3. Create two datasets:

| Dataset ID | Location |
|---|---|
| `flight_data_dev` | `us-central1` |
| `flight_data_prod` | `us-central1` |

> **Note:** Tables will be auto-created by the Spark job, so you don't need to create them manually.

### Step 3: Create Cloud Composer Environments

1. Navigate to **Composer → Environments**
2. Click **+ CREATE ENVIRONMENT → Composer 2**
3. Create two environments:

| Environment Name | Location | Image Version |
|---|---|---|
| `airflow-dev` | `us-central1` | `composer-2-airflow-2` |
| `airflow-prod` | `us-central1` | `composer-2-airflow-2` |

> ⏳ **Heads up:** Composer environments take 20-25 minutes to provision.

### Step 4: Create Service Accounts

Navigate to **IAM & Admin → Service Accounts** and click **+ CREATE SERVICE ACCOUNT** for each:

| Service Account Name | Purpose |
|---|---|
| `github-actions-sa-dev` | GitHub Actions deployment (dev) |
| `github-actions-sa-prod` | GitHub Actions deployment (prod) |
| `dataproc-runtime-sa` | Dataproc Serverless runtime |

For each, fill in:
- **Service account name:** (as above)
- **Service account ID:** (auto-filled)
- **Description:** (optional)
- Click **CREATE AND CONTINUE**
- Skip role assignment here (we'll do it in Step 5)
- Click **DONE**

### Step 5: Grant IAM Roles

Navigate to **IAM & Admin → IAM** and click **+ GRANT ACCESS** for each service account.

#### For `github-actions-sa-dev` and `github-actions-sa-prod`:

| Role | Purpose |
|---|---|
| `Composer User` | Interact with Cloud Composer |
| `Storage Object Admin` | Upload files to GCS |
| `Service Account User` | Required for impersonation |

#### For `dataproc-runtime-sa`:

| Role | Purpose |
|---|---|
| `Dataproc Worker` | Run Spark jobs |
| `Storage Object Admin` | Read from & write to GCS |
| `BigQuery Data Editor` | Write to BigQuery tables |
| `BigQuery Job User` | Run BigQuery jobs |

### Step 6: Grant Composer SA Permission to Impersonate Dataproc SA

This is **critical** — without it, the Dataproc job will fail with a "not authorized to act as service account" error.

1. Go to **IAM & Admin → Service Accounts**
2. Click on **`dataproc-runtime-sa`**
3. Click the **PERMISSIONS** tab
4. Click **+ GRANT ACCESS**
5. In **New principals**, enter the Composer environment's service account:
   ```
   56749168976-compute@developer.gserviceaccount.com
   ```
6. In **Role**, select **`Service Account User`**
7. Click **SAVE**

### Step 7: Set Up Workload Identity Federation

#### 7a. Create the Workload Identity Pool

1. Navigate to **IAM & Admin → Workload Identity Federation**
2. Click **+ CREATE POOL**
3. Fill in:
   - **Name:** `github-pool`
   - **Pool ID:** `github-pool` (auto-filled)
   - **Description:** `Pool for GitHub Actions authentication`
   - **Enabled pool:** ✅ ON
4. Click **CONTINUE**

#### 7b. Add an OIDC Provider

On the "Add a provider to pool" screen:

1. Select provider type: **OpenID Connect (OIDC)**
2. Fill in:
   - **Provider name:** `github-provider`
   - **Provider ID:** `github-provider` (auto-filled)
   - **Issuer URL:** `https://token.actions.githubusercontent.com`
   - **Audiences:** Select **Default audience**
3. Click **CONTINUE**

#### 7c. Configure Provider Attributes

Under **Configure provider attributes**, add the following mappings:

| Google Attribute | OIDC Claim |
|---|---|
| `google.subject` | `assertion.sub` |
| `attribute.actor` | `assertion.actor` |
| `attribute.repository` | `assertion.repository` |
| `attribute.repository_owner` | `assertion.repository_owner` |
| `attribute.ref` | `assertion.ref` |

Under **Attribute Conditions (CEL)**, add this expression to restrict access to your GitHub org:
```
assertion.repository_owner == 'YOUR_GITHUB_ORG'
```

Click **SAVE**.

#### 7d. Grant Access to Service Accounts

After the pool is created:

1. Click on the **`github-pool`** pool
2. Click **GRANT ACCESS** at the top
3. Select: **Grant access using service account impersonation**
4. In **Service account**, select: `github-actions-sa-dev@...`
5. In **Select principals**:
   - **Principal type:** `repository`
   - **Attribute value:** `YOUR_ORG/YOUR_REPO`
6. Click **SAVE**
7. On the download config dialog, click **DISMISS**

Repeat the above for `github-actions-sa-prod`.

#### 7e. Get the Provider Resource Name

1. Inside `github-pool`, click on **`github-provider`**
2. At the top of the page, copy the **full resource name** shown next to **IAM Principal**:
   ```
   projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/providers/github-provider
   ```

   > 💡 **Tip:** To find your **PROJECT_NUMBER**, go to **IAM & Admin → Settings** — the project number is displayed at the top (numeric, not the alphanumeric ID).

You'll need this value for the GitHub repository setup.

---

## 🔐 GitHub Setup

### Add Repository Variables

Go to your GitHub repo → **Settings → Secrets and variables → Actions → Variables tab** → **New repository variable**.

Add the following variables:

| Variable Name | Value |
|---|---|
| `GCP_PROJECT_ID` | `project-03803ccd-8fd5-4d10-a60` |
| `GCP_WIF_PROVIDER` | `projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/providers/github-provider` |
| `GCP_SERVICE_ACCOUNT_DEV` | `github-actions-sa-dev@project-03803ccd-8fd5-4d10-a60.iam.gserviceaccount.com` |
| `GCP_SERVICE_ACCOUNT_PROD` | `github-actions-sa-prod@project-03803ccd-8fd5-4d10-a60.iam.gserviceaccount.com` |

### Create GitHub Environment for Production (Recommended)

1. Go to **Settings → Environments → New environment**
2. Name it `production`
3. Enable **Required reviewers** to enforce manual approval before production deploys

---

## 🚀 Deployment

### Automatic Deployment (GitHub Actions)

The pipeline automatically deploys when you push to either branch:

```bash
# Deploy to dev
git checkout dev
git add .
git commit -m "feat: update pipeline"
git push origin dev

# Deploy to prod (requires approval if enabled)
git checkout main
git merge dev
git push origin main
```

The CI/CD pipeline will:
1. Authenticate to GCP via WIF (keyless!)
2. Upload `variables.json` to the Composer GCS bucket
3. Import variables into Airflow
4. Upload the Spark job to GCS
5. Deploy the DAG to Cloud Composer

### Manual Deployment

You can also trigger the workflow manually from the **Actions tab → Flight Booking CICD → Run workflow**.

---

## 🌍 Environments

| Environment | Branch | Composer Env | GCS Bucket | BigQuery Dataset |
|---|---|---|---|---|
| **Development** | `dev` | `airflow-dev` | `us-central1-airflow-dev-5aa6e2af-bucket` | `flight_data_dev` |
| **Production** | `main` | `airflow-prod` | `us-central1-airflow-prod-7627a852-bucket` | `flight_data_prod` |

---

## ▶️ Running the Pipeline

### 1. Upload Input Data to GCS

Via the **GCP Console**:
1. Go to **Cloud Storage → Buckets → `aiflow-sample-project-dev`**
2. Navigate to `flight-booking-analysis/source-dev/`
3. Click **UPLOAD FILES** and upload `flight_booking.csv`

### 2. Trigger the DAG

1. Open the **Airflow UI** from **Composer → Environments → airflow-dev → Airflow webserver link**
2. Find the DAG: `flight_booking_dataproc_bq_dag`
3. Toggle it **ON** if not already
4. Click the **▶ Play** button → **Trigger DAG**

### 3. Monitor Execution

- **File Sensor task** — waits for the CSV (up to 5 min timeout)
- **Dataproc task** — submits Spark batch, typically completes in 3-5 minutes

### 4. Verify Output in BigQuery

Go to **BigQuery → SQL Workspace** and run:
```sql
SELECT * FROM `project-03803ccd-8fd5-4d10-a60.flight_data_dev.transformed_table` LIMIT 10;
SELECT * FROM `project-03803ccd-8fd5-4d10-a60.flight_data_dev.route_insights` LIMIT 10;
SELECT * FROM `project-03803ccd-8fd5-4d10-a60.flight_data_dev.origin_insights` LIMIT 10;
```

---

## 🔍 Monitoring & Troubleshooting

### Airflow UI
- **Access:** Composer → Environments → `airflow-dev` → **Airflow webserver link**
- **Logs:** Click any task in the DAG → **Logs** tab

### Dataproc Serverless
- **Console:** Dataproc → Serverless → Batches
- **Spark UI:** Available for 30 days after job completion
- **Logs:** Automatically integrated with Cloud Logging

### Cloud Logging
- **Access:** Navigate to **Logging → Logs Explorer**
- Filter by resource type or service for targeted debugging

### Common Issues

| Error | Cause | Fix |
|---|---|---|
| `User not authorized to act as service account` | Missing `iam.serviceAccountUser` role | Grant the Composer SA `Service Account User` role on the Dataproc SA (Step 6) |
| `File not found in GCS bucket` | CSV not uploaded | Upload `flight_booking.csv` to the correct bucket path |
| `Permission denied on BigQuery dataset` | Dataproc SA missing BQ roles | Grant `BigQuery Data Editor` and `BigQuery Job User` via IAM |
| `WIF token validation failed` | Attribute condition mismatch | Verify the org/repo in the WIF provider condition |
| `Composer environment not found` | Wrong env name or location | Check the environment exists in **Composer → Environments** |
| `Service account key creation disabled` | Org policy blocks keys | Use WIF (already implemented here) |

---

## 🛡️ Security Best Practices

This project follows modern GCP security best practices:

- ✅ **No long-lived service account keys** — uses Workload Identity Federation
- ✅ **Least privilege IAM** — separate SAs for deployment vs. runtime
- ✅ **Environment isolation** — separate SAs and resources for dev/prod
- ✅ **Repository-scoped WIF** — only this GitHub repo can authenticate
- ✅ **GitHub Environments** — require manual approval for prod deploys
- ✅ **Secrets management** — no credentials committed to the repo
- ✅ **Compliant with `iam.disableServiceAccountKeyCreation`** org policy

---

## 🔮 Future Enhancements

Potential improvements for the next iteration:

- [ ] Add unit tests for Spark transformations using `pytest-spark`
- [ ] Implement data quality checks (e.g., Great Expectations)
- [ ] Add dbt layer on top of BigQuery for modeling
- [ ] Add Slack/email alerting on pipeline failures
- [ ] Parameterize GCS bucket in the Spark job (currently hardcoded)
- [ ] Add Terraform for GCP infrastructure as code
- [ ] Implement incremental loading instead of full overwrite
- [ ] Add data lineage tracking via Dataplex
- [ ] Create Looker Studio dashboards on top of BigQuery tables

---

## 📝 Variables Reference

The `variables.json` file configures the Airflow DAG:

```json
{
  "env": "dev",
  "gcs_bucket": "aiflow-sample-project-dev",
  "bq_project": "project-03803ccd-8fd5-4d10-a60",
  "bq_dataset": "flight_data_dev",
  "tables": {
    "transformed_table": "transformed_flight_data",
    "route_insights_table": "route_insights",
    "origin_insights_table": "origin_insights"
  }
}
```

---

## 📜 License

This project is for learning and demonstration purposes.

---

## 👤 Author

Built as a hands-on data engineering project to demonstrate end-to-end GCP data pipeline development with modern CI/CD practices.

---

## 🙏 Acknowledgments

- **Google Cloud** — for the amazing managed services
- **Apache Airflow** community — for the excellent orchestration framework
- **Apache Spark** — for fast, distributed data processing

---

> **Note:** This pipeline processes flight booking data for analytical purposes. No actual booking transactions are performed. The dataset is synthetic/sample data.
