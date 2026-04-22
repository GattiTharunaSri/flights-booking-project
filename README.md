# ✈️ Flight Booking Analytics Pipeline

A production-grade data engineering pipeline on **Google Cloud Platform** that processes flight booking data using **Apache Airflow (Cloud Composer)**, **Dataproc Serverless (PySpark)**, and **BigQuery**. The entire CI/CD workflow is automated via **GitHub Actions** with **Workload Identity Federation (WIF)** for secure, keyless authentication.

---

## 📋 Table of Contents

- [Architecture Overview](#-architecture-overview)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Pipeline Workflow](#-pipeline-workflow)
- [Data Transformations](#-data-transformations)
- [Prerequisites](#-prerequisites)
- [GCP Setup](#-gcp-setup)
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
- The following **GCP APIs enabled**:
  - Cloud Composer API
  - Dataproc API
  - BigQuery API
  - Cloud Storage API
  - IAM Service Account Credentials API
  - Security Token Service API

---

## ☁️ GCP Setup

### Step 1: Create GCS Buckets

```bash
# Data bucket (input CSV + Spark job code)
gsutil mb -l us-central1 gs://aiflow-sample-project-dev/

# Create folder structure
gsutil cp /dev/null gs://aiflow-sample-project-dev/flight-booking-analysis/source-dev/
gsutil cp /dev/null gs://aiflow-sample-project-dev/flight-booking-analysis/source-prod/
gsutil cp /dev/null gs://aiflow-sample-project-dev/flight-booking-analysis/spark-job/
```

### Step 2: Create BigQuery Datasets

```bash
bq mk --location=us-central1 flight_data_dev
bq mk --location=us-central1 flight_data_prod
```

### Step 3: Create Cloud Composer Environments

Create two environments:
- `airflow-dev` (for development)
- `airflow-prod` (for production)

```bash
gcloud composer environments create airflow-dev \
  --location=us-central1 \
  --image-version=composer-2-airflow-2

gcloud composer environments create airflow-prod \
  --location=us-central1 \
  --image-version=composer-2-airflow-2
```

### Step 4: Create Service Accounts

```bash
PROJECT_ID="project-03803ccd-8fd5-4d10-a60"

# GitHub Actions deployment SA (per environment)
gcloud iam service-accounts create github-actions-sa-dev \
  --display-name="GitHub Actions Dev SA"

gcloud iam service-accounts create github-actions-sa-prod \
  --display-name="GitHub Actions Prod SA"

# Dedicated Dataproc runtime SA
gcloud iam service-accounts create dataproc-runtime-sa \
  --display-name="Dataproc Serverless Runtime SA"
```

### Step 5: Grant IAM Roles

**For GitHub Actions Service Accounts:**
```bash
for env in dev prod; do
  SA="github-actions-sa-${env}@${PROJECT_ID}.iam.gserviceaccount.com"

  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SA}" \
    --role="roles/composer.user"

  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SA}" \
    --role="roles/storage.objectAdmin"

  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SA}" \
    --role="roles/iam.serviceAccountUser"
done
```

**For Dataproc Runtime Service Account:**
```bash
DATAPROC_SA="dataproc-runtime-sa@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${DATAPROC_SA}" \
  --role="roles/dataproc.worker"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${DATAPROC_SA}" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${DATAPROC_SA}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${DATAPROC_SA}" \
  --role="roles/bigquery.jobUser"
```

**Allow Composer SA to impersonate Dataproc SA:**
```bash
COMPOSER_SA="56749168976-compute@developer.gserviceaccount.com"

gcloud iam service-accounts add-iam-policy-binding $DATAPROC_SA \
  --member="serviceAccount:${COMPOSER_SA}" \
  --role="roles/iam.serviceAccountUser"
```

### Step 6: Set Up Workload Identity Federation

```bash
# Create Workload Identity Pool
gcloud iam workload-identity-pools create "github-pool" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --display-name="GitHub Actions Pool"

# Create OIDC Provider
gcloud iam workload-identity-pools providers create-oidc "github-provider" \
  --project="${PROJECT_ID}" \
  --location="global" \
  --workload-identity-pool="github-pool" \
  --display-name="GitHub OIDC Provider" \
  --issuer-uri="https://token.actions.githubusercontent.com" \
  --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner,attribute.ref=assertion.ref" \
  --attribute-condition="assertion.repository_owner == 'YOUR_GITHUB_ORG'"

# Link GitHub repo to SAs
PROJECT_NUMBER=$(gcloud projects describe ${PROJECT_ID} --format="value(projectNumber)")

for env in dev prod; do
  gcloud iam service-accounts add-iam-policy-binding \
    "github-actions-sa-${env}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/iam.workloadIdentityUser" \
    --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/github-pool/attribute.repository/YOUR_ORG/YOUR_REPO"
done
```

---

## 🔐 GitHub Setup

### Add Repository Variables

Go to **Settings → Secrets and variables → Actions → Variables** and add:

| Variable Name | Value |
|---|---|
| `GCP_PROJECT_ID` | `project-03803ccd-8fd5-4d10-a60` |
| `GCP_WIF_PROVIDER` | `projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/github-pool/providers/github-provider` |
| `GCP_SERVICE_ACCOUNT_DEV` | `github-actions-sa-dev@PROJECT.iam.gserviceaccount.com` |
| `GCP_SERVICE_ACCOUNT_PROD` | `github-actions-sa-prod@PROJECT.iam.gserviceaccount.com` |

### Create GitHub Environment for Production (Optional but Recommended)

1. **Settings → Environments → New Environment → `production`**
2. Enable **Required reviewers** to enforce manual approval before prod deploys

---

## 🚀 Deployment

### Automatic Deployment (GitHub Actions)

Simply push to the appropriate branch:

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

The CI/CD pipeline will automatically:
1. Authenticate to GCP via WIF
2. Upload the Spark job to GCS
3. Deploy the DAG to Composer
4. Import Airflow variables

### Manual Deployment

If needed, you can manually deploy by running the workflow from the **Actions** tab → **Flight Booking CICD** → **Run workflow**.

---

## 🌍 Environments

| Environment | Branch | Composer Env | GCS Bucket | BigQuery Dataset |
|---|---|---|---|---|
| **Development** | `dev` | `airflow-dev` | `us-central1-airflow-dev-5aa6e2af-bucket` | `flight_data_dev` |
| **Production** | `main` | `airflow-prod` | `us-central1-airflow-prod-7627a852-bucket` | `flight_data_prod` |

---

## ▶️ Running the Pipeline

### 1. Upload Input Data
Place the `flight_booking.csv` file in the expected GCS location:
```bash
gsutil cp data/flight_booking.csv \
  gs://aiflow-sample-project-dev/flight-booking-analysis/source-dev/
```

### 2. Trigger the DAG
Go to the **Airflow UI** (via Composer) → **DAGs** → `flight_booking_dataproc_bq_dag` → **Trigger DAG**.

### 3. Monitor Execution
- **File Sensor task** — waits for CSV (up to 5 min timeout)
- **Dataproc task** — submits Spark batch, typically completes in 3-5 minutes

### 4. Verify Output
Check BigQuery:
```sql
SELECT * FROM `project-03803ccd-8fd5-4d10-a60.flight_data_dev.transformed_table` LIMIT 10;
SELECT * FROM `project-03803ccd-8fd5-4d10-a60.flight_data_dev.route_insights` LIMIT 10;
SELECT * FROM `project-03803ccd-8fd5-4d10-a60.flight_data_dev.origin_insights` LIMIT 10;
```

---

## 🔍 Monitoring & Troubleshooting

### Airflow UI
- **URL:** Available through the Cloud Composer console
- **Logs:** Click any task → **Logs** tab

### Dataproc Serverless
- **Console:** Dataproc → Serverless → Batches
- **Spark UI:** Available for 30 days after job completion
- **Logs:** Cloud Logging (integrated with the console)

### Common Issues

| Error | Cause | Fix |
|---|---|---|
| `User not authorized to act as service account` | Missing `iam.serviceAccountUser` role | Grant the Composer SA `Service Account User` role on the Dataproc SA |
| `File not found in GCS bucket` | CSV not uploaded | Upload `flight_booking.csv` to the correct bucket path |
| `Permission denied on BigQuery dataset` | Dataproc SA missing BQ roles | Grant `bigquery.dataEditor` and `bigquery.jobUser` |
| `WIF token validation failed` | Attribute condition mismatch | Check the org/repo in the WIF provider condition |
| `Composer environment not found` | Wrong env name or location | Verify with `gcloud composer environments list` |

---

## 🛡️ Security Best Practices

This project follows modern GCP security best practices:

- ✅ **No long-lived service account keys** — uses Workload Identity Federation
- ✅ **Least privilege IAM** — separate SAs for deployment vs. runtime
- ✅ **Environment isolation** — separate SAs and resources for dev/prod
- ✅ **Repository-scoped WIF** — only this GitHub repo can authenticate
- ✅ **GitHub Environments** — require manual approval for prod deploys
- ✅ **Secrets management** — no secrets committed to the repo
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
