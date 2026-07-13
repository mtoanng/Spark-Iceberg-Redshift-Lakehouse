# Terraform - AWS Glue Infrastructure

Infrastructure as Code for Instacart Lakehouse on AWS

---

## 📋 What This Creates

### AWS Resources

**S3:**
- 1 bucket for lakehouse storage (with versioning, encryption, lifecycle rules)

**AWS Glue:**
- 1 Glue Catalog database (`instacart_lakehouse_dev`)
- 2 Glue Jobs (Bronze ingestion, Silver transformation)
- CloudWatch log groups for job monitoring

**IAM:**
- 1 Glue service role (for running Glue Jobs)
- 1 DuckDB role (for querying via Glue Catalog)
- Policies for S3, Glue Catalog, CloudWatch

**Total Estimated Cost:** ~$5-10/month for dev (pay-per-use, depends on job frequency)

---

## 🚀 Quick Start

### 1. Prerequisites

```bash
# Install Terraform
brew install terraform  # macOS
# Or download from https://www.terraform.io/downloads

# Verify
terraform version

# Configure AWS credentials
aws configure
# Enter: Access Key, Secret Key, Region (us-east-1)
```

### 2. Configuration

```bash
cd terraform

# Copy example variables
cp terraform.tfvars.example terraform.tfvars

# Edit terraform.tfvars
nano terraform.tfvars

# IMPORTANT: Set unique S3 bucket name!
s3_bucket_name = "instacart-lakehouse-<your-initials>-<random>"
```

### 3. Deploy

```bash
# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Apply (create resources)
terraform apply

# Type "yes" when prompted
```

**Expected time:** ~2-3 minutes

### 4. Verify

```bash
# Check outputs
terraform output

# Verify S3 bucket
aws s3 ls | grep instacart

# Verify Glue database
aws glue get-database --name instacart_lakehouse_dev

# Verify Glue jobs
aws glue list-jobs | grep instacart
```

---

## 📁 File Structure

```
terraform/
├── main.tf                    # Provider, variables, outputs
├── s3.tf                      # S3 bucket configuration
├── glue_catalog.tf            # Glue Catalog database
├── glue_jobs.tf               # Glue Jobs (Bronze, Silver)
├── iam.tf                     # IAM roles and policies
├── terraform.tfvars.example   # Example variables
└── README.md                  # This file
```

---

## 🔧 Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `aws_region` | AWS region | `us-east-1` | No |
| `environment` | Environment (dev/prod) | `dev` | No |
| `project_name` | Project name prefix | `instacart-lakehouse` | No |
| `s3_bucket_name` | S3 bucket name | - | **Yes** |

---

## 📤 Outputs

After `terraform apply`, you'll get:

```
s3_bucket_name        = "instacart-lakehouse-xyz-12345"
glue_database_name    = "instacart_lakehouse_dev"
glue_role_arn         = "arn:aws:iam::123456789012:role/..."
bronze_job_name       = "instacart-lakehouse-bronze-ingestion"
silver_job_name       = "instacart-lakehouse-silver-transformation"
duckdb_role_arn       = "arn:aws:iam::123456789012:role/..."
```

**Save these!** You'll need them for:
- Uploading raw CSV files to S3
- Running Glue Jobs
- Configuring dbt-glue
- Configuring DuckDB engine

---

## 🚦 Next Steps After Deployment

### 1. Upload Raw Data to S3

```bash
# Create local raw data directory
mkdir -p raw_data

# Download Instacart dataset from Kaggle
# https://www.kaggle.com/c/instacart-market-basket-analysis/data

# Upload to S3
aws s3 cp raw_data/ s3://$(terraform output -raw s3_bucket_name)/raw/instacart/ --recursive
```

### 2. Run Bronze Ingestion

```bash
# Get job name
BRONZE_JOB=$(terraform output -raw bronze_job_name)

# Start job
aws glue start-job-run --job-name $BRONZE_JOB

# Monitor (get run ID from above command)
aws glue get-job-run --job-name $BRONZE_JOB --run-id jr_xxx

# Check CloudWatch logs
aws logs tail /aws-glue/jobs/$BRONZE_JOB --follow
```

### 3. Run Silver Transformation

```bash
# After Bronze completes
SILVER_JOB=$(terraform output -raw silver_job_name)

aws glue start-job-run --job-name $SILVER_JOB
```

### 4. Configure dbt-glue

```bash
cd ../etl/dbt_project

# Set environment variables
export GLUE_ROLE_ARN=$(cd ../../terraform && terraform output -raw glue_role_arn)
export AWS_REGION="us-east-1"
export DBT_GLUE_STAGING="s3://$(cd ../../terraform && terraform output -raw s3_bucket_name)/dbt-glue-staging/"

# Run dbt
dbt run --profiles-dir . --target glue
```

---

## 🧹 Cleanup

**WARNING:** This will delete all data and resources!

```bash
# Destroy all resources
terraform destroy

# Type "yes" when prompted

# Verify S3 bucket is deleted
aws s3 ls | grep instacart
```

**Note:** If S3 bucket has objects, you may need to empty it first:

```bash
aws s3 rm s3://$(terraform output -raw s3_bucket_name) --recursive
terraform destroy
```

---

## 💰 Cost Estimation

### Development Environment

**S3:**
- Storage: ~100 GB → $2.30/month
- Requests: Minimal → $0.50/month

**Glue Jobs:**
- Bronze: 2 DPU × 0.5 hours × $0.44/DPU-hour = $0.44/run
- Silver: 3 DPU × 1 hour × $0.44/DPU-hour = $1.32/run
- Weekly runs: ~$7.04/month

**Glue Catalog:**
- First 1M requests free
- Storage: First 1M objects free

**Total Dev:** ~$10/month (assuming weekly runs)

### Production Environment

**Adjust for:**
- More frequent runs (daily/hourly)
- Larger worker counts
- S3 lifecycle policies to archive old data

---

## 🔐 Security Best Practices

**Implemented:**
- ✅ S3 bucket encryption (AES256)
- ✅ S3 public access blocked
- ✅ IAM least-privilege policies
- ✅ CloudWatch logging enabled
- ✅ S3 versioning for data recovery

**Additional Recommendations:**
- Use AWS Secrets Manager for credentials
- Enable AWS CloudTrail for audit logs
- Set up S3 bucket policies for cross-account access
- Use VPC endpoints for Glue (avoid public internet)

---

## 🐛 Troubleshooting

### Terraform Errors

**"Error: Error creating S3 bucket: BucketAlreadyExists"**
- S3 bucket names must be globally unique
- Change `s3_bucket_name` in terraform.tfvars

**"Error: error creating Glue Job: InvalidInputException"**
- Check that script files exist in `../etl/glue_jobs/`
- Verify IAM role has correct permissions

### Glue Job Failures

**Check logs:**
```bash
aws logs tail /aws-glue/jobs/<job-name> --follow
```

**Common issues:**
- Missing raw CSV files in S3
- Incorrect S3 paths in job parameters
- Insufficient DPUs for data volume

---

## 📚 Resources

- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Apache Iceberg on AWS](https://iceberg.apache.org/docs/latest/aws/)

---

**Last Updated:** 2026-07-13  
**Terraform Version:** >= 1.0  
**AWS Provider Version:** ~> 5.0
