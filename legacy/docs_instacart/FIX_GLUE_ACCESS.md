# 🔧 FIX: AWS Glue Access Denied Error

**Error:** `AccessDeniedException: Account 114030600807 is denied access`

**Cause:** IAM user `instacart-lakehouse-admin` thiếu AWS Glue permissions

---

## ✅ SOLUTION: Add Glue Permissions to IAM User

### **Step 1: Login AWS Console**
1. Go to: https://console.aws.amazon.com/
2. Login với root account hoặc admin user

### **Step 2: Navigate to IAM**
1. Search "IAM" in top search bar
2. Click **IAM** service

### **Step 3: Find Your User**
1. Click **Users** (left sidebar)
2. Find user: `instacart-lakehouse-admin`
3. Click on the user name

### **Step 4: Add Glue Permissions**

**Option A: Attach AWS Managed Policy (RECOMMENDED - Quickest)**

1. Click **Add permissions** → **Attach policies directly**
2. Search: `AWSGlueConsoleFullAccess`
3. ✅ Check the box
4. Click **Next**
5. Click **Add permissions**

**Option B: Create Custom Policy (More Secure)**

1. Click **Add permissions** → **Create inline policy**
2. Click **JSON** tab
3. Paste this policy:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:*"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "iam:PassRole"
            ],
            "Resource": "arn:aws:iam::114030600807:role/AWSGlueServiceRole-*"
        }
    ]
}
```

4. Click **Review policy**
5. Name: `GlueFullAccess`
6. Click **Create policy**

### **Step 5: Verify Permissions**

Run this command to verify:

```powershell
aws glue get-databases --max-results 1
```

**Expected:** Should return list of databases (or empty list if none exist)

**If still fails:** You may need to add `iam:PassRole` permission

---

## 🎯 FINAL PERMISSIONS YOUR USER NEEDS:

Your IAM user should have these policies attached:

1. ✅ **AmazonS3FullAccess** (already attached)
2. ✅ **AWSGlueConsoleFullAccess** (ADD THIS)
3. ✅ **IAMFullAccess** (already attached)
4. ✅ **CloudWatchLogsFullAccess** (already attached)

---

## 🔄 AFTER ADDING PERMISSIONS:

```powershell
# 1. Test Glue access
aws glue get-databases

# 2. Re-run Terraform
cd terraform
terraform destroy -auto-approve  # Clean up failed resources
terraform plan -out=tfplan
terraform apply tfplan
```

---

## ⚠️ ALTERNATIVE: Use Different Region

If Glue is not enabled in `ap-southeast-1`:

```powershell
# Edit terraform.tfvars or .env
aws_region = "us-east-1"  # Change from ap-southeast-1

# Re-run
terraform destroy -auto-approve
terraform plan -out=tfplan
terraform apply tfplan
```

**Recommended regions:**
- `us-east-1` (N. Virginia) - Most stable
- `us-west-2` (Oregon)
- `eu-west-1` (Ireland)

---

## 📝 SUMMARY

1. **Root cause:** IAM user missing Glue permissions
2. **Quick fix:** Attach `AWSGlueConsoleFullAccess` policy
3. **Verify:** Run `aws glue get-databases`
4. **Re-deploy:** `terraform apply tfplan`

---

## ✅ CHECKLIST

- [ ] Logged into AWS Console
- [ ] Found IAM user: `instacart-lakehouse-admin`
- [ ] Attached policy: `AWSGlueConsoleFullAccess`
- [ ] Verified with: `aws glue get-databases`
- [ ] Re-ran Terraform: `terraform apply tfplan`
- [ ] Success! Glue jobs created

---

**Last Updated:** 2026-07-17  
**Status:** Action Required - Add IAM Permissions
