# Azure Cloud Monitoring Setup Guide

This guide will walk you through setting up Azure Storage monitoring for Finance and Marketing departments.

---

## Part 1: Create Storage Accounts

### Step 1: Create Finance Storage Account

1. **Open Azure Portal**: https://portal.azure.com
2. **Sign in** with your Global Administrator account
3. Click **"+ Create a resource"** (top left)
4. Search for **"Storage account"** → Click **"Create"**

**Configuration:**
- **Subscription**: Select your free trial subscription
- **Resource Group**: Click "Create new" → Name: `rg-storage-monitoring`
- **Storage account name**: `stfinance<yourinitials>` (example: `stfinanceaj01`)
  - ⚠️ Must be globally unique, lowercase, no hyphens
- **Region**: Choose closest to you (e.g., "East US", "Central India")
- **Performance**: Standard
- **Redundancy**: LRS (Locally Redundant Storage) - cheapest for testing

5. Click **"Review + create"**
6. Click **"Create"**
7. Wait ~1-2 minutes for deployment
8. Click **"Go to resource"**

### Step 2: Create Containers in Finance Storage

1. In the Finance storage account, click **"Containers"** (left menu under "Data storage")
2. Click **"+ Container"** (top)
3. Create these 4 containers:

| Container Name | Anonymous Access Level |
|----------------|------------------------|
| `invoices` | Private (no anonymous access) |
| `transactions` | Private (no anonymous access) |
| `financial-reports` | Private (no anonymous access) |
| `audit-logs` | Private (no anonymous access) |

For each:
- Enter name in the **Name** field
- Keep **"Private (no anonymous access)"** selected in the dropdown
- Click **"Ok"** or **"Create"**

### Step 3: Create Marketing Storage Account

Repeat Step 1 with these changes:
- **Storage account name**: `stmarketing<yourinitials>` (example: `stmarketingaj01`)
- **Resource Group**: Use existing `rg-storage-monitoring`
- Same region, performance, redundancy

### Step 4: Create Containers in Marketing Storage

Repeat Step 2 with these containers:

| Container Name | Anonymous Access Level |
|----------------|------------------------|
| `campaigns` | Private (no anonymous access) |
| `analytics` | Private (no anonymous access) |
| `media-assets` | Private (no anonymous access) |
| `customer-data` | Private (no anonymous access) |

---

## Part 2: Create Service Principal

### Step 1: Register Application (Service Principal)

1. In Azure Portal, search for **"Microsoft Entra ID"** (formerly Azure AD) in the top search bar
2. Click on **"Microsoft Entra ID"**
3. In left menu, click **"App registrations"**
4. Click **"+ New registration"** (top)

**Configuration:**
- **Name**: `sp-storage-monitor`
- **Supported account types**: Select "Accounts in this organizational directory only"
- **Redirect URI**: Leave blank
- Click **"Register"**

5. **IMPORTANT**: Copy and save these values (you'll need them later):
   - **Application (client) ID**: (example: `12345678-1234-1234-1234-123456789abc`)
   - **Directory (tenant) ID**: (example: `87654321-4321-4321-4321-cba987654321`)

### Step 2: Create Client Secret (Password)

1. Still in the app registration page, click **"Certificates & secrets"** (left menu)
2. Click **"+ New client secret"**
3. **Description**: `monitoring-node-secret`
4. **Expires**: 24 months (maximum for free trial)
5. Click **"Add"**
6. **IMMEDIATELY COPY** the **"Value"** field (it looks like: `abc123~DEF456.ghi789_JKL012`)
   - ⚠️ **CRITICAL**: This is shown ONLY ONCE! Save it now!

**You should now have 3 values saved:**
```
Tenant ID: 87654321-4321-4321-4321-cba987654321
Client ID: 12345678-1234-1234-1234-123456789abc
Client Secret: abc123~DEF456.ghi789_JKL012
```

---

## Part 3: Grant Permissions to Service Principal

The Service Principal needs permission to read storage metrics.

### Step 1: Assign Role to Finance Storage Account

1. Go back to **Storage accounts** (search in top bar)
2. Click on your **Finance storage account** (`stfinanceaj01`)
3. Click **"Access Control (IAM)"** (left menu)
4. Click **"+ Add"** → **"Add role assignment"**

**Role Assignment:**
- **Role** tab:
  - Search for: `Storage Blob Data Reader`
  - Select it → Click **"Next"**
- **Members** tab:
  - **Assign access to**: User, group, or service principal
  - Click **"+ Select members"**
  - Search for: `sp-storage-monitor`
  - Click on it to select
  - Click **"Select"**
  - Click **"Next"**
- **Review + assign** tab:
  - Click **"Review + assign"**

### Step 2: Assign Role to Marketing Storage Account

Repeat Step 1 for the **Marketing storage account** (`stmarketingaj01`)

---

## Part 4: Get Storage Account Connection Strings

### For Finance Storage:

1. Go to Finance storage account
2. Click **"Access keys"** (left menu under "Security + networking")
3. Click **"Show"** next to "Connection string" for **key1**
4. **Copy** the entire connection string (starts with `DefaultEndpointsProtocol=https;...`)
5. Save it as: **Finance Connection String**

### For Marketing Storage:

Repeat for Marketing storage account and save as: **Marketing Connection String**

---

## Part 5: Summary - Information You Need

Before proceeding to the monitoring node setup, ensure you have:

✅ **Azure Subscription ID**: Found in Azure Portal home → Subscriptions
✅ **Tenant ID**: From Service Principal creation
✅ **Client ID**: From Service Principal creation  
✅ **Client Secret**: From Service Principal creation (only shown once!)
✅ **Finance Storage Account Name**: (e.g., `stfinanceaj01`)
✅ **Finance Connection String**: (starts with `DefaultEndpointsProtocol=...`)
✅ **Marketing Storage Account Name**: (e.g., `stmarketingaj01`)
✅ **Marketing Connection String**: (starts with `DefaultEndpointsProtocol=...`)

---

## Next Steps

Once you have all the above information:

1. Return to your monitoring node VM
2. Run the setup script (to be created next)
3. Configure credentials in `azure_config.json`
4. Test the monitoring scripts

---

## Troubleshooting

### "Storage account name already exists"
- Try adding more numbers: `stfinanceaj02`, `stfinanceaj03`, etc.
- Storage account names must be globally unique across ALL Azure

### "Client secret not showing"
- You can only see it ONCE when created
- If you missed it, create a new client secret (you can have multiple)

### "Service Principal not found"
- Make sure you searched for exact name: `sp-storage-monitor`
- Wait 1-2 minutes after creating, Azure needs time to replicate

### "Insufficient permissions"
- Ensure you're signed in as Global Administrator
- Check your free trial subscription is active

---

**Ready to proceed to monitoring node setup? Save all your credentials first!** 🔐
