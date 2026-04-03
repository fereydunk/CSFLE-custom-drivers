# Installing CipherTrust Manager CE on Azure

This guide walks through deploying Thales CipherTrust Manager Community Edition (CE)
on Microsoft Azure from the Marketplace, completing the first-boot setup, and creating
an AES-256 encryption key that can be used as a KEK for Confluent CSFLE.

---

## Prerequisites

- An Azure subscription with permissions to create VMs and resource groups
- The [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli) installed, or access to [Azure Cloud Shell](https://shell.azure.com)
- An SSH key pair (generate one with `ssh-keygen -t rsa -b 4096` if you don't have one)

---

## Step 1 — Deploy from Azure Marketplace

### Option A — Azure Portal (recommended for first time)

1. Go to the [Azure Portal](https://portal.azure.com) and search for **"CipherTrust Manager"** in the Marketplace.
2. Select **CipherTrust Manager - Community Edition** by Thales.
3. Click **Create** and fill in:

   | Field | Recommended value |
   |---|---|
   | Resource Group | Create new, e.g. `CIPHERTRUST-RG` |
   | VM name | `ciphertrust-ce` |
   | Region | Any region close to your Confluent Cloud cluster |
   | Size | `Standard_D4s_v3` (4 vCPU, 16 GB RAM) or larger |
   | Authentication type | **SSH public key** ← mandatory |
   | Username | `ksadmin` ← must be exactly this |
   | SSH public key | Paste your public key |
   | OS disk | Accept the image default (≥ 300 GB) — do **not** set a smaller size |
   | Public inbound ports | Allow **SSH (22)** and **HTTPS (443)** |

4. Complete the wizard and click **Review + Create → Create**.
5. Note the **Public IP address** once the VM is created.

### Option B — Azure CLI

```bash
# Set variables
RG="CIPHERTRUST-RG"
VM="ciphertrust-ce"
REGION="eastus"
SSH_KEY="$(cat ~/.ssh/id_rsa.pub)"

# Create resource group
az group create --name $RG --location $REGION

# Deploy CipherTrust Manager CE from Marketplace
az vm create \
  --resource-group $RG \
  --name $VM \
  --image "thalesdiscplusainc1596561677238:cm_ce:ciphertrust_manager_ce:latest" \
  --accept-term \
  --admin-username ksadmin \
  --ssh-key-value "$SSH_KEY" \
  --size Standard_D4s_v3 \
  --public-ip-sku Standard

# Open ports 22 and 443
az vm open-port --resource-group $RG --name $VM --port 22 --priority 1000
az vm open-port --resource-group $RG --name $VM --port 443 --priority 1001
```

---

## Step 2 — Critical: Replace the ksadmin SSH Key

> **This step is mandatory.** CipherTrust Manager CE will not start its internal
> services until the default SSH public key for `ksadmin` is replaced with your own.
> You will see this message on the serial console if you skip it:
>
> *"The CipherTrust Manager will be functional after the default SSH public key
> for the ksadmin user is replaced."*

The Azure VM extension may not inject your SSH key into the `ksadmin` account
automatically. Use the **Azure Run Command** to inject it manually:

### Via Azure Portal

1. In the portal, go to your VM → **Run command** → **RunShellScript**.
2. Paste the following (replace `<YOUR_PUBLIC_KEY>` with your actual public key):

```bash
mkdir -p /home/ksadmin/.ssh
echo "<YOUR_PUBLIC_KEY>" >> /home/ksadmin/.ssh/authorized_keys
chmod 700 /home/ksadmin/.ssh
chmod 600 /home/ksadmin/.ssh/authorized_keys
chown -R ksadmin:ksadmin /home/ksadmin/.ssh
```

3. Click **Run** and wait for it to complete.

### Via Azure CLI

```bash
az vm run-command invoke \
  --resource-group CIPHERTRUST-RG \
  --name ciphertrust-ce \
  --command-id RunShellScript \
  --scripts "
    mkdir -p /home/ksadmin/.ssh
    echo '$(cat ~/.ssh/id_rsa.pub)' >> /home/ksadmin/.ssh/authorized_keys
    chmod 700 /home/ksadmin/.ssh
    chmod 600 /home/ksadmin/.ssh/authorized_keys
    chown -R ksadmin:ksadmin /home/ksadmin/.ssh
  "
```

After the key is injected, CipherTrust Manager's Docker services will start automatically.
This takes **2–5 minutes**. You can verify by SSHing in and running `docker ps`.

```bash
ssh ksadmin@<VM_PUBLIC_IP>
docker ps   # should show several running ciphertrust containers
```

---

## Step 3 — First Login and Password Setup

Once the services are running, open a browser and navigate to:

```
https://<VM_PUBLIC_IP>
```

> The VM uses a **self-signed TLS certificate**. Accept the browser warning to proceed.

### Set the admin password

1. Log in with the default credentials:
   - **Username:** `admin`
   - **Password:** `admin`

2. You will be **immediately prompted to change the password**. Choose a strong password
   and save it securely. This is the master admin password for the CipherTrust Manager.

3. After setting the password, you are logged in to the CipherTrust Manager web UI.

> **Save your credentials.** If you lose the admin password, recovery requires
> VM console access.

---

## Step 4 — Create an AES-256 Encryption Key (KEK)

The encryption key you create here will be used as the Key Encryption Key (KEK)
in the CSFLE schema rules.

### Via the Web UI

1. In the CipherTrust Manager UI, go to **Keys → Add Key**.
2. Fill in:
   - **Name:** e.g. `my-aes256-key`
   - **Algorithm:** `AES`
   - **Size:** `256`
   - **Usage:** Encrypt / Decrypt
3. Click **Create**.

### Via the REST API

You can also create the key programmatically. First, obtain an auth token:

```bash
CT_URL="https://<VM_PUBLIC_IP>"
CT_PASSWORD="<your-admin-password>"

TOKEN=$(curl -sk -X POST "$CT_URL/api/v1/auth/tokens" \
  -H "Content-Type: application/json" \
  -d '{"grant_type":"password","username":"admin","password":"'"$CT_PASSWORD"'"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['jwt'])")

echo "Token acquired."
```

Then create the key:

```bash
KEK_NAME="my-aes256-key"

curl -sk -X POST "$CT_URL/api/v1/vault/keys2" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"$KEK_NAME\",
    \"algorithm\": \"AES\",
    \"size\": 256,
    \"usageMask\": 12,
    \"undeletable\": false,
    \"unexportable\": false
  }" | python3 -m json.tool
```

A successful response includes the key `id` (UUID) and `name`. Note these down.

### Verify the key exists

```bash
curl -sk "$CT_URL/api/v1/vault/keys2?name=$KEK_NAME" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -m json.tool
```

---

## Step 5 — Record Your Configuration

Save the following values — you'll need them as environment variables when running
the producer/consumer:

```bash
CT_URL=https://<VM_PUBLIC_IP>
CT_USERNAME=admin
CT_PASSWORD=<your-admin-password>
CT_KEK_NAME=<your-key-name>       # e.g. my-aes256-key
```

These map directly to the variables in `.env.example` at the repo root.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Web UI unreachable at port 443 | Services haven't started yet | Wait 5 min after injecting SSH key; check `docker ps` via SSH |
| `docker ps` shows no containers | SSH key not injected into `ksadmin` | Re-run Step 2 |
| Serial console shows "CipherTrust Manager will be functional after..." | Same as above | Re-run Step 2 |
| Browser shows SSL error | Self-signed certificate | Accept the browser warning; this is expected for CE |
| API returns 401 | Token expired (tokens last 5 min) | Re-run the token acquisition curl command |
| `az vm create` fails with disk size error | Image requires ≥ 300 GB OS disk | Remove `--os-disk-size-gb` and let Azure use the image default |

---

## What's next

Once CipherTrust Manager is running and your KEK is created, set your environment
variables (see `.env.example` in the repo root) and follow the [Java](./java/README.md)
or [Python](./python/README.md) instructions to run the CSFLE producer and consumer.
