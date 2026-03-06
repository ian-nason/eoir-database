# EC2 Setup for Automated EOIR Database Rebuild

## Instance Requirements

- **Type:** t3.xlarge (4 vCPU, 16 GB RAM) or larger
  - The schedule table (45M rows) needs ~8 GB RAM during build
  - t3.large (8 GB) is borderline; xlarge is safe
- **Storage:** 50 GB EBS (4 GB zip + 20 GB extracted + 7 GB database + headroom)
- **OS:** Ubuntu 24.04 LTS
- **Security group:** Outbound HTTPS only (for downloading EOIR zip and uploading to HF)

## Initial Setup

```bash
# Install system dependencies
sudo apt update && sudo apt install -y python3 python3-pip unzip curl git

# Install uv
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc

# Clone the repo
cd /home/ubuntu
git clone https://github.com/ian-nason/eoir-database.git
cd eoir-database

# Install Python dependencies
uv sync

# Set up HuggingFace token
# Create a write token at https://huggingface.co/settings/tokens
echo "export HF_TOKEN=hf_your_token_here" >> ~/.bashrc
source ~/.bashrc

# Create directories
mkdir -p logs data

# Make the script executable
chmod +x scripts/auto_rebuild.sh

# Test a manual run first
./scripts/auto_rebuild.sh
```

## Set Up Cron

```bash
# Run on the 5th of every month at 4 AM UTC
crontab -e
# Add this line:
0 4 5 * * /home/ubuntu/eoir-database/scripts/auto_rebuild.sh >> /home/ubuntu/eoir-database/logs/cron.log 2>&1
```

## Monitoring

```bash
# Check recent logs
tail -f /home/ubuntu/eoir-database/logs/cron.log

# List all build logs
ls -la /home/ubuntu/eoir-database/logs/

# Verify database was updated
duckdb eoir.duckdb "SELECT MAX(OSC_DATE) FROM proceedings"

# Check HF upload timestamp
duckdb eoir.duckdb "SELECT MAX(built_at) FROM _metadata"
```

## Cost Estimate

- t3.xlarge on-demand: ~$0.17/hr
- Build takes ~10-15 min, upload ~20 min = ~$0.10/month
- 50 GB EBS: ~$4/month
- **Total: ~$4/month**

Alternatively, use a spot instance or schedule the instance to start/stop around the cron job to save the EBS cost when idle.
