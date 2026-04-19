FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim
 
WORKDIR /app
 
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
 
# Install Terraform and gcloud CLI
RUN apt-get update && \
    apt-get install -y wget unzip curl apt-transport-https ca-certificates gnupg && \
    # Install Terraform
    wget https://releases.hashicorp.com/terraform/1.11.3/terraform_1.11.3_linux_amd64.zip && \
    unzip terraform_1.11.3_linux_amd64.zip && \
    mv terraform /usr/local/bin/ && \
    rm terraform_1.11.3_linux_amd64.zip && \
    # Install gcloud CLI
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
    apt-get update && \
    apt-get install -y google-cloud-cli && \
    # Cleanup
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
 
# Verify installations
RUN terraform -v && gcloud version && bq version
 
# Copy Python dependencies
COPY ingestion/pyproject.toml ingestion/uv.lock* ./
 
# Install Python dependencies
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev
 
# Copy application files
COPY ingestion/ingestar_circulo_credito.py .
COPY ingestion/producer_cc.py .
 
# Copy Terraform configurations
COPY terraform/ ./terraform/
 
# Copy helper scripts
COPY run-terraform.sh ./
COPY destroy-bq-datasets.sh ./
RUN chmod +x run-terraform.sh destroy-bq-datasets.sh 
 
ENV PATH="/app/.venv/bin:$PATH"
 
ENTRYPOINT []
CMD ["sleep", "infinity"]
 
# ENTRYPOINT ["python"]
# CMD ["ingestar_circulo_credito.py"]