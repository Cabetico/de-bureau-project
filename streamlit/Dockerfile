# ─────────────────────────────────────────────────────────────────────────────
# Streamlit BI Dashboard Dockerfile (uv-based)
# ─────────────────────────────────────────────────────────────────────────────
 
FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim
 
# Set working directory
WORKDIR /app
 
# Copy dependency file
COPY pyproject.toml ./
 
# Install dependencies using uv pip install with inline script
RUN uv pip install --system \
    streamlit>=1.28.0 \
    pandas>=2.0.0 \
    google-cloud-bigquery>=3.11.0 \
    plotly>=5.17.0 \
    google-auth>=2.23.0 \
    db-dtypes>=1.1.1 \
    folium>=0.15.0 \
    streamlit-folium>=0.15.0 \
    requests>=2.31.0
 
# Copy application code
COPY . .
 
# Expose Streamlit default port
EXPOSE 8501
 
# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl --fail http://localhost:8501/_stcore/health || exit 1
 
# Run streamlit app directly
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]