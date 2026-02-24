# == Base image ================================================
FROM python:3.11-slim

# == System deps ===============================================
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# == Working directory =========================================
WORKDIR /app

# == Install Python dependencies ===============================
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# == Copy generator source =====================================
COPY generate_data.py config.py ./

# == Default command ===========================================
CMD ["python", "generate_data.py"]
