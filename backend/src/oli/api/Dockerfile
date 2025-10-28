# 1. Build stage (optional but nice for speed); we’ll keep it single-stage for simplicity first.
FROM python:3.10-slim

# 2. Set workdir
WORKDIR /app

# 3. System deps (asyncpg needs libpq headers/runtime;
#    also build tools for any wheels that need compiling)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
  && rm -rf /var/lib/apt/lists/*

# 4. Copy requirements first (better layer caching)
COPY requirements.txt .

# 5. Install deps
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy the actual app code
#    Assuming api.py is in this directory.
#    If your code lives in backend/src/oli/api.py, adjust COPY and WORKDIR accordingly.
COPY . .

# 7. Expose port (not strictly required, but nice for clarity)
ENV PORT=8080

# 8. Set the start command
#    We run uvicorn pointing at FastAPI app `app` inside module `api`
CMD ["python", "api.py"]