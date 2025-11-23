# Stage 1: Build Frontend
FROM node:18-alpine as frontend-build
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

# Stage 2: Build Backend and Final Image
FROM python:3.10-slim
WORKDIR /app

# Install system dependencies if needed (e.g. for pyarrow/datafusion)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Backend Code
COPY backend/ ./backend/

# Copy Built Frontend to Backend Static Directory
COPY --from=frontend-build /app/frontend/dist ./backend/static

# Expose Port
EXPOSE 8000

# Run Application
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
