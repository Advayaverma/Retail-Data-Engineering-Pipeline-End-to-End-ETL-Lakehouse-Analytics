# Docker Deployment & Containerization Guide
**Project:** Retail Data Engineering Pipeline — End-to-End Lakehouse Analytics  
**Target Role:** Celebal Technologies Data Engineer Evaluation  
**Infrastructure Stack:** Docker, Docker Compose, PostgreSQL 16 Alpine, OpenJDK 17, Python 3.11 Slim

---

## 1. Container Architecture Overview

The pipeline leverages a lightweight, isolated multi-container architecture orchestrated via `docker-compose.yml`:

```
                       +---------------------------------------+
                       |           Host Environment            |
                       +-------------------+-------------------+
                                           |
                    +----------------------+----------------------+
                    |                                             |
                    v                                             v
       +--------------------------+                 +---------------------------+
       |     retail_postgres      |                 |    retail_etl_pipeline    |
       |  (PostgreSQL 16 Alpine)  |                 |    (Python 3.11 + JRE 17) |
       +--------------------------+                 +---------------------------+
       | Port: 5432               |                 | Volumes:                  |
       | Volume: postgres_data    |<=== Network ===>|   - ./data:/app/data      |
       | Entrypoint: init.sql     |  (Healthcheck)  |   - ./logs:/app/logs      |
       | Schemas: retail_source,  |                 | Command:                  |
       |          retail_dw       |                 |   python run_pipeline.py  |
       +--------------------------+                 +---------------------------+
```

### Services Breakdown:

1. **`postgres` (`retail_postgres`)**:
   - Base image: `postgres:16-alpine` (minimal security surface area, < 80MB).
   - Auto-executes `docker/postgres/init.sql` on first boot, provisioning `retail_source` and `retail_dw` schemas.
   - Built-in Docker healthcheck (`pg_isready -U retail_admin -d retail_dw`) ensures dependent services only start when database is fully accepting connections.
   - Persistent volume `postgres_data` prevents data loss across container restarts.

2. **`etl_app` (`retail_etl_pipeline`)**:
   - Multi-layer `Dockerfile` based on `python:3.11-slim`.
   - Embeds `openjdk-17-jre-headless` required for PySpark cluster-in-a-box execution without bloated GUI packages.
   - Installs all dependencies via pinned `requirements.txt`.
   - Mounts local `./data` and `./logs` into the container to write Bronze, Silver, Delta, Gold and execution logs directly to the host filesystem.
   - Configured with `depends_on: postgres: condition: service_healthy`.

---

## 2. Quickstart: Running via Docker

### Prerequisites
- Docker Engine 24.0+
- Docker Compose v2+

### Step 1: Environment Setup
Ensure your `.env` configuration exists (or copy from `.env.example`):
```bash
cp .env.example .env
```

### Step 2: Build & Start All Services
```bash
# Build the images and start the PostgreSQL container with healthcheck
docker compose up -d postgres

# Check health status
docker compose ps
```

### Step 3: Run the Full End-to-End Pipeline
```bash
# Run the ETL pipeline container
docker compose run --rm etl_app
```

### Step 4: Run Tests Inside the Container
```bash
# Execute master test suite inside the identical container environment
docker compose run --rm etl_app python tests/run_tests.py
```

### Step 5: Teardown
```bash
# Stop containers and remove network (preserves database data volume)
docker compose down

# To completely wipe state and clean volumes:
docker compose down -v
```

---

## 3. Production Best Practices Implemented

- **Minimalist Base Images**: `python:3.11-slim` and `postgres:16-alpine` reduce container attack surface and minimize image download times.
- **Headless Java 17**: Only the JRE (`openjdk-17-jre-headless`) is installed to run PySpark executors, omitting development SDK bloat.
- **Health-Checked Dependency Resolution**: `condition: service_healthy` prevents race conditions where the ETL script tries to connect to PostgreSQL before it finishes initializing.
- **Volume Mount Parity**: Shared `./data` and `./logs` bind mounts ensure artifacts generated inside the container are instantly accessible for business reporting on the host.
- **Non-Root & Security Compliance**: Database credentials are parameterized through environment variables with secure defaults.
