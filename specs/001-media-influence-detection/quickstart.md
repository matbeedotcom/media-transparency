# Quickstart: MITDS Development Setup

**Branch**: `001-media-influence-detection` | **Date**: 2026-01-26

## Prerequisites

- Python 3.11+
- Node.js 20+
- Docker & Docker Compose
- Git

## 1. Clone and Setup

```bash
# Clone repository
git clone <repo-url>
cd media_transparency

# Create Python virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# or: .venv\Scripts\activate  # Windows

# Install backend dependencies
cd backend
pip install -e ".[dev]"

# Install frontend dependencies
cd ../frontend
npm install
```

## 2. Environment Configuration

```bash
# Copy example env file
cp .env.example .env
```

Edit `.env` with your credentials:

```env
# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=mitds
POSTGRES_USER=mitds
POSTGRES_PASSWORD=your-secure-password

# Neo4j
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your-secure-password

# Redis
REDIS_URL=redis://localhost:6379/0

# Data Sources (get your own API keys)
OPENCORPORATES_API_KEY=your-key
META_APP_ID=your-app-id
META_APP_SECRET=your-secret
META_ACCESS_TOKEN=your-token

# S3-compatible storage (for raw files)
S3_ENDPOINT=http://localhost:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_BUCKET=mitds-raw
```

## 3. Start Infrastructure

```bash
# Start PostgreSQL, Neo4j, Redis, MinIO
docker-compose up -d

# Wait for services to be healthy
docker-compose ps

# Initialize databases
./infrastructure/scripts/init-db.sh
```

**Docker Compose services:**

| Service | Port | Purpose |
|---------|------|---------|
| postgres | 5432 | Event store, metadata |
| neo4j | 7474 (HTTP), 7687 (Bolt) | Graph database |
| redis | 6379 | Task queue, cache |
| minio | 9000, 9001 (console) | S3-compatible storage |

## 4. Run Database Migrations

```bash
cd backend

# Run PostgreSQL migrations
alembic upgrade head

# Neo4j constraints are created on first startup
```

## 5. Start Development Servers

**Backend API:**

```bash
cd backend
uvicorn src.main:app --reload --port 8000
```

API will be available at `http://localhost:8000`
OpenAPI docs at `http://localhost:8000/docs`

**Frontend:**

```bash
cd frontend
npm run dev
```

Frontend will be available at `http://localhost:5173`

**Celery Worker (for background tasks):**

```bash
cd backend
celery -A src.mitds.worker worker --loglevel=info
```

**Celery Beat (for scheduled ingestion):**

```bash
cd backend
celery -A src.mitds.worker beat --loglevel=info
```

## 6. Verify Setup

```bash
# Check API health
curl http://localhost:8000/health

# Check ingestion status
curl http://localhost:8000/api/v1/ingestion/status

# Access Neo4j browser
open http://localhost:7474  # Login: neo4j / your-password

# Access MinIO console
open http://localhost:9001  # Login: minioadmin / minioadmin
```

## 7. Load Sample Data (Optional)

```bash
cd backend

# Load test fixtures
python -m mitds.cli.ingest --source fixtures --path tests/fixtures/

# Or trigger a small IRS 990 sample
python -m mitds.cli.ingest --source irs990 --limit 100
```

## CLI Commands

All ingestion and analysis operations are available via CLI:

```bash
# Ingestion
python -m mitds.cli.ingest --source irs990 [--incremental] [--limit N]
python -m mitds.cli.ingest --source cra [--incremental]
python -m mitds.cli.ingest --source opencorporates --query "Foundation"
python -m mitds.cli.ingest --source meta_ads --country US

# Entity resolution
python -m mitds.cli.resolve --run-matching
python -m mitds.cli.resolve --review-queue

# Analysis
python -m mitds.cli.analyze --temporal --outlets outlet1,outlet2
python -m mitds.cli.analyze --funding-clusters
python -m mitds.cli.analyze --infrastructure

# Reports
python -m mitds.cli.report --template structural_risk --output report.html
```

## Running Tests

```bash
cd backend

# Unit tests
pytest tests/unit -v

# Integration tests (requires running Docker services)
pytest tests/integration -v

# Contract tests
pytest tests/contract -v

# All tests with coverage
pytest --cov=src/mitds --cov-report=html
```

```bash
cd frontend

# Unit tests
npm test

# E2E tests (requires running backend)
npm run test:e2e
```

## Common Issues

### Neo4j connection refused

```bash
# Check Neo4j is running
docker-compose logs neo4j

# May need to wait for initial startup (30-60s)
docker-compose restart neo4j
```

### PostgreSQL migration errors

```bash
# Reset database
docker-compose down -v
docker-compose up -d postgres
./infrastructure/scripts/init-db.sh
alembic upgrade head
```

### Meta API authentication

1. Create Facebook App at https://developers.facebook.com
2. Request Ads Transparency API access
3. Generate long-lived system user token
4. Add token to `.env`

### OpenCorporates rate limits

Free tier: 500 calls/month. For development:
- Use cached fixtures: `tests/fixtures/opencorp/`
- Mock API in tests: `pytest --mock-apis`

## Project Structure

```
media_transparency/
├── backend/
│   ├── src/mitds/        # Main package
│   ├── tests/            # Test suites
│   ├── migrations/       # Alembic migrations
│   └── pyproject.toml
├── frontend/
│   ├── src/              # React app
│   └── tests/
├── infrastructure/
│   ├── docker-compose.yml
│   └── scripts/
├── specs/                # Feature specifications
│   └── 001-media-influence-detection/
└── .env.example
```

## Next Steps

1. Review [data-model.md](data-model.md) for entity schemas
2. Review [contracts/api.yaml](contracts/api.yaml) for API specification
3. Review [research.md](research.md) for data source details
4. Start implementing tasks from [tasks.md](tasks.md) (after running `/speckit.tasks`)
