# Media Influence Topology & Detection System (MITDS)

**Detect, characterize, and explain structural media manipulation using reproducible technical methods.**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![TypeScript](https://img.shields.io/badge/TypeScript-5.x-blue.svg)](https://www.typescriptlang.org/)

---

## Why This Project Exists

Modern media ecosystems are increasingly shaped by coordinated influence operations that are difficult to detect through traditional journalism alone. Networks of seemingly independent outlets may share hidden funding sources, coordinate publication timing, or operate shared infrastructure - all while appearing autonomous to the public.

**MITDS exists to make these structural patterns visible.**

This is not a content moderation tool. It does not evaluate truthfulness, classify political viewpoints, or recommend takedowns. Instead, it maps the *structural relationships* - funding flows, organizational ties, infrastructure sharing, and temporal coordination - that reveal how media ecosystems actually operate.

---

## Public Benefit

### For Investigative Journalists
- Quickly identify funding relationships behind media networks
- Detect coordinated publication patterns that manual analysis would miss
- Generate evidence-linked reports suitable for publication
- Cross-reference corporate, nonprofit, and advertising data in one place

### For Academic Researchers
- Study media influence with reproducible methods
- Access structured data on organizational relationships
- Validate findings against documented historical operations
- Contribute to open methodologies for influence detection

### For Civil Society & Policymakers
- Understand structural risks in media ecosystems
- Receive reports using neutral, non-accusatory language
- Make evidence-based decisions about media concentration
- Track changes in ownership and funding over time

### For the Public Interest
- **Transparency**: All analysis uses only public data sources
- **Reproducibility**: Any finding can be traced to source evidence
- **Accountability**: Methodology is open and auditable
- **Neutrality**: System detects structural patterns, not political positions

---

## What MITDS Detects

| Detection Type | Description | Example |
|---------------|-------------|---------|
| **Funding Clusters** | Organizations sharing common funders across seemingly independent outlets | Foundation X funds 12 outlets covering the same policy area |
| **Temporal Coordination** | Synchronized publication timing beyond normal news cycles | 8 outlets publish nearly identical narratives within a 30-minute window |
| **Infrastructure Sharing** | Common hosting, analytics, ad networks, or content management | Multiple "independent" sites share the same Google Analytics ID |
| **Entity Networks** | Board memberships, employment, and vendor relationships | The same 3 consultants appear across 15 nonprofit media organizations |

### What MITDS Does NOT Do

- Evaluate or judge the truthfulness of content
- Classify or label political viewpoints
- Recommend content removal or moderation actions
- Attribute intent or motives to detected patterns
- Access private data or platform internals

---

## Key Features

### Data Ingestion
- **IRS 990 Filings** - US nonprofit financial disclosures including grants, officers, and related organizations
- **CRA Charities** - Canadian registered charity data and qualified donee gifts
- **OpenCorporates** - Corporate registry data across jurisdictions
- **Meta Ad Library** - Political advertising spend and targeting (optional)

### Entity Resolution
- Cross-source entity matching with confidence scoring
- Human-in-the-loop review for uncertain matches
- Stable identifiers that persist across system updates
- Alias and name change tracking

### Analysis Capabilities
- Temporal coordination detection with statistical significance testing
- Funding flow tracing through intermediary organizations
- Infrastructure fingerprinting across domains
- Composite scoring requiring multiple signal types (no single signal triggers)

### Visualization & Reporting
- Interactive graph visualization of entity networks
- Timeline views of coordination patterns
- Evidence-linked reports with confidence intervals
- Non-accusatory language suitable for publication

### Validation Framework
- Golden datasets from documented influence operations
- Synthetic pattern generation for testing
- Recall, precision, and false positive rate metrics
- Dashboard for tracking detection performance

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Frontend (React)                         │
│   Dashboard │ Graph Explorer │ Timeline │ Reports │ Validation  │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                      Backend API (FastAPI)                       │
│     /entities │ /relationships │ /analysis │ /reports │ /auth   │
└─────────────────────────────────────────────────────────────────┘
         │              │              │              │
         ▼              ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│  PostgreSQL  │ │    Neo4j     │ │    Redis     │ │    MinIO     │
│  Event Store │ │ Graph Store  │ │ Task Queue   │ │ Raw Storage  │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Data Ingestion (Celery)                       │
│        IRS 990 │ CRA │ OpenCorporates │ Meta Ad Library          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 20+
- Docker & Docker Compose
- Git

### 1. Clone and Setup

```bash
git clone https://github.com/your-org/media_transparency.git
cd media_transparency

# Copy environment template
cp .env.example .env
# Edit .env with your database passwords and API keys
```

### 2. Start Infrastructure

```bash
cd infrastructure
docker-compose up -d

# Services started:
# - PostgreSQL (port 5432)
# - Neo4j (ports 7474, 7687)
# - Redis (port 6379)
# - MinIO (ports 9000, 9001)
```

### 3. Install Backend

```bash
cd backend
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e ".[dev]"

# Run migrations
alembic upgrade head
```

### 4. Install Frontend

```bash
cd frontend
npm install
```

### 5. Start Development Servers

```bash
# Terminal 1: Backend API
cd backend
python -m uvicorn src.main:app --reload --port 8001

# Terminal 2: Frontend
cd frontend
npm run dev

# Terminal 3: Background worker (optional)
cd backend
celery -A src.mitds.worker worker --loglevel=info
```

### 6. Access the Application

- **Frontend**: http://localhost:5173
- **API Docs**: http://localhost:8000/docs
- **Neo4j Browser**: http://localhost:7474
- **MinIO Console**: http://localhost:9001

---

## CLI Usage

MITDS provides a command-line interface for all major operations:

```bash
# Data Ingestion
mitds ingest irs990 --start-year 2022 --end-year 2023 --limit 1000
mitds ingest cra --incremental
mitds ingest sec-edgar --limit 100              # US public companies (free)
mitds ingest canada-corps --limit 100           # Canadian federal corporations (free)
mitds ingest opencorporates --search "Media Foundation" --max-companies 50
mitds ingest status

# Validation
mitds analyze validate --use-sample --verbose
mitds analyze validate --dataset golden_dataset.json --threshold 0.5
mitds analyze list-datasets

# See all commands
mitds --help
```

---

## Data Sources

| Source | Coverage | Refresh Rate | API Key Required |
|--------|----------|--------------|------------------|
| **SEC EDGAR** | US public companies, investment funds | Weekly | No (free) |
| **Canada Corporations** | Canadian federal corporations (CBCA, NFP) | Weekly | No (free) |
| IRS 990 | US nonprofits, 2011-present | Weekly | No |
| CRA Charities | Canadian charities | Weekly | No |
| OpenCorporates | Global corporate registries | Weekly | Yes (free tier available) |
| Meta Ad Library | Political ads (US, Canada, EU) | Daily | Yes (Facebook Developer) |

### Free Data Sources (Recommended)

**SEC EDGAR** - https://www.sec.gov/developer
- US public companies with filings (10-K, 10-Q, proxy statements)
- Officers, directors, beneficial ownership
- No registration required, just User-Agent header

**Canada Corporations** - https://open.canada.ca/
- Federal corporations under CBCA, NFP Act, Coop Act
- Directors, registered office, status
- Bulk data download, no registration required

### Paid/Limited APIs

**OpenCorporates**
1. Register at https://opencorporates.com/users/sign_up
2. Request API access at https://opencorporates.com/api_accounts/new
3. Free tier: 500 calls/month (sufficient for development)

**Meta Ad Library** (Optional)
1. Create Facebook App at https://developers.facebook.com
2. Request Ads Transparency API access
3. Generate long-lived system user token

---

## Project Structure

```
media_transparency/
├── backend/
│   ├── src/mitds/
│   │   ├── api/           # FastAPI routes
│   │   ├── cli/           # Command-line interface
│   │   ├── detection/     # Coordination detection algorithms
│   │   ├── ingestion/     # Data source connectors
│   │   ├── models/        # Database models
│   │   ├── resolution/    # Entity matching
│   │   ├── reporting/     # Report generation
│   │   └── validation/    # Testing framework
│   ├── tests/
│   └── migrations/
├── frontend/
│   ├── src/
│   │   ├── components/    # Reusable UI components
│   │   ├── pages/         # Application pages
│   │   └── services/      # API clients
│   └── tests/
├── infrastructure/
│   ├── docker-compose.yml
│   └── scripts/
├── specs/                 # Feature specifications
└── .env.example
```

---

## Performance Targets

| Metric | Target | Description |
|--------|--------|-------------|
| Detection Recall | ≥85% | Known operations correctly identified |
| False Positive Rate | ≤5% | Legitimate activity incorrectly flagged |
| Query Response | <10s | Funding relationship queries |
| Evidence Traceability | ≤3 clicks | Any finding to source evidence |

---

## Ethical Principles

MITDS is built on these foundational principles:

1. **Public Data Only** - All analysis uses publicly available information. No scraping of private data, no platform backdoors, no covert collection.

2. **Structural Analysis, Not Content Judgment** - The system detects organizational patterns, not the rightness or wrongness of viewpoints.

3. **Non-Accusatory Language** - All outputs use risk-based phrasing ("structural concentration," "temporal correlation") rather than accusatory language ("propaganda," "manipulation").

4. **Explicit Uncertainty** - Every finding includes confidence scores and explicit statements about what the system cannot determine.

5. **Reproducibility** - Any claimed finding can be regenerated from the stored evidence by independent parties.

6. **Transparency** - The methodology, algorithms, and limitations are fully documented and open to scrutiny.

---

## Running Tests

```bash
# Backend
cd backend
pytest                           # All tests
pytest tests/unit -v             # Unit tests only
pytest --cov=src/mitds           # With coverage

# Frontend
cd frontend
npm test                         # Unit tests
npm run test:e2e                 # E2E tests

# Linting
cd backend && ruff check .
cd frontend && npm run lint
```

---

## Contributing

We welcome contributions that align with the project's mission of transparency and public benefit.

### Areas for Contribution

- **Data Source Connectors** - Add support for additional public registries
- **Detection Algorithms** - Improve pattern detection with new methods
- **Visualization** - Enhance graph and timeline visualizations
- **Documentation** - Improve guides and API documentation
- **Validation Datasets** - Contribute golden datasets for testing

### Guidelines

1. All code must include tests
2. Follow existing code style (ruff for Python, ESLint for TypeScript)
3. Update documentation for user-facing changes
4. Ensure no private data or credentials in commits

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

This project draws on research and methods from:
- Academic studies of coordinated influence operations
- Open-source intelligence (OSINT) communities
- Investigative journalism organizations
- Civil society media monitoring groups

---

## Contact

- **Issues**: [GitHub Issues](https://github.com/your-org/media_transparency/issues)
- **Discussions**: [GitHub Discussions](https://github.com/your-org/media_transparency/discussions)

---

*MITDS: Making media structures visible for the public interest.*
