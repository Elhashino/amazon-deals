# Amazon UK Deals (MVP)

This is a local MVP for an Amazon UK deals website powered by Keepa price history.

## What it does
- Pulls UK deal candidates from Keepa
- Enriches products, computes a "real deal" score (discount vs 90-day median + price stability)
- Stores results in Postgres
- Serves an API with FastAPI
- Renders a simple Next.js website

## Prerequisites (Windows)
- Docker Desktop (WSL2 backend recommended)
- Python 3.11+ (3.10+ works)
- Node.js 20+

## Step-by-step (Windows PowerShell)

### 1) Start Postgres
From the project root:
```powershell
docker compose up -d
```

### 2) Backend setup
```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
notepad .env
```

Edit `backend\.env` and set:
- `KEEPA_API_KEY=...` (your real key)
- `AMAZON_ASSOC_TAG=...` (optional for now)

Run a single ingestion cycle:
```powershell
python -m app.ingestion
```

Start the API:
```powershell
uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Test:
- http://127.0.0.1:8000/health
- http://127.0.0.1:8000/api/deals?limit=20

### 3) Frontend setup
Open a new PowerShell window:
```powershell
cd frontend
npm install
copy .env.local.example .env.local
notepad .env.local
npm run dev
```

Open:
- http://localhost:3000
