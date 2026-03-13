# MongoDB Benchmark: Azure Cosmos DB vs MongoDB Atlas

Benchmark tool comparing **Azure Cosmos DB (DocumentDB/vCore)** and **MongoDB Atlas** across common MongoDB operations: insert, update, find, aggregation, lookup, and delete.

## Features

- **19 benchmark operations** covering inserts, updates, finds, aggregations, lookups, and deletes
- **Two-phase testing**: basic indexes vs optimized (single-field + compound) indexes
- **Index impact analysis**: measures gain/loss per operation after adding indexes
- **Side-by-side comparison**: Cosmos DB vs Atlas with % difference and winner
- **Word document export**: results automatically saved to `benchmark_results.docx`
- **Configurable**: connection strings via `config.env`, adjustable user/order counts and iterations

## Operations Benchmarked

| Category | Operations |
|---|---|
| **Insert** | Bulk insert users + orders (batched) |
| **Update** | Increment field, set nested field, push to array, conditional update, update by status |
| **Find** | By indexed ID, age range, regex, projection, sort+limit, orders for user |
| **Aggregation** | Group by status, unwind items, bucket by amount, per-user stats, date breakdown |
| **Lookup** | `$lookup` join users → orders with projection |
| **Delete** | Delete all users and orders |

## Setup

```bash
# Create and activate a virtual environment
python -m venv .venv
.venv\Scripts\activate   # Windows
# source .venv/bin/activate  # macOS/Linux

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Edit `config.env` with your connection strings:

```env
COSMOSDB_URI=mongodb+srv://user:password@your-cosmos-cluster.mongocluster.cosmos.azure.com/?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000
ATLAS_URI=mongodb+srv://user:password@your-atlas-cluster.mongodb.net/
```

You can also tune the workload in `benchmark.py`:

```python
NUM_USERS = 200         # Number of user documents (~2 KB each)
ORDERS_PER_USER = 50    # Orders per user
ITERATIONS = 2          # Repetitions per operation
```

## Run

```bash
python benchmark.py
```

## Output

- **Console**: detailed per-iteration timings + comparison tables
- **Word document**: `benchmark_results.docx` with all tables:
  - Phase 1 comparison (basic indexes)
  - Phase 2 comparison (optimized indexes)
  - Index impact per service (before/after)
  - Combined index impact (Cosmos DB vs Atlas)
  - Detailed min/max/avg/median per service per phase

## Project Structure

```
mongobench/
├── benchmark.py         # Main benchmark script
├── config.env           # Connection strings (not committed)
├── requirements.txt     # Python dependencies
└── README.md            # This file
```
