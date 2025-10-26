# 🛒 Olist E-commerce Data Pipeline

A complete end-to-end data pipeline for analyzing Brazilian e-commerce data from Olist. This project includes data extraction, transformation, loading (ETL), API service, automated orchestration with Prefect, and interactive dashboards with Grafana.

## 📊 Project Overview

This pipeline processes Olist's e-commerce dataset containing customer orders, payments, and delivery information to generate business insights through automated ETL processes and real-time dashboards.

### Key Features

- ✅ **FastAPI REST API** - Serve data as JSON endpoints
- ✅ **Complete ETL Pipeline** - Extract, Transform, Load with error handling
- ✅ **MySQL Database** - Optimized schema with indexes
- ✅ **Prefect Orchestration** - Scheduled hourly execution
- ✅ **Analytics Tables** - Pre-computed metrics for fast queries
- ✅ **Production Ready** - Logging, error handling, monitoring

## 🏗️ Architecture

```
┌─────────────┐
│   CSV Data  │
│   Sources   │
└──────┬──────┘
       │
       ▼
┌─────────────┐     ┌──────────────┐
│   FastAPI   │────▶│  REST API    │
│   Service   │     │  Endpoints   │
└─────────────┘     └──────────────┘
       │
       ▼
┌─────────────┐
│  ETL        │
│  Pipeline   │
│  (Prefect)  │
└──────┬──────┘
       │
       ▼
┌─────────────┐     ┌──────────────┐
│   MySQL     │────▶│   Grafana    │
│   Database  │     │  Dashboards  │
└─────────────┘     └──────────────┘
```

## 📁 Project Structure

```
olist-pipeline/
│
├── data/                              # Data directory
│   ├── olist_customers_dataset.csv
│   ├── olist_orders_dataset.csv
│   ├── olist_order_items_dataset.csv
│   └── olist_order_payments_dataset.csv
│
├── main.py                            # FastAPI service
├── pipeline_olist.py                  # Basic ETL pipeline
├── prefect_pipeline.py                # Prefect orchestrated pipeline
├── deploy.py                          # Automated deployment script
│
├── requirements.txt                   # Python dependencies
├── README.md                          # This file
└── setup_guide.md                     # Detailed setup instructions
```

## 🚀 Quick Start

### 1. Clone and Setup

```bash
# Clone the repository (or create project directory)
mkdir olist-pipeline && cd olist-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Prepare Data

Place the following CSV files in the `data/` directory:
- `olist_customers_dataset.csv`
- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_order_payments_dataset.csv`

### 3. Configure MySQL

```sql
-- Create database
CREATE DATABASE olist_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Update credentials in the code if needed
-- Default: root/Anirudh123
```

### 4. Run Automated Deployment

```bash
# This will setup everything automatically
python deploy.py
```

OR manually:

```bash
# Run the ETL pipeline once
python pipeline_olist.py

# Start FastAPI service
uvicorn main:app --reload --port 8000

# Start Prefect server (Terminal 1)
prefect server start

# Start Prefect agent (Terminal 2)
prefect agent start -q default
```

## 📊 Database Schema

### Master Table: `olist_master`
Complete transaction data with all orders, customers, and payments.

### Analytics Tables:

1. **sales_summary** - Customer-level sales metrics
   - Total spent, orders, items per customer
   - Average order value and pricing

2. **delivery_summary** - State-level delivery performance
   - Average delivery time by state
   - Delivery success rates
   - Min/max delivery times

3. **product_summary** - Product-level performance
   - Revenue per product
   - Order frequency
   - Average pricing and freight costs

4. **state_summary** - Geographic business metrics
   - Customers and revenue by state
   - Average order values
   - Total items sold

## 🔌 API Endpoints

### Base URL: `http://localhost:8000`

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API information |
| `/customers` | GET | List all customers (paginated) |
| `/customers/by_state/{state}` | GET | Customers filtered by state |
| `/orders` | GET | List orders with optional filters |
| `/order_items` | GET | Order items data |
| `/payments` | GET | Payment information |
| `/customer/{customer_id}/orders` | GET | Orders for specific customer |
| `/stats/summary` | GET | Overall statistics |
| `/health` | GET | Health check |

### Example Requests

```bash
# Get first 10 customers
curl "http://localhost:8000/customers?limit=10"

# Get customers from São Paulo
curl "http://localhost:8000/customers/by_state/SP"

# Get summary statistics
curl "http://localhost:8000/stats/summary"
```


## ⏰ Prefect Orchestration

### Automated Schedule
The pipeline runs **every 1 hour** automatically via Prefect.

### Monitor Execution
- Prefect UI: http://localhost:4200
- View flow runs, logs, and schedules
- Check execution history and performance

### Manual Trigger
```bash
# Run flow manually
python prefect_pipeline.py

# Or via Prefect CLI
prefect deployment run olist-etl-flow/olist-etl-hourly
```

## 🔧 Configuration

### MySQL Configuration
Edit in `pipeline_olist.py` and `prefect_pipeline.py`:
```python
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "YOUR_PASSWORD",
    "database": "olist_db"
}
```

### Prefect Schedule
Modify in `prefect_pipeline.py`:
```python
schedule=IntervalSchedule(interval=timedelta(hours=1))  # Change hours as needed
```

### API Configuration
Modify in `main.py`:
```python
# Change port or host
uvicorn.run(app, host="0.0.0.0", port=8000)
```