from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
from typing import Optional
import os

# Create FastAPI instance
app = FastAPI(
    title="Olist E-commerce Data API",
    version="2.0",
    description="API for accessing Olist Brazilian E-commerce dataset"
)

# Load all CSV files
DATA_DIR = "data"

try:
    df_customers = pd.read_csv(f"{DATA_DIR}/olist_customers_dataset.csv")
    df_orders = pd.read_csv(f"{DATA_DIR}/olist_orders_dataset.csv")
    df_order_items = pd.read_csv(f"{DATA_DIR}/olist_order_items_dataset.csv")
    df_payments = pd.read_csv(f"{DATA_DIR}/olist_order_payments_dataset.csv")
except FileNotFoundError as e:
    print(f"Error loading data files: {e}")
    df_customers = pd.DataFrame()
    df_orders = pd.DataFrame()
    df_order_items = pd.DataFrame()
    df_payments = pd.DataFrame()

# Root endpoint
@app.get("/")
def home():
    """Welcome message with available endpoints."""
    return {
        "message": "Welcome to the Olist E-commerce API!",
        "version": "2.0",
        "available_endpoints": {
            "customers": "/customers",
            "customers_by_state": "/customers/by_state/{state}",
            "orders": "/orders",
            "order_items": "/order_items",
            "payments": "/payments",
            "customer_orders": "/customer/{customer_id}/orders",
            "stats": "/stats/summary"
        }
    }

# Customer endpoints
@app.get("/customers")
def get_customers(limit: Optional[int] = 100, offset: Optional[int] = 0):
    """Returns paginated customer data."""
    if df_customers.empty:
        raise HTTPException(status_code=503, detail="Customer data not available")
    
    total = len(df_customers)
    data = df_customers.iloc[offset:offset+limit]
    
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "data": data.to_dict(orient="records")
    }

@app.get("/customers/by_state/{state}")
def get_customers_by_state(state: str):
    """Returns customers filtered by state."""
    if df_customers.empty:
        raise HTTPException(status_code=503, detail="Customer data not available")
    
    filtered = df_customers[df_customers["customer_state"] == state.upper()]
    
    if filtered.empty:
        return {"message": f"No customers found in state: {state}", "data": []}
    
    return {
        "state": state.upper(),
        "count": len(filtered),
        "data": filtered.to_dict(orient="records")
    }

# Orders endpoint
@app.get("/orders")
def get_orders(limit: Optional[int] = 100, status: Optional[str] = None):
    """Returns order data with optional status filter."""
    if df_orders.empty:
        raise HTTPException(status_code=503, detail="Orders data not available")
    
    data = df_orders
    if status:
        data = data[data["order_status"] == status]
    
    return {
        "total": len(data),
        "limit": limit,
        "data": data.head(limit).to_dict(orient="records")
    }

# Order items endpoint
@app.get("/order_items")
def get_order_items(order_id: Optional[str] = None, limit: Optional[int] = 100):
    """Returns order items, optionally filtered by order_id."""
    if df_order_items.empty:
        raise HTTPException(status_code=503, detail="Order items data not available")
    
    data = df_order_items
    if order_id:
        data = data[data["order_id"] == order_id]
    
    return {
        "total": len(data),
        "limit": limit,
        "data": data.head(limit).to_dict(orient="records")
    }

# Payments endpoint
@app.get("/payments")
def get_payments(payment_type: Optional[str] = None, limit: Optional[int] = 100):
    """Returns payment data with optional payment type filter."""
    if df_payments.empty:
        raise HTTPException(status_code=503, detail="Payments data not available")
    
    data = df_payments
    if payment_type:
        data = data[data["payment_type"] == payment_type]
    
    return {
        "total": len(data),
        "limit": limit,
        "data": data.head(limit).to_dict(orient="records")
    }

# Customer orders endpoint
@app.get("/customer/{customer_id}/orders")
def get_customer_orders(customer_id: str):
    """Returns all orders for a specific customer."""
    if df_orders.empty:
        raise HTTPException(status_code=503, detail="Orders data not available")
    
    customer_orders = df_orders[df_orders["customer_id"] == customer_id]
    
    if customer_orders.empty:
        return {"message": f"No orders found for customer: {customer_id}", "data": []}
    
    return {
        "customer_id": customer_id,
        "order_count": len(customer_orders),
        "data": customer_orders.to_dict(orient="records")
    }

# Summary statistics endpoint
@app.get("/stats/summary")
def get_summary_stats():
    """Returns overall summary statistics."""
    stats = {
        "total_customers": len(df_customers) if not df_customers.empty else 0,
        "total_orders": len(df_orders) if not df_orders.empty else 0,
        "total_order_items": len(df_order_items) if not df_order_items.empty else 0,
        "total_payments": len(df_payments) if not df_payments.empty else 0
    }
    
    if not df_orders.empty and "order_status" in df_orders.columns:
        stats["order_status_breakdown"] = df_orders["order_status"].value_counts().to_dict()
    
    if not df_payments.empty and "payment_value" in df_payments.columns:
        stats["total_revenue"] = float(df_payments["payment_value"].sum())
        stats["average_payment"] = float(df_payments["payment_value"].mean())
    
    if not df_customers.empty and "customer_state" in df_customers.columns:
        stats["customers_by_state"] = df_customers["customer_state"].value_counts().head(10).to_dict()
    
    return stats

# Health check endpoint
@app.get("/health")
def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "data_loaded": {
            "customers": not df_customers.empty,
            "orders": not df_orders.empty,
            "order_items": not df_order_items.empty,
            "payments": not df_payments.empty
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)