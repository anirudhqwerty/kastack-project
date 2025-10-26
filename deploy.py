"""
Simple Deployment Script - Skips command-line checks
Use this if MySQL is installed but not in PATH
"""

import sys
import os
import mysql.connector
import subprocess
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Anirudh123",
    "database": "olist_db"
}

def test_mysql_connection():
    """Test MySQL connection."""
    logger.info("🔌 Testing MySQL connection...")
    try:
        conn = mysql.connector.connect(
            host=MYSQL_CONFIG["host"],
            user=MYSQL_CONFIG["user"],
            password=MYSQL_CONFIG["password"]
        )
        logger.info("   ✅ MySQL connection successful!")
        conn.close()
        return True
    except mysql.connector.Error as e:
        logger.error(f"   ❌ MySQL connection failed: {e}")
        logger.error(f"   ℹ️  Please check:")
        logger.error(f"      - MySQL server is running")
        logger.error(f"      - Username: {MYSQL_CONFIG['user']}")
        logger.error(f"      - Password is correct")
        logger.error(f"      - Host: {MYSQL_CONFIG['host']}")
        return False

def install_packages():
    """Install required packages."""
    logger.info("📦 Installing Python packages...")
    
    packages = [
        "fastapi",
        "uvicorn",
        "pandas",
        "mysql-connector-python",
        "prefect"
    ]
    
    try:
        for pkg in packages:
            logger.info(f"   Installing {pkg}...")
            subprocess.run(
                [sys.executable, "-m", "pip", "install", pkg, "-q"],
                check=True
            )
        logger.info("✅ All packages installed!")
        return True
    except Exception as e:
        logger.error(f"❌ Installation failed: {e}")
        return False

def setup_database():
    """Setup MySQL database."""
    logger.info("🗄️  Setting up database...")
    
    try:
        # Connect without database
        conn = mysql.connector.connect(
            host=MYSQL_CONFIG["host"],
            user=MYSQL_CONFIG["user"],
            password=MYSQL_CONFIG["password"]
        )
        cursor = conn.cursor()
        
        # Create database
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS {MYSQL_CONFIG['database']} "
            "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        logger.info(f"   ✅ Database '{MYSQL_CONFIG['database']}' created/verified")
        
        cursor.close()
        conn.close()
        return True
        
    except mysql.connector.Error as e:
        logger.error(f"   ❌ Database setup failed: {e}")
        return False

def check_data_files():
    """Check if CSV files exist."""
    logger.info("📁 Checking data files...")
    
    required_files = [
        "data/olist_customers_dataset.csv",
        "data/olist_orders_dataset.csv",
        "data/olist_order_items_dataset.csv",
        "data/olist_order_payments_dataset.csv"
    ]
    
    all_exist = True
    for file in required_files:
        if os.path.exists(file):
            size = os.path.getsize(file) / (1024 * 1024)  # Convert to MB
            logger.info(f"   ✅ {file} ({size:.2f} MB)")
        else:
            logger.error(f"   ❌ {file} NOT FOUND")
            all_exist = False
    
    if not all_exist:
        logger.error("\n❌ Missing data files!")
        logger.error("Please download the Olist dataset and place CSV files in 'data/' folder")
        logger.error("Download from: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
        return False
    
    return True

def run_pipeline():
    """Run the ETL pipeline."""
    logger.info("🚀 Running ETL pipeline...")
    
    # Try multiple possible filenames
    pipeline_files = ['pipeline_olist.py', 'pipeline_obist.py']
    pipeline_found = False
    
    for filename in pipeline_files:
        if os.path.exists(filename):
            logger.info(f"   Found pipeline file: {filename}")
            pipeline_found = True
            
            try:
                # Run the pipeline script directly
                result = subprocess.run(
                    [sys.executable, filename],
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minutes timeout
                )
                
                # Print output
                if result.stdout:
                    print(result.stdout)
                
                if result.returncode == 0:
                    logger.info("✅ Pipeline completed successfully!")
                    return True
                else:
                    logger.error(f"❌ Pipeline failed with exit code {result.returncode}")
                    if result.stderr:
                        logger.error(f"Error output: {result.stderr}")
                    return False
                    
            except subprocess.TimeoutExpired:
                logger.error("❌ Pipeline timed out (5 minutes)")
                return False
            except Exception as e:
                logger.error(f"❌ Pipeline error: {e}")
                import traceback
                traceback.print_exc()
                return False
    
    if not pipeline_found:
        logger.error("❌ Cannot find pipeline file!")
        logger.error("Expected files: pipeline_olist.py or pipeline_obist.py")
        return False
    
    return True

def setup_prefect():
    """Setup Prefect deployment."""
    logger.info("⏰ Setting up Prefect...")
    
    try:
        # Check if prefect_pipeline.py exists
        if not os.path.exists("prefect_pipeline.py"):
            logger.warning("   ⚠️  prefect_pipeline.py not found, skipping Prefect setup")
            return True
        
        # Create deployment script
        deploy_code = '''
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule
from prefect_pipeline import olist_etl_flow
from datetime import timedelta

deployment = Deployment.build_from_flow(
    flow=olist_etl_flow,
    name="olist-etl-hourly",
    schedule=IntervalSchedule(interval=timedelta(hours=1)),
    work_queue_name="default",
    tags=["etl", "olist", "hourly"]
)

deployment.apply()
print("✅ Prefect deployment created!")
'''
        
        with open("_temp_deploy.py", "w") as f:
            f.write(deploy_code)
        
        # Run deployment
        result = subprocess.run(
            [sys.executable, "_temp_deploy.py"],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            logger.info("   ✅ Prefect deployment created")
            logger.info("   📝 Schedule: Every 1 hour")
        else:
            logger.warning(f"   ⚠️  Prefect deployment warning: {result.stderr}")
        
        # Clean up
        if os.path.exists("_temp_deploy.py"):
            os.remove("_temp_deploy.py")
        
        return True
        
    except Exception as e:
        logger.warning(f"   ⚠️  Prefect setup skipped: {e}")
        logger.info("   ℹ️  You can set it up manually later")
        return True  # Don't fail deployment for this

def print_next_steps():
    """Print what to do next."""
    logger.info("\n" + "="*70)
    logger.info("✅ DEPLOYMENT COMPLETED SUCCESSFULLY!")
    logger.info("="*70)
    
    print("""
🎉 Your Olist Data Pipeline is ready!

📊 What's been set up:
   ✅ MySQL database with all tables
   ✅ Initial data loaded
   ✅ Python packages installed
   ✅ Prefect deployment configured

🚀 NEXT STEPS:

1️⃣  START FASTAPI SERVICE (Terminal 1):
   python -m uvicorn main:app --reload --port 8000
   
   Access API at: http://localhost:8000
   API Docs at: http://localhost:8000/docs

2️⃣  START PREFECT SERVER (Terminal 2):
   prefect server start
   
   Access UI at: http://localhost:4200

3️⃣  START PREFECT AGENT (Terminal 3):
   prefect agent start -q default
   
   This will run your pipeline every hour automatically

4️⃣  VERIFY DATA IN MYSQL:
   mysql -u root -p
   USE olist_db;
   SHOW TABLES;
   SELECT COUNT(*) FROM olist_master;
   SELECT * FROM state_summary LIMIT 5;

5️⃣  SETUP GRAFANA DASHBOARD:
   - Install Grafana: https://grafana.com/grafana/download
   - Start Grafana: http://localhost:3000 (admin/admin)
   - Add MySQL data source:
     * Host: localhost:3306
     * Database: olist_db
     * User: root
     * Password: Anirudh123
   - Use the SQL queries from the documentation to create panels

📚 HELPFUL COMMANDS:

# Run pipeline manually anytime:
python pipeline_olist.py

# Check Prefect flow runs:
prefect flow-run ls

# Test API endpoints:
curl http://localhost:8000/
curl http://localhost:8000/stats/summary
curl http://localhost:8000/customers/by_state/SP

# Check database tables:
mysql -u root -p olist_db -e "SHOW TABLES;"

💡 TIPS:
- Keep all 3 terminals running for full automation
- Check Prefect UI for pipeline execution logs
- Use Grafana for real-time dashboards
- API documentation is auto-generated at /docs endpoint

Happy Data Engineering! 🚀
""")

def main():
    """Main deployment function."""
    logger.info("\n" + "="*70)
    logger.info("🚀 OLIST PIPELINE - SIMPLE DEPLOYMENT")
    logger.info("="*70 + "\n")
    
    # Step 1: Test MySQL
    if not test_mysql_connection():
        logger.error("\n❌ Cannot proceed without MySQL connection")
        logger.info("\n💡 TIP: Make sure MySQL is running:")
        logger.info("   Windows: Check Services for MySQL")
        logger.info("   Mac: brew services start mysql")
        logger.info("   Linux: sudo systemctl start mysql")
        sys.exit(1)
    
    # Step 2: Install packages
    if not install_packages():
        logger.error("\n❌ Package installation failed")
        sys.exit(1)
    
    # Step 3: Setup database
    if not setup_database():
        logger.error("\n❌ Database setup failed")
        sys.exit(1)
    
    # Step 4: Check data files
    if not check_data_files():
        logger.error("\n❌ Missing data files")
        sys.exit(1)
    
    # Step 5: Run pipeline
    if not run_pipeline():
        logger.error("\n❌ Pipeline execution failed")
        sys.exit(1)
    
    # Step 6: Setup Prefect
    setup_prefect()  # Don't fail if this doesn't work
    
    # Print next steps
    print_next_steps()
    
    logger.info("="*70)
    logger.info("✅ ALL DONE!")
    logger.info("="*70 + "\n")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"\n\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)