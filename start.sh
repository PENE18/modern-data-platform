#!/bin/bash

# Modern Data Platform - Automated Startup Script
# This script starts the entire data platform and verifies health

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Banner
echo -e "${BLUE}"
cat << "EOF"
╔════════════════════════════════════════════════════════════════╗
║                                                                ║
║     MODERN DATA LAKEHOUSE PLATFORM                            ║
║     Apache Iceberg + Dremio + Spark + Airflow                 ║
║                                                                ║
╚════════════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

# Check prerequisites
echo -e "${YELLOW}[1/7] Checking prerequisites...${NC}"
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo -e "${RED}Error: Docker Compose is not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker and Docker Compose are installed${NC}\n"

# Clean up old containers
echo -e "${YELLOW}[2/7] Cleaning up old containers...${NC}"
docker-compose down -v 2>/dev/null || true
echo -e "${GREEN}✓ Cleanup complete${NC}\n"

# Create necessary directories
echo -e "${YELLOW}[3/7] Creating directories...${NC}"
mkdir -p data/{raw,bronze,silver,gold}
mkdir -p airflow/logs
mkdir -p monitoring/prometheus monitoring/grafana/{dashboards,datasources}
echo -e "${GREEN}✓ Directories created${NC}\n"

# Start infrastructure services
echo -e "${YELLOW}[4/7] Starting infrastructure services...${NC}"
docker-compose up -d postgres minio minio-setup
echo "Waiting for services to be ready..."
sleep 20
echo -e "${GREEN}✓ Infrastructure services started${NC}\n"

# Start processing services
echo -e "${YELLOW}[5/7] Starting processing services...${NC}"
docker-compose up -d spark-master spark-worker-1 spark-worker-2
sleep 10
echo -e "${GREEN}✓ Processing services started${NC}\n"

# Start orchestration and analytics
echo -e "${YELLOW}[6/7] Starting orchestration and analytics...${NC}"
docker-compose up -d airflow-init
sleep 20
docker-compose up -d airflow-webserver airflow-scheduler dremio jupyter
sleep 10
echo -e "${GREEN}✓ Orchestration services started${NC}\n"

# Start monitoring
echo -e "${YELLOW}[7/7] Starting monitoring services...${NC}"
docker-compose up -d prometheus grafana
sleep 5
echo -e "${GREEN}✓ Monitoring services started${NC}\n"

# Wait for services to be fully ready
echo -e "${YELLOW}Waiting for all services to be healthy...${NC}"
sleep 30

# Check service health
echo -e "\n${BLUE}Checking service health...${NC}"
HEALTHY=true

check_service() {
    if docker ps | grep -q "$1.*Up"; then
        echo -e "  ${GREEN}✓${NC} $1"
    else
        echo -e "  ${RED}✗${NC} $1"
        HEALTHY=false
    fi
}

check_service "postgres"
check_service "minio"
check_service "spark-master"
check_service "airflow-webserver"
check_service "dremio"
check_service "jupyter"
check_service "prometheus"
check_service "grafana"

# Get Jupyter token
echo -e "\n${YELLOW}Retrieving Jupyter token...${NC}"
sleep 5
JUPYTER_TOKEN=$(docker logs jupyter 2>&1 | grep "token=" | tail -1 | sed 's/.*token=\([a-z0-9]*\).*/\1/' || echo "Check logs")

# Display access information
echo -e "\n${GREEN}"
cat << "EOF"
╔════════════════════════════════════════════════════════════════╗
║                   PLATFORM IS READY! 🚀                        ║
╚════════════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}📊 Web Interfaces${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

echo ""
echo -e "${GREEN}Dremio (Query Engine)${NC}"
echo "  URL:      http://localhost:9047"
echo "  Status:   Complete setup wizard on first visit"
echo ""

echo -e "${GREEN}Airflow (Orchestration)${NC}"
echo "  URL:      http://localhost:8081"
echo "  Username: admin"
echo "  Password: admin"
echo ""

echo -e "${GREEN}Spark Master (Processing)${NC}"
echo "  URL:      http://localhost:8080"
echo ""

echo -e "${GREEN}Jupyter Lab (Notebooks)${NC}"
echo "  URL:      http://localhost:8888"
if [ "$JUPYTER_TOKEN" != "Check logs" ]; then
    echo "  Token:    $JUPYTER_TOKEN"
else
    echo "  Token:    Run 'docker logs jupyter' to get token"
fi
echo ""

echo -e "${GREEN}MinIO (Object Storage)${NC}"
echo "  URL:      http://localhost:9001"
echo "  Username: minioadmin"
echo "  Password: minioadmin123"
echo ""

echo -e "${GREEN}Grafana (Monitoring)${NC}"
echo "  URL:      http://localhost:3000"
echo "  Username: admin"
echo "  Password: admin"
echo ""

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}🚀 Quick Start Guide${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

echo ""
echo -e "${GREEN}Step 1: Generate Sample Data${NC}"
echo "  docker exec airflow-webserver python /opt/airflow/dags/../../../scripts/generate_data.py"
echo ""

echo -e "${GREEN}Step 2: Run ETL Pipeline${NC}"
echo "  1. Open Airflow: http://localhost:8081"
echo "  2. Find DAG: 'ecommerce_etl_pipeline'"
echo "  3. Enable the DAG (toggle switch)"
echo "  4. Click 'Trigger DAG' (play button)"
echo "  5. Watch the pipeline execute"
echo ""

echo -e "${GREEN}Step 3: Run Iceberg Operations${NC}"
echo "  docker exec spark-master spark-submit \\"
echo "    --master spark://spark-master:7077 \\"
echo "    /opt/spark-jobs/iceberg_operations.py"
echo ""

echo -e "${GREEN}Step 4: Query in Dremio${NC}"
echo "  1. Open Dremio: http://localhost:9047"
echo "  2. Complete setup wizard"
echo "  3. Add MinIO as S3 source"
echo "  4. Query your Iceberg tables"
echo ""

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}📚 Documentation${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

echo ""
echo "  • README.md           - Project overview"
echo "  • SETUP_GUIDE.md      - Detailed setup instructions"
echo "  • INTERVIEW_PREP.md   - Interview talking points"
echo "  • docs/ARCHITECTURE.md - System design documentation"
echo ""

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${YELLOW}🛠️  Useful Commands${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

echo ""
echo "  Check status:    docker-compose ps"
echo "  View logs:       docker-compose logs -f [service-name]"
echo "  Stop platform:   docker-compose down"
echo "  Restart service: docker-compose restart [service-name]"
echo "  Clean up all:    docker-compose down -v"
echo ""

if [ "$HEALTHY" = false ]; then
    echo -e "${YELLOW}⚠️  Warning: Some services may not be fully healthy yet.${NC}"
    echo -e "${YELLOW}   Wait a few more minutes and check: docker-compose ps${NC}"
    echo ""
fi

echo -e "${GREEN}✨ Platform ready! Happy data engineering! ✨${NC}"
echo ""
