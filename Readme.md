# 🏦 Banking ETL Pipeline

> Automated end-to-end data pipeline orchestrating banking data from AWS S3 to Snowflake using Apache Airflow

[![Airflow](https://img.shields.io/badge/Airflow-2.10.2-017CEE?style=flat&logo=apache-airflow)](https://airflow.apache.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Latest-29B5E8?style=flat&logo=snowflake)](https://www.snowflake.com/)
[![AWS S3](https://img.shields.io/badge/AWS_S3-Storage-FF9900?style=flat&logo=amazon-s3)](https://aws.amazon.com/s3/)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?style=flat&logo=python)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Features](#-features)
- [Tech Stack](#-tech-stack)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [Data Models](#-data-models)
- [Screenshots](#-screenshots)
- [Performance](#-performance)
- [Future Enhancements](#-future-enhancements)

---

## 🎯 Overview

Production-ready ETL pipeline that processes banking data with intelligent orchestration, automated quality checks, and generates actionable business insights.

### Key Highlights

```
📊 Data Tables       : 4 source + 11 analytical models
⚡ Execution Time    : < 1 minute end-to-end
🔄 Tasks             : 26 orchestrated operations
✅ Success Rate      : 100% with automated retry
📈 Parallel Tasks    : Up to 10 concurrent operations
```

---

## 🏗️ Architecture

```
┌─────────────┐         ┌──────────────┐         ┌─────────────┐
│   AWS S3    │────────▶│   Airflow    │────────▶│  Snowflake  │
│             │         │              │         │             │
│ ┌─────────┐ │         │ ┌──────────┐ │         │ ┌─────────┐ │
│ │customers│ │         │ │26 Tasks  │ │         │ │ Source  │ │
│ │accounts │ │────────▶│ │Pipeline  │ │────────▶│ │ Tables  │ │
│ │txns     │ │         │ │          │ │         │ │         │ │
│ │loans    │ │         │ └──────────┘ │         │ └─────────┘ │
│ └─────────┘ │         │              │         │             │
│             │         │ • Validation │         │ ┌─────────┐ │
│             │         │ • Loading    │         │ │Analytics│ │
│             │         │ • Transform  │         │ │ Models  │ │
└─────────────┘         └──────────────┘         └─────────────┘
```

### Pipeline Flow

```
1. S3 Validation → 2. Staging Setup → 3. Data Loading → 4. Quality Checks → 5. Transformations → 6. Summary
```

---

## ✨ Features

### Core Capabilities

- ✅ **Automated S3 to Snowflake** data ingestion
- ✅ **Parallel processing** with intelligent task dependencies
- ✅ **Data quality validation** with automated reporting
- ✅ **11 analytical models** for business intelligence
- ✅ **Error handling** with retry mechanisms
- ✅ **Real-time monitoring** and execution tracking

### Business Analytics

| Model | Purpose |
|-------|---------|
| Customer Segmentation | Classify customers as Premium/Moderate/Normal |
| Risk Profiling | Identify overleveraged customers |
| Loan Aging | Track active loan maturity |
| Transaction Analysis | Credit/Debit summaries per customer |
| High-Value Customers | Flag customers with >$100K balance or >$30K loans |
| City Analytics | Performance metrics by geography |

---

## 🛠️ Tech Stack

```yaml
Orchestration:
  - Apache Airflow: 2.10.2
  
Cloud Services:
  - Snowflake: Cloud Data Warehouse
  - AWS S3: Data Lake Storage
  
Languages:
  - Python: 3.8+
  - SQL: Data Transformation
  
Key Libraries:
  - snowflake-connector-python: 3.12.2
  - boto3: 1.35.36
  - apache-airflow-providers-snowflake: 5.7.0
  - apache-airflow-providers-amazon: 8.29.0
```

---

## 🚀 Installation

### Prerequisites

```bash
✓ Python 3.8-3.11
✓ AWS Account with S3 access
✓ Snowflake Account
✓ 4GB+ RAM
```

### Quick Start

```bash
# 1. Clone repository
git clone https://github.com/PrathameshUpreti/-Banking-ETL-Pipeline.git
cd banking-etl-pipeline

# 2. Create virtual environment
python3 -m venv airflow_venv
source airflow_venv/bin/activate  # Windows: airflow_venv\Scripts\activate

# 3. Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

# 4. Initialize Airflow
export AIRFLOW_HOME=~/airflow
airflow db init

# 5. Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin123

# 6. Copy DAG file
mkdir -p ~/airflow/dags
cp dags/banking_etl_pipeline.py ~/airflow/dags/
```

---

## ⚙️ Configuration

### 1. AWS S3 Setup

```bash
# Create bucket
aws s3 mb s3://banking-bucket-etl --region us-east-1

# Upload data files
aws s3 cp data/customers.csv s3://banking-bucket-etl/
aws s3 cp data/accounts.csv s3://banking-bucket-etl/
aws s3 cp data/transactions.csv s3://banking-bucket-etl/
aws s3 cp data/loans.csv s3://banking-bucket-etl/
```

### 2. Snowflake Setup

```sql
-- Create Storage Integration
CREATE STORAGE INTEGRATION S3_IntegrationS
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/ROLE_NAME'
  STORAGE_ALLOWED_LOCATIONS = ('s3://banking-bucket-etl/');

-- Grant permissions
GRANT USAGE ON INTEGRATION S3_IntegrationS TO ROLE ACCOUNTADMIN;
```

### 3. Airflow Connections

**Navigate to Admin → Connections in Airflow UI**

#### Snowflake Connection
```
Connection ID  : Snowflake_connection
Connection Type: Snowflake
Account        : your_account.region
Warehouse      : COMPUTE_WH
Database       : Bank
Schema         : bank
Login          : your_username
Password       : your_password
```

#### AWS Connection
```
Connection ID       : aws_conn
Connection Type     : Amazon Web Services
AWS Access Key ID   : your_access_key
AWS Secret Access Key: your_secret_key
Region             : us-east-1
```

---

## 📊 Usage

### Access Dashboard

```
URL     : http://localhost:8080
Username: admin
Password: admin123
```

### Trigger Pipeline

1. Navigate to DAGs page
2. Find `BAanking_ETL_Pipeline`
3. Toggle DAG to **ON**
4. Click **Play** button to trigger manually
5. Monitor execution in Graph/Grid view

---

## 📈 Data Models

### Source Tables (4)

```
CUSTOMERS      → Customer master data (~1,000 records)
ACCOUNTS       → Account information (~1,500 records)
TRANSACTIONS   → Transaction history (~5,000 records)
LOAN           → Loan details (~800 records)
```

### Analytical Models (11)

```
01. customer_account              → Denormalized customer-account view
02. customer_transaction_summary  → Credit/debit totals per customer
03. loan_to_balance_ratio         → Loan vs balance analysis
04. customer_risk_profile         → Overleveraged customer detection
05. active_loan_aging             → Loan age in months
06. customer_segmentation         → Premium/Moderate/Normal classification
07. risky_transactions            → Transactions flagged >$30K
08. customer_loan_summary         → Active loan counts per customer
09. city_account_stats            → Geography-based metrics
10. customer_account_summary      → Active accounts per customer
11. high_value_customers          → High-balance/high-loan customers
```

### Quality & Monitoring

```
etl_null_report        → Data quality validation results
etl_execution_summary  → Pipeline execution statistics
```

---

## 📸 Screenshots

### Airflow DAG Visualization
![Airflow DAG](docs\images\airflow\graph.png)
*26 orchestrated tasks with parallel execution and intelligent dependencies*

### Execution Timeline
![Airflow Calender](docs\images\airflow\calender.png)
*Historical run status showing 100% success rate*

### Snowflake Data Warehouse
![Snowflake Tables](docs\images\snowflake\snow.png)
*All source and analytical tables automatically created*

### AWS S3 Data Lake
![S3 Bucket](docs\images\aws\bucket.png)
*Source CSV files stored in S3*

### Customer Segmentation Results
![Segmentation](docs\images\snowflake\image.png)
*Customers classified into Premium, Moderate, and Normal tiers*

---

## ⚡ Performance

| Metric | Value |
|--------|-------|
| **Average Runtime** | 45 seconds |
| **Total Tasks** | 26 |
| **Parallel Execution** | Up to 10 tasks |
| **Success Rate** | 100% |
| **Records Processed** | ~8,300 |
| **Analytical Models** | 11 tables |
| **Schedule** | Daily @ 00:00 UTC |

---

## 🎯 Future Enhancements

### Phase 1: Core Improvements
- [ ] Incremental loading with CDC
- [ ] SCD Type 2 for historical tracking
- [ ] dbt integration for transformations
- [ ] Great Expectations for data quality

### Phase 2: Monitoring
- [ ] Slack/Teams notifications
- [ ] Custom metrics dashboard
- [ ] Anomaly detection
- [ ] Performance alerts

### Phase 3: Scalability
- [ ] Table partitioning
- [ ] Clustering keys
- [ ] Materialized views
- [ ] Query optimization

### Phase 4: Advanced Analytics
- [ ] Predictive loan default modeling
- [ ] Customer churn analysis
- [ ] Fraud detection algorithms
- [ ] Real-time streaming integration

---

---

## 🐛 Troubleshooting

### Common Issues

**DAG not appearing**
```bash
airflow dags list-import-errors
python dags/banking_etl_pipeline.py
```

**S3 Connection Failed**
- Verify AWS credentials in Airflow connections
- Check IAM role permissions
- Confirm bucket exists and is accessible

**Snowflake Connection Failed**
- Verify account identifier format: `account.region`
- Check warehouse is running
- Confirm user has necessary privileges

**Task Failed**
1. Click failed task in Airflow UI
2. View detailed logs
3. Check connection configurations
4. Verify SQL syntax

---

## 📁 Project Structure

```
banking-etl-pipeline/
│
├── dags/
│   └── banking_etl_pipeline.py      # Main DAG file (26 tasks)
│
├── data/
│   ├── customers.csv                # Customer master data
│   ├── accounts.csv                 # Account information
│   ├── transactions.csv             # Transaction records
│   └── loans.csv                    # Loan details
│
├── docs/
│   ├── images/                      # Screenshots
│   ├── architecture.md              # Architecture docs
│   └── data_dictionary.md           # Data model docs
│
├── DockerFile           
├── README.md                        # This file
└── .gitignore                       # Git ignore rules
```

---

## 🤝 Contributing

Contributions welcome! Please follow these steps:

1. Fork the repository
2. Create feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit changes (`git commit -m 'Add AmazingFeature'`)
4. Push to branch (`git push origin feature/AmazingFeature`)
5. Open Pull Request

---

## 👤 Author

**Prathamesh Upreti**

[![GitHub](https://img.shields.io/badge/GitHub-Profile-181717?style=flat&logo=github)](https://github.com/PrathameshUpreti)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?style=flat&logo=linkedin)](https://www.linkedin.com/in/prathamesh-upreti-601b81296)
[![Email](https://img.shields.io/badge/Email-Contact-D14836?style=flat&logo=gmail)](mailto:prathameshupreti408@gmail.com)

---


## 🙏 Acknowledgments

- Apache Airflow Community
- Snowflake Documentation
- AWS Documentation
- Data Engineering Community

---

## 📞 Support

**Need Help?**
1. Check [Troubleshooting](#-troubleshooting) section
2. Review [Airflow Docs](https://airflow.apache.org/docs/)
3. Open GitHub issue
4. Contact author

---

<div align="center">

**⭐ Star this repo if you find it helpful!**

Built with ❤️ by [Prathamesh Upreti](https://github.com/PrathameshUpreti)

</div>