# Airflow ETL: MongoDB to Postgres

## Overview
This repository contains an ETL pipeline orchestrated by Apache Airflow inside Docker.  
The pipeline extracts data from MongoDB on a schedule and, upon manual trigger, transforms and loads the data into Postgres.

## Architecture
- **Airflow**: Orchestration and scheduling  
- **MongoDB**: Data source  
- **Postgres**: Target data warehouse  
- **Docker Compose**: Container orchestration  

## Project Structure

```
├── dags/
│ ├── mongo_db_extraction.py
│ ├── transformation_export.py
│ └── utils/
├── docker-compose.yml
├── .env
├── requirements.txt
├── logs/
├── plugins/
└── README.md
└──requirements.txt
```



## Prerequisites
- Docker  
- Docker Compose  

## Setup

### Clone the repository
```bash
git clone https://github.com/SamarAbbas-DATA/ETL_mongo_project.git
cd airflow-mongo-postgres
```

### services

```docker-compose up -d```

### Initialize Airflow
```docker-compose run airflow-init```

### Access Airflow

```Go to http://localhost:8080```