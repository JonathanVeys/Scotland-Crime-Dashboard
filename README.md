
## 📊 Overview

The **Scottish Crime Prediction Dashboard** combines open government data, machine-learning models, and interactive visualisation to explore how crime rates vary across Scotland’s council areas and wards over time.  

The system automates data ingestion, harmonisation, model training, and dashboard deployment using modern data-science and MLOps practices.

---

## 🚀 Current Features

### 🔹 Data Engineering
- **Automated ETL pipeline** for downloading, cleaning, and transforming data from:
  - [Scottish Government Open Data](https://statistics.gov.scot/)
  - [UK Data Service](https://ukdataservice.ac.uk/)
  - [ONS](https://www.ons.gov.uk/)
  - [data.gov.uk](https://data.gov.uk/)
- **Geospatial harmonisation** between 2017 and 2022 ward boundaries using GeoPandas and shapefile/GeoJSON overlays.
- **Standardised schema** stored in a **Supabase-hosted PostgreSQL** database with versioned data layers (raw → processed → analytical).
- Robust **error-handling, validation, and unit testing** via `pandera`, `pytest`, and CI checks.

### 🔹 Data Analysis & Modelling
- Exploratory analysis of long-term crime trends by region and type.  
- Time-series and regression-based predictive modelling (e.g. linear regression, random forest, and early neural network prototypes).  
- Evaluation metrics and model comparison tracked via **MLflow**.  
- Configurable experiment scripts for reproducibility.

### 🔹 Application Layer
- **FastAPI backend** serving pre-processed data and model predictions.  
- **Plotly Dash** frontend providing:
  - Interactive maps (choropleths and point visualisations)
  - Trend plots and statistical summaries
  - Model prediction overlays with confidence intervals
- Deployment planned via **Heroku/Docker** with environment management through `poetry` and `.env` secrets.

---

## 🧱 Project Structure

```bash
scotland-crime-dashboard/
├── src/
│   ├── etl/                  # Data extraction, cleaning, transformation
│   ├── database/             # Supabase schema & SQL scripts
│   ├── modelling/            # ML models and evaluation pipelines
│   ├── api/                  # FastAPI backend routes
│   ├── dashboard/            # Dash frontend components
│   ├── tests/                # Unit and integration tests
│   └── utils/                # Shared helpers (logging, config, constants)
├── data/                     # Local data cache (not tracked in Git)
├── notebooks/                # Exploratory analysis and visualisation
├── requirements.txt or pyproject.toml
├── Dockerfile
├── .github/workflows/ci.yml  # Continuous integration pipeline
└── README.md
