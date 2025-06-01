# Project-Mapreduce
# Climate Data Analysis with Hadoop MapReduce
---

### Objectives

- Implement batch processing architecture using Hadoop MapReduce to analyze large climate datasets.  
- Process temperature and precipitation data by city, month, and season.  
- Store raw and processed data in HDFS.  
- Develop a REST API with Flask to serve analysis results.  
- Gain practical understanding of distributed data processing fundamentals.

---

### Scope

- Raw climate data stored and processed using Hadoop ecosystem.  
- MapReduce jobs written in Python with MRJob library.  
- Results output as CSV files in HDFS and accessed via API.  
- API provides endpoints for querying, filtering, and downloading processed data.

---

### 1. Requirements

#### Functional Requirements

- Process raw climate CSV data to extract meaningful statistics.  
- Calculate monthly totals and seasonal patterns for precipitation.  
- Compute average, max, and min temperatures monthly by city.  
- Provide a REST API for accessing analysis results.  
- Enable filtering of results by city, year, and season.

#### Non-Functional Requirements

- Scalability to handle large datasets in a distributed manner.  
- Reliability in data processing and API availability.  
- Ease of use through API documentation and filtering options.

#### Technical Requirements

- Hadoop and HDFS for storage and processing.  
- Python with MRJob for MapReduce jobs.  
- Flask and Pandas for API and data handling.

---

### 2. Architecture & Analysis

#### High-Level Architecture

- **HDFS:** Distributed storage for raw and processed data.  
- **MapReduce Jobs:** Python scripts for temperature and precipitation analysis.  
- **Flask API:** REST endpoints for accessing processed data.  
- **Data Flow:** Raw CSV → HDFS → MapReduce → Processed CSV → API → User.

---

### 3. Design

#### MapReduce Jobs

| Job                   | Description                                    |
|-----------------------|------------------------------------------------|
| PrecipitationAnalysis  | Monthly and seasonal precipitation aggregation |
| TemperatureAnalysis    | Monthly average, max, min temperature stats    |

#### API Endpoints

| Endpoint               | Purpose                                        |
|------------------------|------------------------------------------------|
| `/`                    | API overview                                   |
| `/health`              | Health check of API and data files             |
| `/data/temperature`    | Temperature data with optional filters         |
| `/data/precipitation`  | Precipitation data with optional filters       |
| `/data/summary`        | Summary of all analyses                         |
| `/data/cities`         | List of cities and related stats                |
| `/download/temperature`| Download temperature CSV                        |
| `/download/precipitation`| Download precipitation CSV                     |

---

### 4. Implementation Details

#### Languages & Frameworks

- Python 3 (MRJob, Pandas, Flask)  
- Hadoop MapReduce for batch processing  
- HDFS for distributed storage  

#### Running MapReduce Jobs

- Run with MRJob locally or in Hadoop mode.  
- Input: Raw CSV files in HDFS.  
- Output: Processed CSV files saved in `data/processed/`.

#### Running the API

- Flask app runs on port 5000 by default.  
- Requires `requirements.txt` dependencies installed.  
- Serves JSON responses and CSV downloads.

---

### 5. Running the System

#### Prerequisites

- Hadoop cluster or local Hadoop setup  
- Python 3.8+ and required packages (`mrjob`, `flask`, `pandas`, `flask-cors`)  
- Data files in HDFS or local paths  

#### Steps

1. Upload raw CSV files to HDFS (`scripts/hdfs_upload.sh` or manually).  
2. Run MapReduce jobs:  
   ```bash
   python mapreduce/precipitation_analysis.py -r hadoop hdfs:///path/to/input.csv > data/processed/precipitation_results.csv
   python mapreduce/temperature_analysis.py -r hadoop hdfs:///path/to/input.csv > data/processed/temperature_results.csv
3. Start Flask API:
    ```bash
        Copy
        Edit
        cd api
        pip install -r requirements.txt
        python app.py
4. Access API at http://localhost:5000.

### 6. Usage Example

- Query temperature data filtered by city and year.
- Retrieve precipitation patterns by season.
- Download CSV reports for external analysis.

### 7. Architecture Model

#### C4 Model
- Distributed batch processing architecture with HDFS and MapReduce.
-API layer provides data access and filtering.

Authors
Sofia Zapata Zuluaga
Universidad EAFIT
2025-1
