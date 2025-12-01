# PySpark AML ETL Pipeline

## 🚀 Project Overview

This project implements a complete, robust, and automated ETL (Extract, Transform, Load) pipeline designed to process financial transaction data for Anti-Money Laundering (AML) purposes. The pipeline is built using a modern data engineering stack, including Python, Apache Spark, PostgreSQL, and Docker.

The system is designed to be resilient and scalable, capable of processing batches of transaction files, identifying suspicious activities based on predefined rules, and loading both clean data and generated alerts into a structured database for further analysis.

This project demonstrates a full cycle of data engineering best practices, from data ingestion and transformation to schema management and error handling.

---

## ✨ Key Features

*   **Data Generator:** Unique transactions generator
*   **Automated Batch Processing:** The pipeline automatically finds and processes all new transaction files from a designated `raw` data directory.
*   **Data Transformation with Spark:** Leverages PySpark for scalable data processing, including data type casting, cleaning, and business logic implementation.
*   **Rule-Based Alerting:** Identifies suspicious transactions based on configurable rules (e.g., high transaction amounts, transactions involving high-risk countries).
*   **Resilient Error Handling:** Fault-tolerant design ensures that an error in one file does not stop the entire process. Failed files are automatically moved to an `error` directory for investigation, while successful ones are archived in `done`.
*   **Database Integration:** Loads processed data into a PostgreSQL database, automatically creating separate tables for all transactions (`transactions`) and identified alerts (`alerts`).
*   **Professional Logging:** Implements comprehensive logging to both console and a file (`logs/pipeline.log`) for easy monitoring and debugging.
*   **Containerized Environment:** The entire database environment is managed by Docker and Docker Compose, ensuring consistency and ease of setup.

---

## 🛠️ Tech Stack

*   **Language:** Python 3.10
*   **Core Processing Engine:** Apache Spark (PySpark) 3.5.x
*   **Database:** PostgreSQL (running in a Docker container)
*   **Containerization:** Docker & Docker Compose
*   **Key Python Libraries:**
    *   `pyspark`: For data processing.
    *   `psycopg2-binary`: For database connectivity and schema migration.
    *   `python-dotenv`: For managing environment variables.
    *   `faker`: For generating realistic synthetic data.

---

## ⚙️ Setup and Installation

### Prerequisites

*   Python 3.10+
*   Docker and Docker Compose
*   A configured Java 11/17 environment (`JAVA_HOME` should be set).
*   A configured Spark environment (`SPARK_HOME` should be set).

### Installation Steps

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/pawel-jasnowski/pyspark-etl-pipeline.git
    cd pyspark-etl-pipeline
    ```

2.  **Create a virtual environment and install dependencies:**
    ```bash
    # Create virtual environment
    python -m venv venv

    # Activate it
    # On Windows:
    venv\Scripts\activate
    # On macOS/Linux:
    source venv/bin/activate

    # Install required packages
    pip install -r requirements.txt
    ```

3.  **Configure environment variables:**
    *   Create a file named `.env` in the root directory of the project.
    *   Copy the contents of `.env.example` into `.env` and fill in your database credentials. The default values should work with the provided Docker Compose setup.
    ```env
    # .env file
    DB_HOST=localhost
    DB_PORT=5432
    DB_NAME=aml_db
    DB_USER=user
    DB_PASSWORD=password
    ```

---

## 🚀 How to Run the Pipeline

1.  **Start the PostgreSQL database:**
    *   Make sure Docker Desktop is running.
    *   In the project's root directory, run:
    ```bash
    docker compose up -d
    ```
    This will start the PostgreSQL container in the background.

2.  **Generate sample data (optional):**
    *   The pipeline processes files from the `data/raw` directory. To generate some sample data, run the data generator script:
    ```bash
    python src/data_generator.py
    ```
    You can run this multiple times to create several files for batch processing.

3.  **Run the ETL pipeline:**
    *   Execute the main pipeline script:
    ```bash
    python src/etl_pipeline.py
    ```
    The script will find all files in `data/raw`, process them, load the data into the database, and move the files to `data/done` or `data/error`.

4.  **Check the results:**
    *   You can connect to the PostgreSQL database using any SQL client (like DBeaver) with the credentials from your `.env` file.
    *   Inspect the `transactions` and `alerts` tables.
    *   Check the `logs/pipeline.log` file for detailed information about the run.

5.  **Stop the database:**
    *   When you are finished, you can stop the database container:
    ```bash
    docker compose stop
    ```

---

## 💡 Future Enhancements (Version 2.0 Roadmap)

This project provides a solid foundation that can be extended with more advanced features. The following are planned for the next version:

*   **Input Data Validation with Pydantic:** Implement a proactive data validation layer using `Pydantic` to define a "data contract". This will allow the pipeline to reject malformed or incomplete records at the very beginning of the transformation stage and move them to a quarantine table.
*   **Advanced Analytical Layer:** Create a separate `reporting.py` script that uses `SQLAlchemy` and `pandas` to connect to the database, run complex analytical SQL queries (e.g., using window functions to analyze customer behavior), and generate visualizations with `Matplotlib`.
*   **Data Enrichment:** Introduce a `customers` dimension table and enrich the transaction data by performing a `JOIN` in Spark. This will enable more complex business rules, such as "flag a transaction if it's from a newly registered customer".
*   **Unit & Integration Testing:** Add a suite of tests using `pytest` to verify the logic of the transformation functions and ensure the pipeline's reliability.
*   **Full Containerization & Orchestration:** Dockerize the Python application itself and use **Apache Airflow** to orchestrate the entire workflow (e.g., run the data generator and then the ETL pipeline on a schedule).