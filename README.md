# ETL and Analytics Project with Flask and Airflow

This is an end-to-end project demonstrating an ETL pipeline using Apache Airflow and MySQL, with a Flask API to provide data analytics.

---

## How to Run

1. **Clone this repository**

```bash
git clone <your-repo-url>
cd <your-repo-folder>
```

2. **Start the project using Docker Compose**

Make sure you have Docker and Docker Compose installed.

```bash
docker-compose up --build
```

3. **Access the services**

Airflow UI: http://localhost:8081

Flask API: http://localhost:5000

That's it! The ETL will run in Airflow, and you can access analytics through the Flask API.
