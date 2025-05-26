# E-Commerce Analytics Pipeline

This project builds a hybrid real-time and batch analytics pipeline for an e-commerce platform using:

- Kafka for event ingestion
- PySpark for processing
- Airflow for orchestration
- AWS S3 + RDS for storage
- Metabase/Streamlit for visualization

## Folder Structure
(Describe briefly or link to docs)

## Setup
- `python -m venv venv`
- `pip install -r requirements.txt`
- `remove second editor from project`
- `Kafka setup is working as expected`



# Ecommerce Analytics Pipeline – Project Goal and Summary

## Project Objective

The objective of this project is to design and implement an end-to-end data pipeline for a simulated ecommerce platform. This pipeline will ingest user activity events (such as product views, cart additions, and purchases) in real-time, process the data through batch and streaming frameworks, orchestrate tasks for automation, store the output in cloud and relational storage, and finally generate business insights through an analytics dashboard.

This project demonstrates the complete data engineering lifecycle by integrating tools commonly used in production systems, following modular and scalable design principles.

---

## Scope and Components

This project covers the following components:

1. **Data Simulation**

   * Generate synthetic user behavior events (e.g., pageviews, orders).
   * Output events in JSON format to mimic production telemetry data.

2. **Data Ingestion (Kafka)**

   * Use Apache Kafka to stream events to various topics (e.g., `pageviews`, `orders`).
   * Implement a Python-based Kafka producer.

3. **Raw Data Storage**

   * Capture raw streamed data and store it in AWS S3 (or local file system as a fallback).
   * Enable archival and further processing.

4. **Data Processing (PySpark)**

   * Batch Job: Transform and clean raw data from S3, calculate aggregates (e.g., daily revenue, user activity).
   * Streaming Job (optional): Perform near real-time processing from Kafka streams.

5. **Orchestration (Airflow)**

   * Define DAGs (Directed Acyclic Graphs) to schedule batch processing jobs.
   * Automate daily ingestion, transformation, and loading tasks.

6. **Data Storage (S3 and RDS)**

   * Store cleaned and aggregated data in S3 and PostgreSQL (RDS).
   * Enable structured querying and dashboard integration.

7. **Visualization (Streamlit or Metabase)**

   * Build an analytics dashboard to display key performance indicators (KPIs) such as:

     * Daily Active Users (DAU)
     * Revenue by Product Category
     * Top Selling Products
     * Orders per Hour

8. **Testing and Logging**

   * Implement basic logging, error handling, and unit tests for data integrity.

9. **Dockerized Setup**

   * Use Docker and Docker Compose to run services like Kafka, Zookeeper, and Airflow locally.
   * Ensure reproducibility and consistent local development environment.

---

## Learning Outcomes

This project offers hands-on experience in:

* Designing scalable data pipelines.
* Working with real-time and batch data processing frameworks.
* Automating workflows using orchestration tools.
* Integrating storage and visualization layers.
* Applying version control, modular design, and containerization in a data engineering context.

---

