Hourly Supermarket Sales Data Pipeline

Description:

This project is a data ingestion pipeline that fetches hourly supermarket sales data from a web server running on an NGINX container. The pipeline is built using Python, Airflow, and MinIO. It extracts the data using a custom API endpoint, handles missing data gracefully, and stores the data in a structured format in MinIO for further analysis.

Key Features:

* **Data Extraction:** Fetches hourly sales data from a custom API endpoint (`http://localhost:5000/data`) running on an NGINX container.
* **Fault Tolerance:** Handles scenarios where data is not available for certain hours without failing the pipeline.
* **Structured Storage:** Stores data in MinIO using a well-organized folder structure (`supermarket_sales/{today}/hour_{this_hour}_supermarket_sales.csv`) for easy archiving and retrieval.
* **Scheduling:** Uses Airflow to schedule and automate the data ingestion process on an hourly basis.

Technologies Used:

* **Python:** Core programming language for data extraction, transformation, and loading.
* **Airflow:** Workflow management platform for scheduling and orchestrating the pipeline.
* **MinIO:** Object storage system for storing the sales data.
* **NGINX:** Web server used to host the custom API endpoint for data access.
* **Docker:** Containerization platform for creating and managing the NGINX container.

Setup and Usage:

1. **Prerequisites:**
   - Docker and Docker Compose installed
   - MinIO server running and accessible
   - Airflow environment set up

2. **Run NGINX Container:**
   - Use the provided `docker-compose.yaml` file to start the NGINX container.
   - Download the required sales data files to the container using the provided commands.

3. **Configure Airflow DAG:**
   - Set up the Airflow DAG (`minio_s3_dag`) with the correct MinIO credentials and configuration.
   - Schedule the DAG to run hourly.

4. **Run the Pipeline:**
   - Trigger the Airflow DAG to start the data ingestion process.

Additional Notes:

- The pipeline is designed to handle missing data gracefully. If data is not available for a particular hour, it will not create a file for that hour in MinIO.
- The `README.txt` file provides instructions for setting up and running the pipeline, as well as a brief overview of its functionality and the technologies used.