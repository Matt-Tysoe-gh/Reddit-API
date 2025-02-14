ETL Pipeline with Flask API, Azure, and Google Looker
Overview:
This project demonstrates an automated ETL (Extract, Transform, Load) pipeline built using Python and Flask. It is designed to fetch, process, and load data into an Azure SQL database, with real-time insights visualized in Google Looker. The pipeline is containerized with Docker and deployed on an Azure Web App, with automation handled by Azure Scheduler.

Features:
- Data Extraction: Automated hourly API calls to gather data.
- Data Transformation: Processing and cleaning data before loading.
- Data Loading: Transformed data is inserted into an Azure SQL database.
- Visualisation: Data is connected to Google Looker for dashboards and reporting.
- Scalable Deployment: Dockerised application hosted on an Azure Web App.
- Automation: Hourly execution using Azure Scheduler.

Technologies Used:
- Backend: Python, Flask, PRAW, pyodbc
- Containerisation: Docker
- Cloud Services: Azure Web Apps, Azure SQL Database, Azure Scheduler
- Visualisation: Google Looker

ETL Roadmap:
- Remove jsonify functionality (no longer required) – ✅ DONE
- Add logging and store log data in an Azure service for info logs, debugging, and catching warning/critical errors.
- Store connection strings and API details in a secure Azure secrets manager to allow for dynamic reusability.
