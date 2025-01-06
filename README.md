ETL Pipeline with Flask API, Azure, and Google Looker

Overview:
This project demonstrates an automated ETL (Extract, Transform, Load) pipeline built using Python and Flask, designed to fetch, process, and load data into an Azure SQL database. The data is visualised in Google Looker for real-time insights. The pipeline is containerised with Docker and deployed on an Azure Web App, with automation handled by Azure Scheduler.

Features:

Data Extraction: Automated hourly API calls to gather data.
Data Transformation: Processing and cleaning data before loading.
Data Loading: Transformed data is inserted into an Azure SQL database.
Visualisation: Data is connected to Google Looker for dashboards and reporting.
Scalable Deployment: Dockerised application hosted on an Azure Web App.
Automation: Hourly execution using Azure Scheduler.
Technologies Used:

Backend: Python, Flask, PRAW, pyodbc
Containerisation: Docker
Cloud Services: Azure Web Apps, Azure SQL Database, Azure Scheduler
Visualisation: Google Looker
Looker Dashboard

Challenges and Solutions:

Challenge: Persistent 404 error when launching localhost via Docker.
Solution: Too many API calls were being generated to too many subreddits. I reduced the number of subreddits, which resolved the issue. Alternatively, this could have been solved using asynchronous API calls, threaded requests, or API caching, which I plan to implement in the future.

Challenge: Connecting Azure SQL database to Looker.
Solution: I had to add some IP ranges to allow Azure and Looker to communicate with each other.

Future Challenge: Secure storage of API secrets and connection strings (currently stored in plain text in the Flask app).
Future Solution: Use Azure Key Vault to store this information and retrieve it securely during the automated process.

Looker Dashboard: https://lookerstudio.google.com/s/tCrhR_b1lMY
