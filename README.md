<h1 align="center">Azure Meal Project | ETL Pipeline</h1>

**Overview**
The Azure Meal Project is a cloud-based ETL pipeline designed for efficient data management and analysis. It processes raw data stored in Azure Blob Storage using Azure Function Apps, ingests it into an Azure SQL Server database, and visualizes insights via Power BI reports. The architecture diagram (Figure 1) outlines this flow.


<p align="center">
 <b>Figure 1: Architecture Design of Meal Project ETL</b>
    <img src="https://github.com/user-attachments/assets/7269920c-fb1a-497d-8e8a-79e1d41d270d" alt="Architecture Diagram" width="600">
</p>



</div>

**Pipeline Components**
**Blob Storage**
Stores raw input files (primarily .csv) in a structured format.
Organized into three directories:
InputFiles: For files awaiting processing.
Archived: For successfully processed files.
FaultyFiles: For files with data ambiguities.

**Function App**
A serverless solution triggered at midnight to process data automatically.
Key steps:
* Retrieve input files from InputFiles directory.
* Process data based on predefined calculations (WLD).
* Store clean data in Azure SQL Server Database.
* Move ambiguous files to FaultyFiles and processed files to Archived.
* Developed in Visual Studio Code and linked to repository.

**Azure SQL Server Database**
File_Metadata: Stores file-level information, including group IDs and file codes.
WeightLoss: Tracks daily weight data and unique weight IDs.

**Power BI Reporting**
Provides a comprehensive dashboard with insights such as:
Maximum and minimum weight loss.
Variations and totals of formulations.
Daily weight trends.
This pipeline ensures efficient data processing, storage, and reporting for meaningful insights into weight loss data.
