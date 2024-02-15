# allianz_demo

It is a demo to develop an ETL pipeline such that extracting dataa from CSV and loading to SQL DB (Postgres)
  - Read the data from the CSV file developed by python script.
  - supporting incremental loading to process only new records since the last ETL run
  - Parameterize the script to allow configuration of database connection details, file paths, and other settings without modifying the code
  - Store registers with missing values in another table
  - data encryption techniques to secure sensitive information during storage

