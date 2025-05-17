# Unilever Scraping: ETL
This is the repository for the data pipeline.

## Precaution
- Set [database](https://github.com/willyyeremi/unilever-scrapper-database) first, so the component can run.
- don't forget to set the .env file for this variable
  - FERNET_KEY
  - SECRET_KEY
  - API_JWT_SECRET
- Run start-container.bat first to set up needed components.

## ETL Tools
1. Apache Airflow

## Notes
- redis is used as a rate limiter to api webserver, with database 0 to be specified
- Sometimes the pipeline is failing because html element changed
