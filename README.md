# Unilever Scraping: ETL
This is the repository for the data pipeline.

## Precaution
- Set [database](https://github.com/willyyeremi/unilever-scrapper-database) first, so the component can run.
- dont forget to set .env file
- Run start-container.bat first to set up needed components.

## ETL Tools
1. Apache Airflow

## Notes
- redis is used for rate limiter to api webserver, with database 0 to be specified
