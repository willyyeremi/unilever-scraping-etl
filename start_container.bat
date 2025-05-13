@echo off

REM Creating directories that needed by container
mkdir ".\data\apache_airflow\logs"
mkdir ".\data\apache_airflow\plugins"

REM Start Docker Compose
docker-compose -p unilever-scrapper -f docker-compose.yaml up -d --build