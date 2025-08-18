#!/bin/bash

echo "Iniciando a migração do banco de dados..."
airflow db migrate

echo "Iniciando o Webserver..."

exec airflow webserver