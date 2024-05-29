#!/bin/bash

create_environment() {
	echo Creating evironment
	
	if [ -f ./.env ]; then
		echo "The .env file alredy created"
	else
		echo -e "AIRFLOW_UID=$(id -u)\nDB_CONNECTION=airflow-db\nDB_DATABASE=postgres" > .env
	fi

	if [ -d ./dags ] && [ -d ./logs ] && [ -d ./plugins ] && [ -d ./config ]; then
		echo "The directories alredy created"
	else
		mkdir -p ./dags ./logs ./plugins ./config	./plugins/hooks
	fi
	
}

airflow_init() {
	echo Starting airflow
	sudo docker compose up airflow-init && sudo docker compose up -d
	sudo docker compose logs -f

}

create_environment
airflow_init
