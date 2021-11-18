#!/bin/bash

DATE=$(date "+%Y-%m-%d")

airflow tasks test tutorial print_date $DATE

airflow tasks test tutorial sleep $DATE

airflow tasks test tutorial templated $DATE
