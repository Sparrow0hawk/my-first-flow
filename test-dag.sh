#!/bin/bash

# runs test dag
airflow dags test tutorial $(date "+%Y-%m-%d")
