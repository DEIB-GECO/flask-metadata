#!/usr/bin/env bash


TABLES="donor,biosample,replicate,replicate2item,item,case2item,case_study,project,experiment_type,dataset"



flask-sqlacodegen --flask --tables $TABLES --outfile model/models.py postgresql+psycopg2://$POSTGRES_USER:$POSTGRES_PW@$POSTGRES_URL/$POSTGRES_DB
