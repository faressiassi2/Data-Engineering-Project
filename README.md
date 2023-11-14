# Data-Engineering-Project

# Overview
The aims of this project is to use a very essentiel tool for Data Engineering Spark to analyze,preprocess and create a classification Machine Learning model and then evaluate this model using Spark. For that I use the datasets show here: 
https://www.kaggle.com/datasets/tylerx/flights-and-airportsdata?select=flights.csv

# Project Workflow
1- I'll start by extracting the data into a pyspark dataframe.
2- Then performing data analysis and preprocessing.
3- After creating a Machine Learning model using the data preprocessed.
4- Evaluate this model using some metrics for classification.
5- Finally I'll use Apache Airflow to orchestrate all of this data pipeline.

# Environment Setup
Ensure that you have the following tools on your local system:
   1- Apache Spark
   2- Apache Airflow

# Environment Variables
For the part of extarcting the data I use the code in extract.py
For the analyze and preprocess I use analyse_traitement.py
Then modelisation.py for the part of modelization and the choice of the model
Finally the part of evaluation in evaluation.py


