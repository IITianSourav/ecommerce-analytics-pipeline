# test_env.py
import pandas as pd
import pyspark
import kafka
import boto3
import streamlit as st

print("✅ All libraries imported successfully!")
print("Pandas version:", pd.__version__)
print("PySpark version:", pyspark.__version__)
print("Kafka version:", kafka.__version__)
print("Boto3 version:", boto3.__version__)
print("Streamlit version:", st.__version__)
