# Databricks notebook source
import json
def get_config(env, path):
    with open(f"{path}") as f:
        config = json.load(f)
    return config[env]

# COMMAND ----------


