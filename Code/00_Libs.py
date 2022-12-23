# Databricks notebook source
from kaggle.api.kaggle_api_extended import KaggleApi
import pyspark.sql.functions as sql_func
from datetime import date, datetime, timedelta
import pyspark.sql.types as sql_types
from pyspark.sql import DataFrame
from dateutil.parser import parse
import dateutil.relativedelta
from functools import reduce
import pandas as pd
import numpy as np
import math
import json
import os

spark.conf.set("spark.sql.shuffle.partitions",4)
