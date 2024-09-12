# Databricks notebook source
# MAGIC %pip install pytest
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pytest
import os
import sys

def run_pytest(pytest_path):
  # Skip writing pyc files on a readonly filesystem.
  sys.dont_write_bytecode = True

  retcode = pytest.main([pytest_path, "-p", "no:cacheprovider", "-p", "no:warnings"])

  # Fail the cell execution if we have any test failures.
  assert retcode == 0, 'The pytest invocation failed. See the log above for details.'

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC

# COMMAND ----------

# MAGIC %reload_ext autoreload

# COMMAND ----------

from flights.transforms import flight_transforms
schema = flight_transforms.get_flight_schema()

# COMMAND ----------

run_pytest("transforms/test_flight_transforms.py")

# COMMAND ----------

# sys.path
