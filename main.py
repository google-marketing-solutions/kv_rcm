# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Functions for aggregating Key-Valu Recommendation for UPR in Data Transfer.

Run from project's root directory.
"""
import itertools
import logging
from typing import Collection

from google.cloud.bigquery import Client
import helpers
import pandas as pd


_DEFAULT_CONFIGS = 'configs.yaml'

config_data = helpers.get_configs(_DEFAULT_CONFIGS)
_PROJECT_ID = config_data.project_id
_DATASET_NAME = config_data.dataset_name
_PARSED_KV_TABLE = config_data.parsed_kv_table
_AGGREGATED_DATA_WITH_KV = config_data.aggregated_data_with_kv
_DISTINCT_TABLE = config_data.distinct_table
_KV = config_data.key_patterns
_INPUT_TABLE = _DATASET_NAME + '.' + _PARSED_KV_TABLE
_OUTPUT_TABLE = _DATASET_NAME + '.' + _AGGREGATED_DATA_WITH_KV
_DISTINCT_OUTPUT_TABLE = _DATASET_NAME + '.' + _DISTINCT_TABLE

_TEMPLATE_COLUMNS = ['AdUnitId', 'estimated_revenue', 'eCPM', 'imp']

logging.basicConfig(
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.INFO,
)

_DISTINCT_OUTPUT_QUERY = f"""
          CREATE OR REPLACE TABLE {_DISTINCT_OUTPUT_TABLE} AS (
            SELECT DISTINCT
              *
            FROM
              `{_OUTPUT_TABLE}`
          )
          """


def create_query(key_pattern: Collection[str]) -> str:
  """Creates a query to run in BigQuery.

  Args:
    key_pattern: A key_pattern to run in SQL for BigQuery.

  Returns:
    A query embedding key_pattern and _INPUT_FILE_TABLE1.
  """

  comma_separated_keys = ', '.join(key_pattern)
  query = f"""
          SELECT
            {comma_separated_keys}
            , AdUnitId
            , SUM(EstimatedBAckfillRevenue) as sum_revenue
            , SUM(EstimatedBAckfillRevenue) / COUNT(1) * 1000 as eCPM
            , COUNT(1) as impressions
          FROM
            `{_INPUT_TABLE}`
          GROUP BY
            AdUnitId, {comma_separated_keys}
          ;
          """

  return query


def run_query(key_patterns: Collection[str]) -> None:
  """Calculate combinations of Key-Value pattern.

  Args:
    key_patterns: A list of key_patterns that you want to calculate impressions,
      eCPM and revenue each key_pattern.
  """

  clmns = _KV + _TEMPLATE_COLUMNS
  df_ecpm = pd.DataFrame(columns=clmns)

  for key_pattern in key_patterns:
    query = create_query(key_pattern)
    df_tmp = Client().query(query).to_dataframe()
    df_ecpm = pd.concat([df_ecpm, df_tmp], ignore_index=True, sort=False)

  df_ecpm.to_gbq(_OUTPUT_TABLE, _PROJECT_ID, if_exists='append')


def execute_combinations_of_kv(keys: Collection[str],
                               ) -> Collection[Collection[str]]:
  """Calculate combinations of Key-Value pattern.

  Args:
    keys: A list of Keys of Key-Value. Example data is ['kv_age_group',
      'kv_genre', 'kv_keywords'].

  Returns:
    A 2 dimension list of combination of Key-Value. Example data is [[
    'kv_age_group', 'kv_genre'], ['kv_genre'], ['kv_age_group', 'kv_genre',
    'kv_keywords'], ... etc.].
  """
  key_patterns = []
  for i in range(1, len(keys) + 1):
    for combination in itertools.combinations(keys, i):
      key_patterns.append(list(combination))

  return key_patterns


def execute_run_query_with_all_key_value_patterns() -> None:
  """Executes run_query with all combinations of Key Value pattern."""
  logging.info('Start process of execute_run_query_with_all_key_value'
               '_patterns...')

  key_patterns = execute_combinations_of_kv(_KV)

  run_query(key_patterns)
  logging.info('Completed process of execute_run_query_with_all_key_value'
               '_patterns.')


def run_query_for_distinguishes_outputs() -> None:
  """Distinguishes rows in output table."""

  Client().query(_DISTINCT_OUTPUT_QUERY)
