# Copyright 2023-2024 Google LLC
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

import concurrent.futures
import itertools
import logging
import threading
from typing import Collection
from google.cloud.bigquery import Client
from google.cloud.exceptions import NotFound
import helpers
import pandas as pd


_DEFAULT_CONFIGS = 'configs.yaml'

config_data = helpers.get_configs(_DEFAULT_CONFIGS)
_IMPRESSIONS_SOURCE = config_data.network_backfill_impressions
_PROJECT_ID = config_data.project_id
_DATASET_NAME = config_data.dataset_name
_PARSED_KV_TABLE = config_data.parsed_kv_table
_AGGREGATED_DATA_WITH_KV = config_data.aggregated_data_with_kv
_DISTINCT_TABLE = config_data.distinct_table
_KV = config_data.key_patterns
_PARSED_KV_SOURCE = _PROJECT_ID + '.' + _DATASET_NAME + '.' + _PARSED_KV_TABLE
_OUTPUT_TABLE = (
    _PROJECT_ID + '.' + _DATASET_NAME + '.' + _AGGREGATED_DATA_WITH_KV
)
_DISTINCT_OUTPUT_TABLE = (
    _PROJECT_ID + '.' + _DATASET_NAME + '.' + _DISTINCT_TABLE
)

_TEMPLATE_COLUMNS = ('AdUnitId', 'estimated_revenue', 'eCPM', 'imp')

_THREAD_NO = 4
_KEY_SPLIT_NUMBER = 100

logging.basicConfig(
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    level=logging.INFO,
)

_DISTINCT_OUTPUT_QUERY = f"""
          CREATE OR REPLACE TABLE `{_DISTINCT_OUTPUT_TABLE}` AS (
            SELECT DISTINCT
              *
            FROM
              `{_OUTPUT_TABLE}`
          )
          ;
          """


def run_query_for_parsing_impression_table(client: Client) -> None:
  """Parses Key Value format of CustomTargeting column to columns.

  Args:
    client: BigQuery Client.
  """

  logging.info('Starting STEP01 run query for parsing key values...')
  parsing_queries = []
  for key in _KV:
    parsing_queries.append(
        ('(select SPLIT(KV,"=")[1] from unnest(SPLIT(CustomTargeting, ";"))'
         'KV where REGEXP_CONTAINS(KV, "^{key}=.*")) as {key}'
        ).format(key=key)
    )

  comma_separated_parsing_query = ', '.join(parsing_queries)
  query = f"""
          CREATE OR REPLACE TABLE `{_PARSED_KV_SOURCE}` AS (
            SELECT
              FORMAT_TIMESTAMP("%Y%m%d", _PARTITIONTIME) as yyyymmdd
              , CustomTargeting
              , EstimatedBackfillRevenue
              , AdUnitId
              , {comma_separated_parsing_query}
            FROM
              `{_IMPRESSIONS_SOURCE}`
            WHERE
              CustomTargeting IS NOT NULL
              AND TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP(CURRENT_DATE() - 29) AND TIMESTAMP(CURRENT_DATE() - 1)
          )
          ;
         """

  query_job = client.query(query)
  query_job.result()

  _validate_table_exist(client, _PARSED_KV_SOURCE)
  logging.info('Completed STEP01 run query for parsing key values.')


def create_query(key_pattern: Collection[str]) -> str:
  """Creates a query to run in BigQuery.

  Args:
    key_pattern: A key_pattern to run in SQL for BigQuery.

  Returns:
    A query embedding key_pattern and _PARSED_KV_SOURCE.
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
            `{_PARSED_KV_SOURCE}`
          GROUP BY
            AdUnitId, {comma_separated_keys}
          ;
          """

  return query


def run_query(
    client: Client,
    key_patterns: Collection[str],
    thread_id: int == 1
    ) -> None:
  """Calculate combinations of Key-Value pattern.

  Args:
    client: BigQuery Client.
    key_patterns: A list of key_patterns that you want to calculate impressions,
      eCPM and revenue each key_pattern.
    thread_id: A thread id from calling of run_query.
  """
  thread_no = threading.current_thread().name
  logging.info('Start run_query task id: %s, (thread name: %s)',
               thread_id, thread_no)

  clmns = _KV + list(_TEMPLATE_COLUMNS)
  df_ecpm = pd.DataFrame(columns=clmns)

  for key_pattern in key_patterns:
    query = create_query(key_pattern)
    df_tmp = client.query(query).to_dataframe()
    df_ecpm = pd.concat([df_ecpm, df_tmp], ignore_index=True, sort=False)

  df_ecpm.to_gbq(_OUTPUT_TABLE, _PROJECT_ID, if_exists='append')
  logging.info('Completed run_query task id: %s, (thread name: %s)',
               thread_id, thread_no)


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


def execute_run_query_with_all_key_value_patterns(client: Client) -> None:
  """Executes run_query with all combinations of Key Value pattern.

  Args:
    client: BigQuery Client.
  """
  logging.info('Starting STEP02 the process of running queries with all key'
               'value patterns...')

  key_patterns = execute_combinations_of_kv(_KV)

  split_key_patterns = []
  logging.info('Starting the process of splitting key_patterns = %s ...',
               key_patterns)
  for i in range(0, len(key_patterns), _KEY_SPLIT_NUMBER):
    split_key_patterns.append(key_patterns[i:i+_KEY_SPLIT_NUMBER])
  logging.info('Completed splitting key_patterns = %s', split_key_patterns)

  logging.info('Starting a process of threadPoolExecutor %s ...', _THREAD_NO)
  with concurrent.futures.ThreadPoolExecutor(
      max_workers=_THREAD_NO,
      thread_name_prefix='thread',
      ) as executor:
    for i, split_key_pattern in enumerate(split_key_patterns):
      logging.info('Starting a process i= %s, split_key_pattern = %s ...',
                   i, split_key_pattern)
      executor.submit(run_query, client, split_key_pattern, i)
      logging.info('Completed the process of i = %s, split_key_pattern = %s',
                   i, split_key_pattern)

  _validate_table_exist(client, _OUTPUT_TABLE)
  logging.info('Completed STEP02 the process of running queries with all key'
               'value patterns.')


def run_query_for_distinguishes_outputs(client: Client) -> None:
  """Makes the rows in the output table unique."""

  logging.info('Starting STEP03 Doing distinct output table...')
  query_job = client.query(_DISTINCT_OUTPUT_QUERY)
  query_job.result()
  logging.info('Completed STEP03 Doing distinct output table.')


def _validate_table_exist(client: Client, table_name: str) -> None:
  """Validetes whether table exists in BigQuery.

  Args:
    client: BigQuery Client.
    table_name: A table name in BigQuery.

  Raise:
    ValueError: error occurring when BigQuery table does not exist.
  """

  try:
    client.get_table(table_name)
  except NotFound as e:
    raise ValueError(
        'BigQuery path not exists: {table_name}'.format(table_name=table_name)
    ) from e


def main() -> None:
  """Executes Key Value Recommendation for UPR all steps."""
  client = Client(project=_PROJECT_ID)
  _validate_table_exist(client, _IMPRESSIONS_SOURCE)
  run_query_for_parsing_impression_table(client)

  execute_run_query_with_all_key_value_patterns(client)

  run_query_for_distinguishes_outputs(client)


if __name__ == '__main__':
  main()
