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
from typing import Collection

# TODO(): Replace it with OS env in the next CL.
_INPUT_TABLE_STEP1 = ('sample.table_step01')


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
            `{_INPUT_TABLE_STEP1}`
          GROUP BY
            AdUnitId, {comma_separated_keys}
          ;
          """

  return query


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
