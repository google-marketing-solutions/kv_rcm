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

"""Tests for main.py."""

import unittest
from unittest import mock
from google.cloud import exceptions
import main


_DUMMY_KV = (['kv_1', 'kv_2', 'kv_3'])
_DUMMY_KV_PATTERNS = [
    ['kv_1'],
    ['kv_2'],
    ['kv_3'],
    ['kv_1', 'kv_2'],
    ['kv_1', 'kv_3'],
    ['kv_2', 'kv_3'],
    ['kv_1', 'kv_2', 'kv_3'],
    ]
_FAKE_THREAD_ID = 1
_FAKE_KEY_SPLIT_NUMBER = 2

_DUMMY_KV_PATTERN = _DUMMY_KV_PATTERNS[-1]
_FAKE_IMPRESSIONS_SOURCE = 'fake_impressions_source'
_FAKE_PROJECT_ID = 'fake_project_id'
_FAKE_DATASET_NAME = 'fake_dataset_name'
_FAKE_PARSED_KV_TABLE = 'fake_parsed_kv_table'
_FAKE_AGGREGATED_DATA_WITH_KV = 'fake_aggreagated_data_with_kv'
_FAKE_DISTINCT_TABLE = 'fake_distinct_table'
_FAKE_PARSED_KV_SOURCE = _FAKE_DATASET_NAME + '.' + _FAKE_PARSED_KV_TABLE
_FAKE_OUTPUT_TABLE = _FAKE_DATASET_NAME + '.' + _FAKE_AGGREGATED_DATA_WITH_KV
_FAKE_DISTINCT_OUTPUT_TABLE = _FAKE_DATASET_NAME + '.' + _FAKE_DISTINCT_TABLE

_DUMMY_QUERY = f"""
               SELECT
                 {', '.join(_DUMMY_KV_PATTERN)}
                 , AdUnitId
                 , SUM(EstimatedBAckfillRevenue) as sum_revenue
                 , SUM(EstimatedBAckfillRevenue) / COUNT(1) * 1000 as eCPM
                 , COUNT(1) as impressions
               FROM
                 `{_FAKE_PARSED_KV_SOURCE}`
               GROUP BY
                 AdUnitId, kv_1, kv_2, kv_3
               ;
               """
_FAKE_DISTINCT_OUTPUTS_QUERY = f"""
                               CREATE OR REPLACE TABLE '{_FAKE_DISTINCT_OUTPUT_TABLE}' AS (
                                 SELECT DISTINCT
                                   *
                                 FROM
                                   `{_FAKE_OUTPUT_TABLE}`
                               )
                               """


class MainTest(unittest.TestCase):

  def setUp(self):
    super().setUp()
    self.addCleanup(mock.patch.stopall)
    mock.patch('main._KV', _DUMMY_KV).start()
    mock.patch('main._PROJECT_ID', _FAKE_PROJECT_ID).start()
    mock.patch('main._DATASET_NAME', _FAKE_DATASET_NAME).start()
    mock.patch('main._PARSED_KV_TABLE', _FAKE_PARSED_KV_TABLE).start()
    mock.patch('main._AGGREGATED_DATA_WITH_KV',
               _FAKE_AGGREGATED_DATA_WITH_KV).start()
    mock.patch('main._PARSED_KV_SOURCE', _FAKE_PARSED_KV_SOURCE).start()
    mock.patch('main._OUTPUT_TABLE', _FAKE_OUTPUT_TABLE).start()
    mock.patch('main._DISTINCT_TABLE', _FAKE_DISTINCT_TABLE).start()
    mock.patch('main._DISTINCT_OUTPUT_TABLE',
               _FAKE_DISTINCT_OUTPUT_TABLE).start()
    mock.patch('main._DISTINCT_OUTPUT_QUERY',
               _FAKE_DISTINCT_OUTPUTS_QUERY).start()
    mock.patch('main._IMPRESSIONS_SOURCE',
               _FAKE_IMPRESSIONS_SOURCE).start()
    mock.patch('main._KEY_SPLIT_NUMBER',
               _FAKE_KEY_SPLIT_NUMBER).start()
    self.mock_client = mock.patch.object(main,
                                         'Client',
                                         autospec=True).start()

  @mock.patch.object(main, '_validate_table_exist', return_value=None)
  def test_execute_run_query_for_parsing_impression_table(
      self,
      mock_validate_table_exist,
  ):
    expected_selects = []
    for key in _DUMMY_KV:
      expected_selects.append(
          ('(select SPLIT(KV,"=")[1] from unnest(SPLIT(CustomTargeting, ";"))'
           'KV where REGEXP_CONTAINS(KV, "^{key}=.*")) as {key}'
           ).format(key=key)
      )

    comma_separated_parsing_query = ', '.join(expected_selects)
    expected_query = f"""
          CREATE OR REPLACE TABLE `{_FAKE_PARSED_KV_SOURCE}` AS (
            SELECT
              FORMAT_TIMESTAMP("%Y%m%d", _PARTITIONTIME) as yyyymmdd
              , CustomTargeting
              , EstimatedBackfillRevenue
              , AdUnitId
              , {comma_separated_parsing_query}
            FROM
              `{_FAKE_IMPRESSIONS_SOURCE}`
            WHERE
              CustomTargeting IS NOT NULL
              AND TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) BETWEEN TIMESTAMP(CURRENT_DATE() - 29) AND TIMESTAMP(CURRENT_DATE() - 1)
          )
          ;
         """

    main.run_query_for_parsing_impression_table(self.mock_client)

    self.mock_client.query.assert_called_once_with(
        expected_query
    )
    mock_validate_table_exist.assert_called_once_with(
        self.mock_client,
        _FAKE_PARSED_KV_SOURCE,
    )

  def test_execute_combinations_of_kv(self):
    actual_list = main.execute_combinations_of_kv(_DUMMY_KV)

    self.assertEqual(_DUMMY_KV_PATTERNS, actual_list)

  def test_execute_create_query(self):
    actual_query = main.create_query(_DUMMY_KV_PATTERN)

    self.assertEqual(_DUMMY_QUERY.replace(' ', ''),
                     actual_query.replace(' ', ''))

  @mock.patch.object(main, '_validate_table_exist')
  @mock.patch.object(main, 'pd', autospec=True)
  def test_execute_run_query(self, mock_pd, _):
    call_queries = []
    for key in _DUMMY_KV:
      call_queries.append(mock.call(main.create_query(key)))
      call_queries.append(mock.call().to_dataframe())

    main.run_query(self.mock_client, _DUMMY_KV, _FAKE_THREAD_ID)

    mock_pd.DataFrame.assert_called_once_with(
        columns=_DUMMY_KV+list(main._TEMPLATE_COLUMNS)
    )
    self.mock_client.query.assert_has_calls(call_queries)
    mock_pd.concat.assert_has_calls([
        mock.call([mock.ANY, mock.ANY], ignore_index=True, sort=False),
        mock.call([mock.ANY, mock.ANY], ignore_index=True, sort=False),
        mock.call([mock.ANY, mock.ANY], ignore_index=True, sort=False),
    ])
    mock_pd.DataFrame.to_gbq(main._OUTPUT_TABLE,
                             main._PROJECT_ID,
                             if_exists='append',
                             )

  @mock.patch.object(main, '_validate_table_exist')
  @mock.patch.object(main, 'run_query', autospec=True)
  @mock.patch.object(main, 'concurrent', autospec=True)
  def test_execute_run_query_with_all_key_value_patterns_and_load_configs(
      self,
      mock_concurrent,
      mock_run_query,
      mock_validate_table_exist,
      ):
    mock_executor = (
        mock_concurrent.futures.ThreadPoolExecutor.return_value.__enter__
        .return_value
    )

    main.execute_run_query_with_all_key_value_patterns(self.mock_client)

    mock_executor.submit.assert_has_calls([
        mock.call(
            mock_run_query,
            self.mock_client,
            [['kv_1'], ['kv_2']],
            0,
        ),
        mock.call(
            mock_run_query,
            self.mock_client,
            [['kv_3'], ['kv_1', 'kv_2']],
            1,
        ),
        mock.call(
            mock_run_query,
            self.mock_client,
            [['kv_1', 'kv_3'], ['kv_2', 'kv_3']],
            2,
        ),
        mock.call(
            mock_run_query,
            self.mock_client,
            [['kv_1', 'kv_2', 'kv_3']],
            3,
        ),
    ])
    self.assertEqual(main._PROJECT_ID, _FAKE_PROJECT_ID)
    self.assertEqual(main._PARSED_KV_TABLE, _FAKE_PARSED_KV_TABLE)
    self.assertEqual(main._AGGREGATED_DATA_WITH_KV,
                     _FAKE_AGGREGATED_DATA_WITH_KV)
    self.assertEqual(main._PARSED_KV_SOURCE, _FAKE_PARSED_KV_SOURCE)
    self.assertEqual(main._OUTPUT_TABLE, _FAKE_OUTPUT_TABLE)
    mock_validate_table_exist.assert_called_once_with(
        self.mock_client,
        main._OUTPUT_TABLE,
    )

  def test_execute_run_query_for_distinguishes_outputs(self):
    main.run_query_for_distinguishes_outputs(self.mock_client)

    self.assertEqual(_FAKE_DISTINCT_OUTPUTS_QUERY.replace(' ', ''),
                     main._DISTINCT_OUTPUT_QUERY.replace(' ', ''))
    self.mock_client.query.assert_called_once_with(_FAKE_DISTINCT_OUTPUTS_QUERY)

  def test_check_dataset_exists_with_success(self):
    main._validate_table_exist(self.mock_client, _FAKE_IMPRESSIONS_SOURCE)

  def test_check_dataset_exists_with_failue(self):
    mock_client = mock.MagicMock()
    mock_client.get_table.side_effect = exceptions.NotFound('A NotFound error')
    with self.assertRaises(ValueError):
      main._validate_table_exist(mock_client, '')


if __name__ == '__main__':
  unittest.main()
