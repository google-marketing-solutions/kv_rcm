# Key-Value recommendation for UPR

## Problem
Determining the optimal value of UPR using key-value often depends on
experience, and the optimal value is difficult.

## Solution
Investigate the impression, eCPM, and revenue for each key value of past Data
Transfer, and analyze to propose the optimal UPR.


## Requirements
- Python 3.10.9+
- Google Cloud
  - Create a BigQuery project and dataset before executing this solution.
- [Google Ad Manager Data Transfer](https://support.google.com/admanager/answer/1733124)
  - [Recommend] A or some ad Unit should have 10M impressions for 28 days.
  - [BigQuery Data Transfer Service](https://cloud.google.com/bigquery/docs/doubleclick-publisher-transfer)
  for Google Ad Manager Data Transfer
  - This solution requires `NetworkBackfillImpressions` file type.


## Usage
1. Set up a Python execute environment. This solution require Python
version 3.10.9+.

2. Install python libraries.
```
pip install -r requirements.txt
```

3. Kye Value Recommendation for UPR can be run with either
[Service](https://developers.google.com/workspace/guides/create-credentials#service-account)
or [User](https://developers.google.com/workspace/guides/create-credentials#oauth-client-id)
credentials. Service credentials are
ideal for most workflows however you have the option to use either.
Please follow Google Cloud Security Best Practices when handling credentials.
  1. For Service you have 2 options:
      - Be sure to grant the service the IAM Roles
      roles/bigquery.dataOwner
      and roles/bigquery.jobUser.
  2. For User
      - Follow these [install references](https://cloud.google.com/sdk/docs/install)
      and [setting up reference](https://cloud.google.com/sdk/docs/authorizing#user-account)

4. Write down your Ad Manager Data Transfer environment and your Google Cloud
environment in `configs.yaml`.

  ```
  project_id: Project ID for which you want to use this solution.
  dataset_name: A dataset name to save tables.
  parsed_kv_table: A table that split each key values to each columns from the impression table (default = STEP01_PARSED_KV_TABLE).
  aggregated_data_with_kv: A table with eCPM, impressions, revenue by key value pattern including duplicates in the last 28 days (default = STEP02_AGGREGATED_DATA_WITH_KV).
  distinct_table: A table with eCPM, impressions, revenue by key value pattern excluding duplicates in the last 28 days (default = STEP03_AGGREGATED_eCPM_DATA_WITHOUT_DUPLICATES).
  network_backfill_impressions: Data Transfer table name (example, p_NetworkBackfillImpressions).
  key_patterns: ['kv_age', 'kv_keyword', 'kv_genre', ....] (key names in key-value pairs set up in Ad Manager)
  ```

5. run main.py
  ```
  python main.py
  ```

  Output tables are below.

  ```
  STEP01_PARSED_KV_TABLE: A table that splits key value data into each key column.
  column.
  STEP02_AGGREGATED_DATA_WITH_KV: eCPM, impressions, revenue by key value pattern including duplicates in the last 28 days.
  STEP03_AGGREGATED_eCPM_DATA_WITHOUT_DUPLICATES: eCPM, impressions, revenue by key value pattern excluding duplicates in the last 28 days.
  ```

6. Look for the UPR in each Ad Unit
  - Spreadsheet way

      1. extract data from `STEP03_AGGREGATED_eCPM_DATA_WITHOUT_DUPLICATES`
      with below query.

          ```
          SELECT
            *
          FROM
            `STEP03_AGGREGATED_eCPM_DATA_WITHOUT_DUPLICATES`
          WHERE
            AdUnitID = 123456
          ```
      2. Export the query results to a spreadsheet in the BigQuery UI.

      3. Look for pairs with large impressions and high eCPM.
      For example, impressions is more than 10,000,000 and eCPM is more than
      $0.2 higher than average eCPM.

- Looker Studio way (TBA)
