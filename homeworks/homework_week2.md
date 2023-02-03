## Homework

Tasks: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_2_workflow_orchestration/homework.md

### Question 1. Load January 2020 data

```parquet-tools inspect data/green_tripdata_2020-01.parquet```

Answer: 447770

### Question 2. Scheduling with Cron

```prefect deployment build .\web_to_cloud.py:main_flow -n etl_deploy --cron "0 5 1 * *"```

![Cron scheduling](https://ostechnix.com/wp-content/uploads/2018/05/cron-job-format-1.png)

Answer: 0 5 1 * *

### Question 3. Loading data to BigQuery

```
23:48:17.703 | INFO    | Flow run 'delicate-deer' - Executing 'ingest_data-e7246262-0' immediately...
23:49:12.290 | INFO    | Task run 'ingest_data-e7246262-0' - 7049370
```

```
23:49:28.255 | INFO    | Flow run 'delicate-deer' - Executing 'ingest_data-e7246262-1' immediately...
23:50:21.859 | INFO    | Task run 'ingest_data-e7246262-1' - 7866620
```

Answer: 14915990

### Question 4. Github Storage Block

```
pip install prefect-github
prefect block register -m prefect_github
prefect deployment build web_to_cloud.py:main_flow --name etl_deploy --tag dev -sb github/dev
prefect agent start -q 'default'
```

Answer: 88605

### Question 5. Email or Slack notifications

![Email Notification](img_2.png)

Answer: 567852

### Question 6. Secrets

![Secret Block](img_1.png)

Answer: 8