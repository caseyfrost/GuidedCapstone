# GuidedCapstone

## Data Ingestion Outputs
1. The data ingestion notebook takes the raw text files from azure storage, processes them by date, and uploads them to azure storage partitioned by type (Quote or Trade).
2. ![Screen Shot 2022-05-02 at 7 14 50 PM](https://user-images.githubusercontent.com/20688436/166396313-df2fa82a-93cf-48bb-8c25-9e6be7d17e9f.png)


## End of Day Data Load Outputs
1. The end of day data load notebook takes the merges the partitioned files so there is one quote for and one trade file per day
2. ![Screen Shot 2022-05-02 at 7 33 15 PM](https://user-images.githubusercontent.com/20688436/166396605-2d775f87-e3ae-40f3-a169-e29c511d8a8f.png)


## Analytical ETL Output
1. The analytical etl notebook creates a 30 minute running average trade price for the current day and the end of the past day.
2. It unions the quote and trade data together to find the last trade price before each quote. (Shown in noteobok)
3. Finally it writes the output back to azure storage
4. ![Screen Shot 2022-05-02 at 7 36 30 PM](https://user-images.githubusercontent.com/20688436/166396789-9dac895d-54ad-40ed-8081-55988b7f8bc7.png)
