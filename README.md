# Crypto Data Pipeline Using GCP
The Crypto Data Pipeline is an automated data processing project that extracts cryptocurrency market data from the CoinGecko API. This pipeline is designed to provide a seamless flow of data from extraction to visualization, making it easier to analyze trends in the cryptocurrency market.

### Architectue
![Architecture Diagram](https://github.com/mrpatil34/Crypto-data-Pipeline-using-GCP/blob/main/Architecture.png)

### Data Sources
- CoinGecko API: Provides real-time cryptocurrency market data, including prices, market capitalization, and trading volume.</br > API URL : (https://api.coingecko.com/api/v3/)

### Service Used:
- **Google Cloud Storage (GCS)**: For storing raw and transformed data.
- **Google BigQuery**: For data warehousing and analysis.
- **Looker Studio**: For visualizing data through interactive dashboards.
- **Cloud Composer**: For orchestrating the entire data pipeline.

### Project Flow
The Crypto Data Pipeline operates as follows:

1. **Data Extraction**:
  - Extracts data from the CoinGecko API using Python.
  - Stores the raw data in JSON format in the raw folder of a Google Cloud Storage (GCS) bucket.

2. **Data Transformation**:
  - Performs necessary transformations on the extracted data.
  - Saves the cleaned data in CSV format in the transformed folder of the GCS bucket.

3. **Data Loading**:
  - Loads the transformed data into Google BigQuery for analysis.

4. **Data Visualization**:
  - Visualizes the data using Looker Studio, creating interactive dashboards and reports.

5. **Orchestration**:
  - The entire pipeline is orchestrated using Google Cloud Composer, ensuring reliable execution and scheduling of the ETL tasks.

### Dashboard in Looker Studio
![Dashboard](https://github.com/mrpatil34/Crypto-data-Pipeline-using-GCP/blob/main/Crypto-currency%20Dashboard.png)
