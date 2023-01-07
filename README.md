# Project4 Data Lake

## 1. Goal in the Project
### 1.1. The purpose of this project in Sparkify
Because Sparkify company want to move their data warehouse to a data lake. They had put their data in S3 and they want us
help them to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables.

### 1.2 Their analytical goals
They want to use song data and user event data to analyze these users behavior, such as what songs their users are listening to and what time or how many times they listened...etc.

## 2. State and justify my schema design and ETL pipeline
The schema design and ETL pipeline code in etl.py file. There are two method to process this.

#### 2.1.1 process_song_data method
We use spark dataframe to keep song_data, then extract data from dataframe then write to songs_table(S3 songs folder) partition by year and artist and write to artists_table(S3 artists folder).

#### 2.1.2 process_log_data method
We use spark dataframe to keep log_data, then extract data from dataframe then write to users_table(S3 users folder) and write to time_table(S3 time folder)partition by year and month.
And we use spark sql to create template view to build songplays_table(S3 songplays folder)partition by year and month.

## 3. Provide example queries and results for song play analysis.
I had create two jupyter notebook file to implement the project, as follows
1. etlCodeTest.ipynb: Use to check etl every expression is correct to work.
2. execute.ipynb: Use the file to execute etl.py, then analyze the tables schema and example queries.

Please read the execute.ipynb result to check my example queries, thanks.