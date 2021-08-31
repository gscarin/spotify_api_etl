# My first Data Engineering Project

I wanted to est my skills on python, such as learning how to use Apache Airflow.

The project itself uses Spotify API to get last listened songs, making a ETL process and finally inserteing them into a SQLite databse.

Also using AirFlow to schedule daily process.

_Spotify URL to get api-key_: _https://developer.spotify.com/console/get-recently-played/_

```mermaid
graph TD
A[AirFlow DAG] ----> B((Spotify ETL))
B --> C(Connect API)
B --> D(Connect Database)
D --> E(Insert unique songs into database)
C --> F{Final}
E --> F{Final}

```

