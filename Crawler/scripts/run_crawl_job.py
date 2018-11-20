"""
Author- Justin Berg
Date- July 28, 2018
"""

from Crawler.crawler_API_lib import *
from Crawler.crawler_lib import *
import multiprocessing as mp

# result_filename needs to be provided to provide query results

if __name__ == '__main__':

    run_crawl_job(
        input_files=files_from_directory('C:/dev/data/twitter_election_integrity/ira_tweets_csv_hashed/ira_tweets_csv/'),
        apply_func=crawler_csv_to_parquet,
        apply_func_args={
            'dest_dir': 'C:/dev/data/twitter_election_integrity/ira_tweets_csv_hashed/ira_tweets_parquet/'
        },
        count_processes= 2
    )
