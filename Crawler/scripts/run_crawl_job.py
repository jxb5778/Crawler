"""
Author- Justin Berg
Date- July 28, 2018
"""

from Crawler.crawler_API_lib import *
from Crawler.crawler_lib import *

# result_filename needs to be provided to provide query results

if __name__ == '__main__':
    run_crawl_job(
        file_list=files_from_directory('C:/'),
        apply_func=crawler_csv_query,
        apply_func_args={
            'pandas_query': 'Column in @value_list',
            'value_list': [1, 2, 3, 4, 5]
        },
        result_filename='',
        count_processes=8
    )
