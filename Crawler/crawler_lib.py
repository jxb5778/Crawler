import pandas as pd
import os


def pandas_query(crawler, query, value_list=[]):
    """ Helper function that queries the reader and append the results to the query return

    :param crawler: Crawler object, attributes used-
                reader: DataFrame of the current file read into memory
                query_return: list of DataFrames holding query results
    :param pandas_query: string that follows pandas query syntax
    :param value_list: list of values that can be referenced in the pandas_query
    :return: None
    """

    crawler.reader = crawler.reader.query(query)
    crawler.query_return.append(crawler.reader)

    return


def crawler_csv_query(file, crawler, query, value_list=[]):
    """ Crawler job that reads in csv files and aggregates query results

    :param file: string file path/ filename that is being operated on
    :param crawler: Crawler object, attributes used-
                reader: DataFrame of the current file read into memory
                query_return: list of DataFrames holding query results
    :param pandas_query: string that follows pandas query syntax
    :param value_list: list of values that can be referenced in the pandas_query
    :return: None
    """

    crawler.reader = pd.read_csv(file, encoding='latin1')
    pandas_query(crawler=crawler, query=query, value_list=value_list)

    return


def crawler_parquet_query(file, crawler, query, value_list=[]):
    """ Crawler job that reads in parquet files and aggregates query results

    :param file: string file path/ filename that is being operated on
    :param crawler: Crawler object, attributes used-
                reader: DataFrame of the current file read into memory
                query_return: list of DataFrames holding query results
    :param pandas_query: string that follows pandas query syntax
    :param value_list: list of values that can be referenced in the pandas_query
    :return: None
    """

    crawler.reader = pd.read_parquet(file, engine='pyarrow')
    pandas_query(crawler=crawler, query=query, value_list=value_list)

    return


def crawler_csv_to_parquet(file, crawler, dest_dir):
    print(file)
    crawler.reader = pd.read_csv(file, low_memory=False)
    dest_filename = '{0}{1}.parquet'.format(dest_dir, os.path.basename(file).replace('.csv', ''))
    crawler.reader.to_parquet(dest_filename, engine='pyarrow')

    return


def crawler_parquet_query_from_file(file, crawler, query, value_list_file='', value_column=''):
    """ Crawler job that reads in parquet files and aggregates query results

    :param file: string file path/ filename that is being operated on
    :param crawler: Crawler object, attributes used-
                reader: DataFrame of the current file read into memory
                query_return: list of DataFrames holding query results
    :param pandas_query: string that follows pandas query syntax
    :param value_list_file: string filename for csv to be read into @crawler.filter,
                            and will be used to query against the data
    :param value_column: string for the column name of the data you want read from the file
    :return: None
    """

    print(file)

    crawler.reader = pd.read_parquet(file, engine='pyarrow')

    if crawler.filter is None:
        crawler.filter = pd.read_csv(value_list_file)[value_column]

    pandas_query(crawler=crawler, query=query, value_list=crawler.filter)

    return


def crawler_csv_query_from_file(file, crawler, query, value_list_file='', value_column=''):
    """ Crawler job that reads in parquet files and aggregates query results

    :param file: string file path/ filename that is being operated on
    :param crawler: Crawler object, attributes used-
                reader: DataFrame of the current file read into memory
                query_return: list of DataFrames holding query results
    :param pandas_query: string that follows pandas query syntax
    :param value_list_file: string filename for csv to be read into @crawler.filter,
                            and will be used to query against the data
    :param value_column: string for the column name of the data you want read from the file
    :return: None
    """

    print(file)

    crawler.reader = pd.read_csv(file, encoding='latin1')

    if crawler.filter is None:
        crawler.filter = pd.read_csv(value_list_file)[value_column]

    pandas_query(crawler=crawler, query=query, value_list=crawler.filter)

    return
