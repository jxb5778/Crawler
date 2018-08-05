import pandas as pd


def pandas_query(reader_agg, query_result_agg, query, value_list=[]):
    """ Helper function that queries the reader and concatenated the results to the query return

    :param reader_agg: v
    :param query_result_agg: Aggregator used to accumulate qury results
    :param pandas_query: string that follows pandas query syntax
    :param value_list: list of values that can be referenced in the pandas_query
    :return: None
    """

    reader_agg.return_value = reader_agg.return_value.query(query)
    query_result_agg.return_value = pd.concat([query_result_agg.return_value, reader_agg.return_value], sort=True)

    return


def crawler_csv_query(file, reader_agg, filter_agg, query_result_agg, query, value_list=[]):
    """ Crawler job that reads in csv files and aggregates query results

    :param file: string file path/ filename that is being operated on
    :param reader_agg: Aggregator used to read in the input file
    :param filter_agg: Aggregator that can be used to operate on individual slices of the reader_agg
    :param query_result_agg: Aggregator used to accumulate qury results
    :param pandas_query: string that follows pandas query syntax
    :param value_list: list of values that can be referenced in the pandas_query
    :return: None
    """

    reader_agg.return_value = pd.read_csv(file, encoding='latin1')
    pandas_query(reader_agg=reader_agg, query_result_agg=query_result_agg, query=query)

    return


def crawler_parquet_query(file, reader_agg, filter_agg, query_result_agg, query, value_list=[]):
    """ Crawler job that reads in parquet files and aggregates query results

    :param file: string file path/ filename that is being operated on
    :param reader_agg: Aggregator used to read in the input file
    :param filter_agg: Aggregator that can be used to operate on individual slices of the reader_agg
    :param query_result_agg: Aggregator used to accumulate qury results
    :param pandas_query: string that follows pandas query syntax
    :param value_list: list of values that can be referenced in the pandas_query
    :return: None
    """

    reader_agg.return_value = pd.read_parquet(file, engine='pyarrow')
    pandas_query(reader_agg=reader_agg, query_result_agg=query_result_agg, query=query)

    return
