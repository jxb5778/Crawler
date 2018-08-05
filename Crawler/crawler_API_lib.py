from Aggregator import Aggregator
import pandas as pd
import multiprocessing as mp
import numpy as np
import os


def files_from_directory(directory):
    """ Provides a list of all the files the provided directory

    :param directory: string filename that will be searched
    :return: list of string of filenames
    """
    return [f'{directory}{f}' for f in os.listdir(directory) if os.path.isfile(f'{directory}{f}')]


def map_file_apply(file_list, apply_func, apply_func_args):
    """ Main controller for the crawl job, applies the provided func and args to each file

    :param file_list: list of files that will be operated on by @apply_func
    :param apply_func: function to be applied to each file
    :param apply_func_args: dict of keyword args needed for @apply_func
    :return: data aggregated in the @query_result_agg
    """

    reader_agg = Aggregator()
    filter_agg = Aggregator()
    query_result_agg = Aggregator()

    for file in file_list:
        print(file)
        apply_func(file, reader_agg, filter_agg, query_result_agg, **apply_func_args)

    return query_result_agg.return_value


def run_crawl_job(input_files, apply_func, apply_func_args, result_filename='', count_processes=8):
    """ Crawl jobs provide ease in concurrently interacting with large sets of data in a filesystem

    :param input_files: list of files to be operated on
    :param apply_func: function that will be applied to each file in @input_files
    :param apply_func_args: dict of keyword args needed for @apply_func
    :param result_filename: string file name/ path for the resulting file -> default is ''
    :param count_processes: int, number of processes that will be concurrently run
    :return: None
    """

    split_file_list = np.array_split(input_files, count_processes)

    outputs = Aggregator()

    pool = mp.Pool(count_processes)

    results = [
        pool.apply_async(
            func=map_file_apply, args=(file_list, apply_func, apply_func_args)
        ) for file_list in split_file_list
    ]

    if result_filename == '':
        for p in results:
            p.get()
    else:
        outputs.return_value = [p.get for p in results]
        outputs.return_value = pd.concat(outputs.return_value, sort=True)
        outputs.return_value.to_csv(result_filename, index=False)

    return
