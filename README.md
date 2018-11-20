# Crawler
Crawler utilizes multiprocessing to concurrently apply a function to files

Required packages- pandas, numpy, pyarrow

Crawler was developed trying to explore just how far you can go in big/ large data with just a desktop. Turns out if it fits it ships...
The first stepping stone was Buffer, which eliminates pandas' RAM overhead from creatating copies of dataframes during trasnformations.
Crawler makes good use of Buffers, and scales up the operation using multiprocessing
