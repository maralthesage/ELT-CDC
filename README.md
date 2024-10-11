# ETL and CDC

In this project, two main scripts, namely, `cdc.py` and `user_rechnungen.py` are automating the process of loading files from DBF to CSV. 

In `cdc.py`, incremental loading is implemented so at each update, the .csv files are not overwritten from the beginning, but instead, only the updates and changes are captured and added from the dbf files to the csv files.

With `user_rechnungen.py`, there is a heavy amount of transformation before loading and since this file is needed to be updated regularly, this is more efficient to have the transformations integrated in the pipeline.

While this code is task-specific, the patterns and general approaches can be adopted and implemented to other, similar problems.