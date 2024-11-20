import os
import psutil
import humanize
import pandas as pd
import gc
# import time 
 
# inner psutil function
def process_memory():
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    return mem_info.rss

# decorator function
def memory_usage(func):
    def wrapper(*args, **kwargs):

        mem_before = process_memory()
        result = func(*args, **kwargs)
        mem_after = process_memory()
        print("{}:consumed memory: {:,}".format(
            func.__name__,
            mem_before, mem_after, mem_after - mem_before))

        return result
    return wrapper



@memory_usage
def first_query():
    df = pd.read_csv("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")
    filtered_df = df[df["Electric Vehicle Type"].notna() & (df["City"] == "Seabeck")]

    # Select specific columns
    filtered_df = filtered_df[["City", "Electric Vehicle Type"]]
    return filtered_df
@memory_usage
def second_query():
    # Load the CSV file into a DataFrame
    df = pd.read_csv("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")
    avg_df = df["Electric Range"].mean()
    return avg_df
@memory_usage
def third_query():
    df = pd.read_csv("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")
    # # Load the data
    res_df = df[df["Legislative District"] == 15]
    return res_df
@memory_usage
def fourth_query():
    df = pd.read_csv("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")
    # Calculate the maximum of the 'Electric Range' column
    max_range = df["Electric Range"].max()

    # Filter rows where 'Electric Range' is equal to the maximum value
    high_range_df = df[df["Electric Range"] == max_range]

    # Select 'Make' and 'Model' columns
    high_range_df = high_range_df[["Make", "Model"]]
    return high_range_df
@memory_usage
def fifth_query():
    df = pd.read_csv("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")
    # Group by 'County', 'Model Year', and 'Make' and count occurrences of 'Make'
    county_df = df.groupby(['County', 'Model Year', 'Make']).size().reset_index(name='Count')
    return county_df
@memory_usage
def sixth_query():
    df = pd.read_csv("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")
    electric_df = df.groupby(['Electric Vehicle Type', 'Electric Utility']).size().reset_index(name='EV Type Count')
    return electric_df
@memory_usage
def seventh_query():
    df = pd.read_csv("E:\\rust\\csv_processing\\Electric_Vehicle_Population_Data.csv")
    bd_df = df.groupby(['Make', 'Model']).size().reset_index(name='Vehicle Count')
    return bd_df

f1 = first_query()
f2 = second_query()
f3 = third_query()
f4 = fourth_query()
f5 = fifth_query()
f6 = sixth_query
f7 = seventh_query()
