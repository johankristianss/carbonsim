import os
import pandas as pd
import queue
import time

def get_sorted_filenames(directory):
    filenames = os.listdir(directory)
    csv_filenames = [f for f in filenames if f.endswith('.csv')]
    sorted_filenames = sorted(csv_filenames, key=lambda x: int(x.split('.')[0]))
    return sorted_filenames

def process_file(filepath):
    df = pd.read_csv(filepath)
    print(f"Processing file: {filepath}")
    for index, row in df.iterrows():
        print(f"Row {index} - cpus_req: {row['cpus_req']}")
        time.sleep(1/10000) 

def replay_simulator(directory, max_files=10):
    sorted_filenames = get_sorted_filenames(directory)
    file_queue = queue.Queue()
    for filename in sorted_filenames:
        file_queue.put(filename)

    processed_count = 0
    while not file_queue.empty() and processed_count < max_files:
        filename = file_queue.get()
        filepath = os.path.join(directory, filename)

        print(filepath)
        process_file(filepath)

        file_queue.task_done()
        processed_count += 1

if __name__ == "__main__":
    directory = "/scratch/cognit/workloads"
    replay_simulator(directory, max_files=100)
