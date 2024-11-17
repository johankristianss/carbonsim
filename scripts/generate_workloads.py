import csv
import os
import random
import pandas as pd
import matplotlib.pyplot as plt

slurm_csv_file_path = '/scratch/datacenter-challenge/202201/slurm-log.csv'
app_csv_file_path = '/scratch/datacenter-challenge/202201/labelled_jobids.csv'
base_dir = '/scratch/datacenter-challenge/202201/gpu'
output_dir = '/scratch/cognit/workloads'

slurm_data_dict = {}
with open(slurm_csv_file_path, mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        id_job = row['id_job']
        slurm_data_dict[id_job] = {key: value for key, value in row.items() if key != 'id_job'}


app_data_dict = {}
with open(app_csv_file_path, mode='r') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        id_job = row['id_job']
        app_data_dict[id_job] = {key: value for key, value in row.items() if key != 'id_job'}

subdirs = [os.path.join(base_dir, subdir) for subdir in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, subdir))]

all_files = []
for subdir in subdirs:
    files = [os.path.join(subdir, file) for file in os.listdir(subdir) if os.path.isfile(os.path.join(subdir, file)) and file.endswith('.csv')]
    all_files.extend(files)

selected_files = random.sample(all_files, 100000)

print("Selected Files:")
for file in selected_files:
    print(file)

os.makedirs(output_dir, exist_ok=True)

# idle_threshold = 0

for idx, file in enumerate(selected_files):
    df = pd.read_csv(file)
    df = df[['timestamp', 'power_draw_W']]
    df['timestamp'] = df['timestamp'] - df['timestamp'].min()
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    df['power_draw_W'] = pd.to_numeric(df['power_draw_W'], errors='coerce')

    df.set_index('timestamp', inplace=True)
    
    # Resample and process the entire DataFrame
    #df_resampled = df.resample('1S').mean().dropna().reset_index()
    #df_resampled['timestamp'] = (df_resampled['timestamp'] - df_resampled['timestamp'].min()).dt.total_seconds()


    #above_threshold = df['power_draw_W'] > idle_threshold
    #first_above_idx = df.index[df['power_draw_W'] > idle_threshold][0]

    # first_above_idx = above_threshold.idxmax()  # Get the index of the first occurrence above the threshold
    #df_filtered = df.iloc[first_above_idx:].copy()
    #df_filtered.set_index('timestamp', inplace=True)# Keep all rows from the first occurrence above the threshold onwards
    df_resampled = df.resample('1S').mean().dropna().reset_index()
    df_resampled['timestamp'] = (df_resampled['timestamp'] - df_resampled['timestamp'].min()).dt.total_seconds()

    job_id = os.path.basename(file).split('-')[0]
    
    if job_id in slurm_data_dict:
        job_info = slurm_data_dict[job_id]

        #print(int(job_info['mem_req']))
        #if 	int(job_info['mem_req']) > 90000000:
        #    print("Skipping index: ", idx)
        #    continue
        
        df_resampled.loc[:, 'id_user'] = job_info['id_user']
        df_resampled.loc[:, 'cpus_req'] = job_info['cpus_req']
        df_resampled.loc[:, 'mem_req'] = job_info['mem_req']
        df_resampled.loc[:, 'gres_used'] = job_info['gres_used']
        df_resampled.loc[:, 'timelimit'] = job_info['timelimit']
        df_resampled.loc[:, 'time_submit'] = job_info['time_submit']
        df_resampled.loc[:, 'time_start'] = job_info['time_start']
        df_resampled.loc[:, 'time_end'] = job_info['time_end']
        df_resampled.loc[:, 'time_eligible'] = job_info['time_eligible']
        if job_id in app_data_dict:
            app_info = app_data_dict[job_id]
            print(app_info['model'])
            df_resampled.loc[:, 'model'] = app_info['model']
    
    output_file = os.path.join(output_dir, f'{idx}.csv')
    df_resampled.to_csv(output_file, index=False)
    print(f"Processed data saved to {output_file}")
