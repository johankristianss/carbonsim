import json
import os
import time
from edge_cluster import EdgeCluster
from process import Process
from scheduler import Scheduler
import numpy as np

scheduler = Scheduler()

max_utilization_grade = 0.7 
force_cluster_utilization = False 
random_wait = True 
rate = 1.0  # Rate parameter (lambda) for the exponential distribution

#with open('/scratch/cognit/edge-clusters-test.json', 'r') as f:
with open('/scratch/cognit/edge-clusters.json', 'r') as f:
    edge_clusters_data = json.load(f)

for cluster_data in edge_clusters_data:
    edge_cluster = EdgeCluster(
        cluster_data['name'],
        cluster_data['nodes'],
        cluster_data['gpus_per_node'],
        cluster_data['carbon-intensity-trace']
    )

    print("adding edge cluster: ", edge_cluster.name)
    scheduler.add_edge_cluster(edge_cluster)

workload_dir = "/scratch/cognit/filtered_workloads"
csv_files = sorted([f for f in os.listdir(workload_dir) if f.endswith('.csv')])

tick = 0
for idx, csv_file in enumerate(csv_files):
    if tick > 7*24*60*60: # 7 days
        break
    if idx == 1000: # stop adding new processes
        # tick until all processes finished running
        while scheduler.num_running_processes > 0:
            print(scheduler.num_running_processes, "processes still running, tick:", tick)
            scheduler.tick()
            tick += 1
        break
    process = Process(f"test_process_{idx+1}", 0, os.path.join(workload_dir, csv_file))
    ok = scheduler.run(process)
    if not ok:
        # tick until an edge-cluster is available
        print("waiting for cluster to become available, tick: ", tick)
        while not scheduler.run(process):
            scheduler.tick()
            tick += 1
  
    if random_wait:
        waiting_time = np.random.exponential(1.0 / rate)
        ticks_to_wait = int(waiting_time) * 2000
        print("waiting (random tick), tick: ", tick)
    
        for _ in range(ticks_to_wait):
            scheduler.tick()
            tick += 1
            #print("waiting, utilization:", scheduler.get_total_utilization()*100, "% tick: ", tick) 

    # Check cluster utilization
    if force_cluster_utilization:
        while scheduler.get_total_utilization() > max_utilization_grade:
            print("waiting for utilization to drop, utilization: ", scheduler.get_total_utilization()*100, "% tick: ", tick) 
            scheduler.tick()
            tick += 1
    else:
        print("utilization: ", scheduler.get_total_utilization()*100, "% tick: ", tick) 

    scheduler.tick()
    tick += 1

    print("tick: ", tick)

print("Total carbon emission [g]: ", scheduler.cumulative_emission)
print("Total energy [kWh]: ", scheduler.cumulative_energy)
