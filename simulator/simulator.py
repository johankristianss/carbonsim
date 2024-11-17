import json
import os
from edge_cluster import EdgeCluster
from process import Process
from scheduler import Scheduler
import numpy as np

class Simulator:
    def __init__(self,
                 max_processes,
                 max_days,
                 cluster_utilization_threshold ,
                 random_wait,
                 wait_time,
                 rate, 
                 workload_dir, 
                 cluster_config,
                 result_dir):
        self.__random_wait = random_wait
        self.__rate = rate
        self.__workload_dir = workload_dir
        self.__cluster_config = cluster_config
        self.__cluster_utilization_threshold = cluster_utilization_threshold
        self.__max_processes = max_processes
        self.__max_days = max_days
        self.__wait_time = wait_time

        self.result_dir = result_dir
        if not os.path.exists(self.result_dir):
            print("creating result dir: ", self.result_dir)
            os.makedirs(self.result_dir)
        
        self.scheduler = Scheduler( csv_filename=result_dir + "/scheduler.csv")

    def should_finish(self, tick):
        return tick > self.__max_days*24*60*60

    def start(self):
        with open(self.__cluster_config, 'r') as f:
            edge_clusters_data = json.load(f)
        
        for cluster_data in edge_clusters_data:
            edge_cluster = EdgeCluster(
                cluster_data['name'],
                cluster_data['nodes'],
                cluster_data['gpus_per_node'],
                cluster_data['carbon-intensity-trace'],
                cluster_data['gpu_cost_euro_per_second'],
                self.__cluster_utilization_threshold,
            )
            self.scheduler.add_edge_cluster(edge_cluster)
        
        csv_files = sorted([f for f in os.listdir(self.__workload_dir) if f.endswith('.csv')])
        
        tick = 0
        for idx, csv_file in enumerate(csv_files):
            if self.should_finish(tick):
                break
            if idx == self.__max_processes: # stop adding new processes
                # tick until all processes finished running
                while self.scheduler.num_running_processes > 0:
                    print(self.scheduler.num_running_processes, "processes still running, tick:", tick)
                    self.scheduler.tick()
                    tick += 1
                    if self.should_finish(tick):
                        break
                break
            process = Process(f"test_process_{idx+1}", 0, os.path.join(self.__workload_dir, csv_file))
            ok = self.scheduler.run(process)
            if not ok:
                # tick until an edge-cluster is available
                print("waiting for a cluster to become available, tick: ", tick)
                while not self.scheduler.run(process):
                    self.scheduler.tick()
                    tick += 1
                    if self.should_finish(tick):
                        break
          
            if self.__random_wait:
                waiting_time = np.random.exponential(1.0 / self.__rate)
                ticks_to_wait = int(waiting_time * self.__wait_time)
                print("waiting (random tick), tick: ", ticks_to_wait)
            
                for _ in range(ticks_to_wait):
                    self.scheduler.tick()
                    tick += 1
                    if self.should_finish(tick):
                        break
      
            self.scheduler.tick()
            tick += 1
        
        print()
        print("------------------------- Simulation finished -------------------------")
        print("Total number of GPUs: ", self.scheduler.total_gpus)
        print("Total carbon emission [g]: ", self.scheduler.cumulative_emission)
        print("Total GPU energy [kWh]: ", self.scheduler.cumulative_energy)
        print("Total GPU cost [Euro]: ", self.scheduler.total_gpu_cost)
        print("Total finished processes: ", self.scheduler.finished_processes)
        print("Total processing time [h]: ", self.scheduler.total_processing_time/60/60)
        print("Theoretical max processing time [h]: ", self.scheduler.total_gpus * tick / 60 / 60)
        print("Total time [h]: ", tick/60/60)
