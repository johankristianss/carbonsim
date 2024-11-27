import json
import os
from edge_cluster import EdgeCluster
from process import Process
from scheduler import Scheduler
import csv

class Simulator:
    def __init__(self,
                 alg,
                 max_processes,
                 max_days,
                 cluster_utilization_threshold,
                 log_file_dir,
                 log_file,
                 workload_dir,
                 workloads_stats_dir,
                 cluster_config,
                 result_dir):
        self.__workload_dir = workload_dir
        self.__workloads_stats_dir = workloads_stats_dir
        self.__cluster_config = cluster_config
        self.__cluster_utilization_threshold = cluster_utilization_threshold
        self.__log_file_dir = log_file_dir
        self.__log_file = log_file
        self.__max_processes = max_processes
        self.__max_days = max_days
        self.__alg = alg

        self.result_dir = result_dir
        if not os.path.exists(self.result_dir):
            print("creating result dir: ", self.result_dir)
            os.makedirs(self.result_dir)
        
        self.scheduler = Scheduler(csv_filename=result_dir + "/scheduler.csv", alg=self.__alg)

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
                self.result_dir + "/" + cluster_data['name'] + ".csv"
            )
            self.scheduler.add_edge_cluster(edge_cluster)

        log_file_path = os.path.join(self.__log_file_dir, self.__log_file)
        with open(log_file_path, 'r') as log_file:
            reader = csv.DictReader(log_file)

            csv_arr = []

            for row in reader:
                file_name = row["File Name"]
                ticks_to_wait = int(row["Random Wait (ticks)"])

                csv_dict = {
                    "csv_file": file_name,
                    "ticks_to_wait": ticks_to_wait
                }

                csv_arr.append(csv_dict)

            tick = 0
            process_counter = 0
            for idx, csv_dict in enumerate(csv_arr):
                scan_size = 40
                end_idx = min(idx + scan_size, len(csv_arr))
                next_elements = csv_arr[idx:end_idx]
                next_process_filenames = [csv_dict["csv_file"] for csv_dict in next_elements]

                csv_file = csv_dict["csv_file"]
                ticks_to_wait = csv_dict["ticks_to_wait"]
       
                if self.should_finish(tick):
                    break
                if idx == self.__max_processes: # stop adding new processes
                    # tick until all processes finished running
                    while not self.scheduler.finalize():
                        self.scheduler.tick()
                        tick += 1
                    while self.scheduler.num_running_processes > 0:
                        print(self.scheduler.num_running_processes, "processes still running, tick:", tick)
                        self.scheduler.tick()
                        tick += 1
                        if self.should_finish(tick):
                            break
                    break
                process = Process(f"test_process_{idx}", idx, 0, os.path.join(self.__workload_dir, csv_file), self.__workloads_stats_dir)
                ok = self.scheduler.run(process, next_process_filenames)
                if not ok:
                    # tick until an edge-cluster is available
                    print("waiting for a cluster to become available, tick: ", tick)
                    while not self.scheduler.run(process, next_process_filenames):
                        self.scheduler.tick()
                        tick += 1
                        if self.should_finish(tick):
                            break
                    process_counter += 1
                else:
                    process_counter += 1
        
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
        print("Total process added: ", process_counter)
        print("Total scheduled processes: ", self.scheduler.total_processes)
        print("Total finished processes: ", self.scheduler.finished_processes)
        print("Scheduler Pool Size:", self.scheduler.pool_size)
        print("Total processing time [h]: ", self.scheduler.total_processing_time/60/60)
        print("Theoretical max processing time [h]: ", self.scheduler.total_gpus * tick / 60 / 60)
        print("Total time [h]: ", tick/60/60)
