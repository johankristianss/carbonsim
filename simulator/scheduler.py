import random
import csv
from stats import get_process_power_draw_stat
import os
import sys
sys.path.append('/home/johan/dev/github/johankristianss/carbonsim/simulator/genetic')
from genetic_pool import GeneticPool
from greedy_binpack_pool import GreedyBinpackPool
from delay_pool import DelayPool
import pandas as pd
import math

class Scheduler:
    def __init__(self, csv_filename='./scheduler.csv', workloads_stats_dir='./filtered_workloads_1s_stats', alg='random', power_threshold=150, co2_intensity_threshold=20):
        self.__edge_clusters_dict = {}
        self.tick_count = 0
        self.csv_filename = csv_filename
        self.csvfile = open(self.csv_filename, 'w', newline='')
        self.writer = csv.DictWriter(self.csvfile, fieldnames=['tick', 'cumulative_emission', 'cumulative_energy', 'utilization', 'total_gpu_cost'])
        self.writer.writeheader()
        self.workloads_stats_dir = workloads_stats_dir
        self.__c02_intensity_threshold = co2_intensity_threshold
        self.__alg = alg
        self.__genetic_pool = GeneticPool(workloads_stats_dir)
        self.__power_threshold = power_threshold
        self.__greedy_binpack_pool = GreedyBinpackPool(power_threshold=self.__power_threshold)
        self.__delay_pool = DelayPool()

        self.average_utilization_sum = 0
        self.__scheduled_processs = 0

    def add_edge_cluster(self, edge_cluster):
        self.__edge_clusters_dict[edge_cluster.name] = edge_cluster

    def background(self):
        print("=============================== run background ================================")

        if self.__alg == 'greedy_binpack':
            high_effect_processes, low_effect_processes, must_run_processes = self.__greedy_binpack_pool.select_processes()
            self.__greedy_binpack_pool.print_pool()

            print("------------------------------- greedy_binpack background -------------------------------")
            # print("high_effect_processes: ", len(high_effect_processes))
            # print("low_effect_processes: ", len(low_effect_processes))
            # print("must_run_processes: ", len(must_run_processes))
            #
            # print("processing must run processes")
            for process in must_run_processes:
                available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]

                if len(available_edge_clusters) > 0:
                    available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
                    selected_edge_cluster = available_edge_clusters[0]
                    print("selected edge cluster: ", selected_edge_cluster.name)
                    if selected_edge_cluster.run(process):
                        self.__greedy_binpack_pool.remove_process(process.name)

            for process in high_effect_processes:
                available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
                available_edge_clusters = [c for c in available_edge_clusters if c.carbon_intensity <= self.__c02_intensity_threshold]
                if len(available_edge_clusters) > 0:
                    available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
                    selected_edge_cluster = available_edge_clusters[0]
                    print("selected edge cluster: ", selected_edge_cluster.name)
                    if selected_edge_cluster.run(process):
                        self.__greedy_binpack_pool.remove_process(process.name)
                    else:
                        print("failed to run process, no available gpus")
                else:
                    print("no available edge clusters with low carbon intensity")

            print("processing low energy processes")
            for process in low_effect_processes:
                available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
                if len(available_edge_clusters) > 0:
                    available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
                    selected_edge_cluster = available_edge_clusters[0]

                    if selected_edge_cluster.run(process):
                        self.__greedy_binpack_pool.remove_process(process.name)

                # print pool size left after scheduling
            print("process not scheduled: ")
            self.__greedy_binpack_pool.print_pool()
        
        if self.__alg == 'delay':
            selected_processes, must_run_processes = self.__delay_pool.select_processes()

            self.__delay_pool.print_pool()

            # these processes must run now immediately
            for process in must_run_processes:
                available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]

                if len(available_edge_clusters) > 0:
                    available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
                    selected_edge_cluster = available_edge_clusters[0]
                    print("selected edge cluster: ", selected_edge_cluster.name)
                    if selected_edge_cluster.run(process):
                        self.__delay_pool.remove_process(process.name)

            for process in selected_processes:
                if process.power_draw_mean > self.__power_threshold:
                     clusters = self.__edge_clusters_dict
                     deadline_hours = math.ceil(process.deadline / 3600)
                     forecast_hours = range(0, deadline_hours)
         
                     forecast_data = {}
                     for cluster_name, cluster_obj in clusters.items():
                         hourly_values = []
                         for hour in forecast_hours:
                             future_seconds = hour * 3600
                             intensity = cluster_obj.carbon_intensity_future(future_seconds)
                             hourly_values.append(intensity)
                             forecast_data[cluster_name] = hourly_values
         
                     df = pd.DataFrame.from_dict(forecast_data, 
                             orient='index', 
                             columns=[f'H+{h}' for h in forecast_hours])
                     best_hour = df.min().idxmin()
                     # print("process: ", process.name, " best hour: ", best_hour, "power draw: ", process.power_draw_mean, "deadline: ", process.deadline)
     
                     if best_hour == 'H+0':
                         available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]                         
                         print("Available clusters:", len(available_edge_clusters))
                         if len(available_edge_clusters) < 1:
                             process.deadline = -1
                         else:
                            available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
                            selected_edge_cluster = available_edge_clusters[0]
                            print("selected edge cluster: ", selected_edge_cluster.name, "carbon intensity: ", selected_edge_cluster.carbon_intensity)
     
                            if selected_edge_cluster.run(process):
                                self.__delay_pool.remove_process(process.name)
                else:
                    available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
                    if len(available_edge_clusters) > 0:
                        available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
                        selected_edge_cluster = available_edge_clusters[0]
     
                        if selected_edge_cluster.run(process):
                            self.__delay_pool.remove_process(process.name)

        if self.__alg == 'genetic_timepool':
            edge_clusters = list(self.__edge_clusters_dict.values())

            schedule = self.__genetic_timepool.calc_schedule(edge_clusters)
            print("schedule: ", schedule)
            if schedule is None:
                print("genetic_timepool, schedule is None")
                return True # processes are queued in the pool, but don't need to run now 

            if len(schedule) == 0:
                print("genetic_timepool, schedule is empty")
                return True  # processes are queued in the pool, but don't need to run now

            for process_idx, edge_cluster_name in schedule.items():
                #print("process_idx: ", process_idx)
                print("edge_cluster_name: ", edge_cluster_name)
                if edge_cluster_name == 'wait':
                    continue
                edge_cluster = self.__edge_clusters_dict[edge_cluster_name]
                process = self.__genetic_timepool.get_process(process_idx)
                print("selected process: ", process.name)
                if edge_cluster.run(process):
                    self.__genetic_timepool.remove_process(process_idx)
            return True

    def run(self, process):
        if self.__alg == 'genetic_timepool':
            print("----------------------- genetic time pool scheduling -----------------------")
            if self.__genetic_timepool.add_process(process) == False:
                print("genetic_timepool.add_process failed")
                return False
         
            self.__scheduled_processs += 1
            return True

        elif self.__alg == 'greedy':
            print("----------------------- greedy scheduling -----------------------")
            ############# greedy scheduling ############
            available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
            if not available_edge_clusters:
                return False

            available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)

            selected_edge_cluster = available_edge_clusters[0]
            self.__scheduled_processs += 1
            return selected_edge_cluster.run(process)
        
        elif self.__alg == 'greedy_binpack':
            print("----------------------- greedy bin packing scheduling -----------------------")
            ############# greedy bin packing scheduling ############
            self.__greedy_binpack_pool.add_process(process)
            self.__scheduled_processs += 1
            return True
        
        elif self.__alg == 'delay':
            print("----------------------- delay scheduling -----------------------")
            ############# delay scheduling ############
            self.__delay_pool.add_process(process)
            self.__scheduled_processs += 1
            return True
        
        elif self.__alg == 'random':
            print("-----------------------  random scheduling -----------------------")
            ############# random scheduling ############
            available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
            if not available_edge_clusters:
                return False
            selected_edge_cluster = random.choice(available_edge_clusters)
            print("selected edge cluster: ", selected_edge_cluster.name)
            self.__scheduled_processs += 1
            return selected_edge_cluster.run(process)
        else :
            print("Invalid scheduling algorithm")
            return False

    def tick(self):
        for edge_cluster in self.__edge_clusters_dict.values():
             edge_cluster.tick()

        total_utilization = self.get_total_utilization()
        self.average_utilization_sum += total_utilization

        self.writer.writerow({
            'tick': self.tick_count,
            'cumulative_emission': self.cumulative_emission,
            'cumulative_energy': self.cumulative_energy,
            'utilization': self.get_total_utilization(),
            'total_gpu_cost': self.total_gpu_cost,
        })

        # run background processes every 60 seconds
        if self.tick_count % 60 == 0:
            print("runnning background, tick: ", self.tick_count)
            self.background()
        
        self.tick_count += 1
        
        self.__genetic_pool.tick()
        self.__greedy_binpack_pool.tick()
        self.__delay_pool.tick()

    @property
    def num_edge_clusters(self):
        return len(self.__edge_clusters_dict)

    @property
    def num_available_edge_clusters(self):
        return len([edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available])

    @property
    def num_running_processes(self):
        return sum([edge_cluster.num_running_processes for edge_cluster in self.__edge_clusters_dict.values()])

    def get_total_utilization(self):
        total_utilization = sum(edge_cluster.utilization for edge_cluster in self.__edge_clusters_dict.values())
        return total_utilization / self.num_edge_clusters if self.num_edge_clusters > 0 else 0

    @property
    def cumulative_energy(self):
        return sum([edge_cluster.cumulative_energy for edge_cluster in self.__edge_clusters_dict.values()])

    @property
    def cumulative_emission(self):
        return sum([edge_cluster.cumulative_emission for edge_cluster in self.__edge_clusters_dict.values()])
    
    @property
    def total_gpu_cost(self):
        return sum([edge_cluster.total_gpu_cost for edge_cluster in self.__edge_clusters_dict.values()])
    
    @property
    def total_processing_time(self):
        return sum([edge_cluster.total_processing_time for edge_cluster in self.__edge_clusters_dict.values()])
    
    @property
    def finished_processes(self):
        return sum([edge_cluster.finished_processes for edge_cluster in self.__edge_clusters_dict.values()])
  
    @property
    def scheduled_processes(self):
        return self.__scheduled_processs

    @property
    def total_processes(self):
        return sum([edge_cluster.total_processes for edge_cluster in self.__edge_clusters_dict.values()])

    @property
    def avg_utilization(self):
        return self.average_utilization_sum / self.tick_count

    @property
    def total_gpus(self):
        return sum([edge_cluster.gpus for edge_cluster in self.__edge_clusters_dict.values()])
