import random
import csv
from stats import get_process_power_draw_stat
from pool import ProcessPool
from timepool import ProcessTimePool
import os
import sys
sys.path.append('/home/johan/dev/github/johankristianss/carbonsim/simulator/genetic')
from genetic_timepool import GeneticTimePool
from greedy_binpack_pool import GreedyBinpackPool

class Scheduler:
    def __init__(self, csv_filename='./scheduler.csv', workloads_stats_dir='./filtered_workloads_1s_stats', alg='random', timepool_power_threshold=150, pool_size=50, pool_alg="mean"):
        self.__edge_clusters_dict = {}
        self.tick_count = 0
        self.csv_filename = csv_filename
        self.csvfile = open(self.csv_filename, 'w', newline='')
        self.writer = csv.DictWriter(self.csvfile, fieldnames=['tick', 'cumulative_emission', 'cumulative_energy', 'utilization', 'total_gpu_cost'])
        self.writer.writeheader()
        self.workloads_stats_dir = workloads_stats_dir
        self.__alg = alg
        self.__pool = ProcessPool(pool_size=pool_size, pool_alg=pool_alg)
        self.__timepool = ProcessTimePool(power_threshold=timepool_power_threshold)
        self.__genetic_timepool = GeneticTimePool(workloads_stats_dir)
        self.__timepool_power_threshold = timepool_power_threshold
        self.__greedy_binpack_pool = GreedyBinpackPool(power_threshold=self.__timepool_power_threshold)
        self.__best_edge_cluster_first = False

        self.average_utilization_sum = 0
        self.__scheduled_processs = 0

    def add_edge_cluster(self, edge_cluster):
        self.__edge_clusters_dict[edge_cluster.name] = edge_cluster

    def finalize(self):
        if self.__alg == 'pool':
            self.empty_pool()
        if self.pool_size > 0:
            return False
        return True

    def empty_pool(self):
        while self.__pool.is_not_empty():
            available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
            if len(available_edge_clusters) == 0:
                return True
            selected_process = self.__pool.select_process()
            if selected_process is None:
                return False
            available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
            selected_edge_cluster = available_edge_clusters[0]
            selected_edge_cluster.run(selected_process)
    
    def background(self):
        print("=============================== run background ================================")
        if self.__alg == 'timepool':
            selected_processes = self.__timepool.select_processes()
            if selected_processes is None:
                print("timepool, selected_processes is None")
                return True # processes are queued in the pool, but don't need to run now 

            if len(selected_processes) == 0:
                print("timepool, selected_processes is empty")
                return True  # processes are queued in the pool, but don't need to run now
            
            for selected_process in selected_processes:
                available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
                # for edge_cluster in available_edge_clusters:
                #     print("edge_cluster: ", edge_cluster.name, "gpus: ", edge_cluster.available_gpus)

                available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
                if len(available_edge_clusters) == 0:
                    print("Timepool, available_edge_clusters is empty")
                    return True  # processes are queued in the pool, but don't need to run now
                selected_edge_cluster = available_edge_clusters[0]
                if selected_edge_cluster.run(selected_process):
                    self.__timepool.remove_process(selected_process.name)
            return True

        if self.__alg == 'greedy_binpack':
            high_effect_processes, low_effect_processes, must_run_processes = self.__greedy_binpack_pool.select_processes()
            self.__greedy_binpack_pool.print_pool()

            print("------------------------------- greedy_binpack background -------------------------------")
            print("high_effect_processes: ", len(high_effect_processes))
            print("low_effect_processes: ", len(low_effect_processes))
            print("must_run_processes: ", len(must_run_processes))

            print("processing must run processes")
            for process in must_run_processes:
                available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]

                if len(available_edge_clusters) > 0:
                    available_edge_clusters.sort(key=lambda edge_cluster: edge_cluster.carbon_intensity)
                    selected_edge_cluster = available_edge_clusters[0]
                    print("selected edge cluster: ", selected_edge_cluster.name)
                    if selected_edge_cluster.run(process):
                        self.__greedy_binpack_pool.remove_process(process.name)

            print("processing high energy processes")
            for process in high_effect_processes:
                available_edge_clusters = [edge_cluster for edge_cluster in self.__edge_clusters_dict.values() if edge_cluster.available]
                available_edge_clusters = [c for c in available_edge_clusters if c.carbon_intensity <= 20]
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
        print("=============================== run background DONE ================================")

    def run(self, process):
        print("=============================== run ================================")
        if self.__alg == 'timepool':
            print("----------------------- time pool scheduling -----------------------")
            if self.__timepool.add_process(process) == False:
                print("timepool.add_process failed")
                return False
            
            self.__scheduled_processs += 1
            return True

        if self.__alg == 'genetic_timepool':
            print("----------------------- genetic time pool scheduling -----------------------")
            if self.__genetic_timepool.add_process(process) == False:
                print("genetic_timepool.add_process failed")
                return False
         
            self.__scheduled_processs += 1
            return True

        if self.__alg == 'pool':
            print("----------------------- pool scheduling -----------------------")
            if self.__pool.is_full():
                self.empty_pool()
            if self.__pool.add_process(process) == False:
                print("pool.add_process failed")
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
        
        self.__genetic_timepool.tick()
        self.__timepool.tick()
        self.__greedy_binpack_pool.tick()

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
    def pool_size(self):
        return self.__pool.size()

    @property
    def avg_utilization(self):
        return self.average_utilization_sum / self.tick_count

    @property
    def total_gpus(self):
        return sum([edge_cluster.gpus for edge_cluster in self.__edge_clusters_dict.values()])
