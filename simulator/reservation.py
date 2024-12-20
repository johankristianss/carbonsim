import math
import pandas as pd
from collections import deque
import os
import math
import pandas as pd
import json

class Reservation:
    def __init__(self):
        self.edgeclusters = {} 
        self.reservation = {}
        self.t = 0
        self.processes = []

    def set_edgeclusters(self, edgeclusters):
        self.edgeclusters = edgeclusters

        for edgecluster in edgeclusters:
            self.reservation[edgecluster] = []

    def planned_exectime(self):
        total = 0
        for clustername in self.reservation:
            for p in self.reservation[clustername]:
                total += p.total_length_seconds
        return total


    def available_gpus_at_tick(self, cluster_name, start, end, debug=True):
        # Calculate available GPUs in the specified time range [start, end) for the given cluster
        total_gpus = self.edgeclusters[cluster_name].gpus
        reserved_gpus = 0
    
        for process in self.reservation[cluster_name]:
            process_start_time = process.planned_start_time
            process_end_time = process.planned_start_time + process.total_length_seconds
    
            # Check if [start, end) overlaps with [process_start_time, process_end_time)
            if not (end <= process_start_time or start >= process_end_time):
                reserved_gpus += 1
    
        available_gpus = total_gpus - reserved_gpus
    
        return available_gpus

    def dump_json(self, filename):
        j = []
        for clustername in self.reservation:
            for p in self.reservation[clustername]:
                j.append({"name": p.name, "tick": p.planned_start_time, "length": p.total_length_seconds, "cluster": p.planned_cluster_name})

        with open(filename, 'w') as f:
            f.write(json.dumps(j))

    def calculate_benefit(self, process, cluster_name, tick):
        start_time = tick
        end_time = min(tick + process.total_length_seconds, tick + 60)
        co2_benefit = 0

        for t in range(start_time, end_time):
            co2_benefit += self.edgeclusters[cluster_name].carbon_intensity_future(t - self.t)

        return co2_benefit * process.total_length_seconds * process.power_draw_mean

    def add_process(self, process):
        print("Adding process:", process.name, "Deadline:", process.deadline, "Length:", process.total_length_seconds)
        deadline_ticks = process.deadline
        forecast_ticks = range(self.t, self.t + deadline_ticks, 60)

        # Calculate benefit and sort by benefit
        possible_assignments = []
        for tick in forecast_ticks:
            for cluster_name in self.edgeclusters.keys():
                available_gpus = self.available_gpus_at_tick(cluster_name, tick, tick+process.total_length_seconds)
                if available_gpus > 0:
                    benefit = self.calculate_benefit(process, cluster_name, tick)
                    #print(f"Benefit of running {process.name} on {cluster_name} cluster at tick {tick}: {benefit: .2f}")
                    possible_assignments.append((benefit, cluster_name, tick))

        # Sort assignments by highest benefit and spread start times
        possible_assignments.sort(reverse=False, key=lambda x: x[0])
        assigned = False

        #for benefit, cluster_name, tick in possible_assignments:
        #       print(benefit, cluster_name, tick)

        for benefit, cluster_name, tick in possible_assignments:
            available_gpus = self.available_gpus_at_tick(cluster_name, tick, tick+process.total_length_seconds)
            #print("Cluster:", cluster_name, "Tick:", tick, "Available GPUs:", available_gpus, "Benefit:", benefit)
            if available_gpus > 0:
                process.planned_start_time = tick
                process.planned_cluster_name = cluster_name
                #print("Setting planned start time to", process.planned_start_time)
                self.reservation[cluster_name].append(process)
                #print("Cluster now has", len(self.reservation[cluster_name]), "reserved processes.")

                estimated_co2_at_start_time = self.edgeclusters[cluster_name].carbon_intensity_future(tick - self.t)

                print(f"Assigned {process.name} to {cluster_name} cluster at tick {tick} with benefit{benefit: .2f}.", f"Estimated CO2 intensity at start time:{estimated_co2_at_start_time: .2f} gCO2/kWh.")
                assigned = True
                break
            else:
                print("No available GPUs at tick", tick, "for", process.name, "on", cluster_name)

        if not assigned:
            print(f"Unable to schedule process {process.name} within its deadline using bin-packing.")
            return False

        return True

    def select_processes(self, range=0):
        return self.select_processes_at_tick(self.t, range)

    def select_processes_at_tick(self, tick, range=0):
        processes = []
        for clustername in self.reservation:
            for p in self.reservation[clustername]:
                # plus minus the range
                planned_start_time = p.planned_start_time
                if planned_start_time - range <= tick <= planned_start_time + range:
                    processes.append(p)

        return processes

    def remove_process(self, processname):
        for clustername in self.reservation:
            for p in self.reservation[clustername]:
                if p.name == processname:
                    # remove p from the list
                    self.reservation[clustername].remove(p)

    def print(self):
        for clustername in self.reservation:
            print(f"Cluster: {clustername}")
            for p in self.reservation[clustername]:
                print(f"Tick: {self.t} Process: {p.name}, Start: {p.planned_start_time}, End: {p.planned_start_time + p.total_length_seconds}, Cluster: {p.planned_cluster_name}")
                #print(f"Tick {self.t}: Process: {p.name}, Start: {p.planned_start_time}, End: {p.planned_start_time + p.total_length_seconds}, Cluster: {p.planned_cluster_name})

    def tick(self):
        self.t += 1
        for cluster_name in self.reservation:
            for process in self.reservation[cluster_name]:
                process.decrease_deadline()

    def set_tick(self, tick):
        self.t = tick

    def increase_tick(self, tick):
        self.t += tick
