import math
import pandas as pd
from collections import deque
import os
import math
import pandas as pd
import json
from time import time 
from bisect import bisect_left
    
def available_gpus_at_tick(start, end, processesStatus):
    reserved_gpus = 0

    processStartTimes = processesStatus["start_times"]
    processEndTimes = processesStatus["end_times"]
    total_gpus = processesStatus["total_gpus"]

    for i in range(len(processStartTimes)):
        process_start_time = processStartTimes[i]
        process_end_time = processEndTimes[i]

        if not (end <= process_start_time or start >= process_end_time):
            reserved_gpus += 1
            if total_gpus - reserved_gpus <= 0:
                #print(f"No GPU available between [{start}, {end})")
                return False
    return True

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

    def dump_json(self, filename):
        j = []
        for clustername in self.reservation:
            for p in self.reservation[clustername]:
                j.append({"name": p.name, "tick": p.planned_start_time, "length": p.total_length_seconds, "cluster": p.planned_cluster_name})

        with open(filename, 'w') as f:
            f.write(json.dumps(j))

    def calculate_benefit(self, process, cluster_name, tick):
        #start_time = tick
        #end_time = tick + process.total_length_seconds
        #co2_benefit = 0

        #for t in range(start_time, end_time):
        #    co2_benefit += self.edgeclusters[cluster_name].carbon_intensity_future(t - self.t)

        # return co2_benefit * process.total_length_seconds * process.power_draw_mean
        return self.edgeclusters[cluster_name].carbon_intensity_future(tick) * process.power_draw_mean

    def add_process(self, process):
        # if process.name == "p_205":
        #     debug = True
        #     self.dump_json("layout.json")
        #     os._exit(1)
        # else:
        #     debug = False

        print("Adding process:", process.name, "Deadline:", process.deadline, "Length:", process.total_length_seconds)
        deadline_ticks = process.deadline
        forecast_ticks = range(self.t, self.t + deadline_ticks, 1)

        print("Forecast ticks:", forecast_ticks)

        possible_assignments = []
        processStatus = {}
        for cluster_name in self.edgeclusters.keys():
            processStatus[cluster_name] = {}
            processes = self.reservation[cluster_name]

            processStatus[cluster_name]["start_times"] = [p.planned_start_time for p in processes]
            processStatus[cluster_name]["end_times"] = [p.planned_start_time + p.total_length_seconds for p in processes]
            processStatus[cluster_name]["total_gpus"] = self.edgeclusters[cluster_name].gpus

        for tick in forecast_ticks:
            for cluster_name in self.edgeclusters.keys():
                available_gpus = available_gpus_at_tick(tick, tick + process.total_length_seconds, processStatus[cluster_name])

                if available_gpus:
                    benefit = self.calculate_benefit(process, cluster_name, tick)
                    possible_assignments.append((benefit, cluster_name, tick))

        # Sort assignments by tick (ascending) and benefit (descending)
        #possible_assignments.sort(key=lambda x: (x[2], -x[0]))
        #print("Possible assignments:", possible_assignments)
        # Sort assignments by highest benefit and spread start times

        possible_assignments.sort(reverse=True, key=lambda x: x[0])

        # for i in range(len(possible_assignments)):
        #     print(f"Benefit: {possible_assignments[i][0]: .2f}, Cluster: {possible_assignments[i][1]}, Tick: {possible_assignments[i][2]}")

        if len(possible_assignments) == 0:
            print(f"Unable to schedule process {process.name} within its deadline using bin-packing.")
            return False

        # Pick the best assignment
        best_benefit, best_cluster_name, best_tick = possible_assignments[0]
        process.planned_start_time = best_tick
        process.planned_cluster_name = best_cluster_name
        self.reservation[best_cluster_name].append(process)
    
        estimated_co2_at_start_time = self.edgeclusters[process.planned_cluster_name].carbon_intensity_future(process.planned_start_time)
    
        print(f"{self.t}: Assigned {process.name} with length {process.total_length_seconds}s to {best_cluster_name} cluster at tick {best_tick} with benefit{best_benefit: .2f}.", f"Estimated CO2 intensity at start time:{estimated_co2_at_start_time: .2f} gCO2/kWh.")

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
