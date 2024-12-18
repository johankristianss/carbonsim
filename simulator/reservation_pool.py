import math
import pandas as pd
from collections import deque

import math
import pandas as pd

class Reservation:
    def __init__(self, edgeclusters):
        self.edgeclusters = edgeclusters
        self.reservation = {}
        self.t = 0
        self.processes = []

    def set_edgeclusters(self, edgeclusters):
        self.edgeclusters = edgeclusters

        for edgecluster in edgeclusters:
            self.reservation[edgecluster] = []

    def available_gpus_at_tick(self, cluster_name, tick):
        # Calculate available GPUs at a specific tick
        total_gpus = self.edgeclusters[cluster_name].gpus
        reserved_gpus = 0

        # Loop through all processes assigned to this cluster
        for processes in self.reservation[cluster_name]:
            for process in processes:
                # Check if the process overlaps with the given tick
                process_end_time = process.planned_start_time + process.total_length_seconds
                if process.planned_start_time <= tick < process_end_time:
                    reserved_gpus += 1

        # Return the available GPUs
        return total_gpus - reserved_gpus

    def calculate_benefit(self, process, cluster_name, tick):
        # Calculate the benefit of running a process within a 60-second timeslot
        start_time = tick
        end_time = min(tick + process.total_length_seconds, tick + 60)
        co2_benefit = 0

        for t in range(start_time, end_time):
            co2_benefit += self.edgeclusters[cluster_name].carbon_intensity_future(t - self.t)

        return co2_benefit / (end_time - start_time)

    def add_process(self, process):
        # Implement bin-packing in the time dimension with 60-second resolution
        deadline_ticks = process.deadline
        forecast_ticks = range(self.t, self.t + deadline_ticks, 60)
        #
        # forecast_data = {}
        # for cluster_name, cluster_obj in self.edgeclusters.items():
        #     tick_values = []
        #     for tick in forecast_ticks:
        #         intensity = cluster_obj.carbon_intensity_future(tick - self.tick)
        #         tick_values.append(intensity)
        #     forecast_data[cluster_name] = tick_value
        #
        # df = pd.DataFrame.from_dict(forecast_data, orient='index', columns=[t for t in forecast_ticks])
        #
        # print(df)

        # Calculate benefit and sort by benefit
        possible_assignments = []
        for tick in forecast_ticks:
            for cluster_name in self.edgeclusters.keys():
                available_gpus = self.available_gpus_at_tick(cluster_name, tick)
                if available_gpus > 0:
                    benefit = self.calculate_benefit(process, cluster_name, tick)
                    #print(f"Benefit of running {process.name} on {cluster_name} cluster at tick {tick}: {benefit: .2f}")
                    possible_assignments.append((benefit, cluster_name, tick))

        # Sort assignments by highest benefit and spread start times
        possible_assignments.sort(reverse=False, key=lambda x: x[0])
        assigned = False

        for _, cluster_name, tick in possible_assignments:
            available_gpus = self.available_gpus_at_tick(cluster_name, tick)
            if available_gpus > 0:
                if cluster_name not in self.reservation:
                    self.reservation[cluster_name] = [] 
                if tick not in self.reservation[cluster_name]:
                    self.reservation[cluster_name] = []

                self.reservation[cluster_name].append(process)
                process.planned_start_time = tick
                process.planned_cluster_name = cluster_name

                estimated_co2_at_start_time = self.edgeclusters[cluster_name].carbon_intensity_future(tick - self.t)

                print(f"Assigned {process.name} to {cluster_name} cluster at tick {tick} with benefit{_: .2f}.", f"Estimated CO2 intensity at start time:{estimated_co2_at_start_time: .2f} gCO2/kWh.")
                assigned = True
                break

        if not assigned:
            # If no valid time slot is found
            raise RuntimeError(f"Unable to schedule process {process.name} within its deadline using bin-packing.")

    def select_processes(self):
        processes = []

        for clustername in self.reservation:
            for p in self.reservation[clustername]:
                if p.planned_start_time == self.tick:
                    processes.append(p)

        return processes

    def remove_process(self, processname):
        for clustername in self.reservation:
            for p in self.reservation[clustername]:
                print(p)
                if p.name == processname:
                    # remove p from the list
                    self.reservation[clustername].remove(p)


    def tick(self):
        self.t += 1
        for cluster_name in self.reservation:
            for process in self.reservation[cluster_name]:
                process.decrease_deadline()
