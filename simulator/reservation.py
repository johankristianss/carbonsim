import os
import json

def available_gpus_at_tick(start, end, processesStatus):
    reserved_gpus = 0
    processStartTimes = processesStatus["start_times"]
    processEndTimes   = processesStatus["end_times"]
    total_gpus        = processesStatus["total_gpus"]

    for i in range(len(processStartTimes)):
        p_start = processStartTimes[i]
        p_end   = processEndTimes[i]

        if not (end < p_start or start > p_end):
            reserved_gpus += 1
            if reserved_gpus >= total_gpus:
                return False

    return True

def find_processes_at_tick(tick, processesStatus):
    """
    Return a list of process references that are active at time `tick`.
    We'll assume inclusive intervals: a process is active if
    start_time <= tick <= end_time.
    """
    active_processes = []

    # Retrieve the parallel lists
    starts    = processesStatus["start_times"]
    ends      = processesStatus["end_times"]
    processes = processesStatus["processes"]  # references to the actual Process objects

    for i in range(len(processes)):
        if starts[i] <= tick <= ends[i]:
            active_processes.append(processes[i])

    return active_processes

def find_processes_about_to_start(tick, processesStatus, threshold=10):
    """
    Return a list of process references that are scheduled to start 
    between [tick, tick + threshold] (inclusive by default).
    """
    starts    = processesStatus["start_times"]
    ends      = processesStatus["end_times"]     # Not used here, but available
    processes = processesStatus["processes"]     # references to the actual Process objects

    about_to_start = []
    for i, p_start in enumerate(starts):
        if tick <= p_start <= tick + threshold:
            about_to_start.append(processes[i])

    return about_to_start

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

    def calculate_benefit_s(self, process, cluster_name, tick):
        start_time = tick
        end_time = tick + process.total_length_seconds
        co2_benefit = 0

        cluster = self.edgeclusters[cluster_name]

        #print(f"Calculating benefit for process {process.name} on cluster {cluster_name} at tick {tick} with length {process.total_length_seconds}s")

        for t in range(start_time, end_time):
            co2_benefit += cluster.carbon_intensity_future(t - self.t)

        return co2_benefit
    
    def calculate_energy(self, process, cluster_name, tick):
        start_time = tick
        end_time = tick + process.total_length_seconds
    
        # Switch to 5-minute resolution
        start_interval = start_time // 300  # 300 seconds = 5 minutes
        end_interval = (end_time + 299) // 300  # Ensure full coverage
        co2 = 0
    
        cluster = self.edgeclusters[cluster_name]
    
        for interval in range(start_interval, end_interval):
            t = interval * 300  # Convert interval to seconds
            co2 += cluster.carbon_intensity_future(t - self.t) * 300
    
        return co2 * process.power_draw_mean
    
    def find_smallest_energy(self, possible_assignments):
        if not possible_assignments:
            return None 

        smallest_benefit_assignment = min(possible_assignments, key=lambda x: x[0])
        return smallest_benefit_assignment
    
    def simulate_rescheduling(self, process):
        print("Simulating Adding process:", process.name, "Deadline:", process.deadline, "Length:", process.total_length_seconds)
        deadline_ticks = process.deadline
        forecast_ticks = range(self.t, self.t + deadline_ticks, 1)
       
        margin = 5

        possible_assignments = []
        processStatus = {}
        for cluster_name in self.edgeclusters.keys():
            processStatus[cluster_name] = {}
            processes = self.reservation[cluster_name]

            processStatus[cluster_name]["start_times"] = [p.planned_start_time for p in processes]
            processStatus[cluster_name]["end_times"] = [p.planned_start_time + p.total_length_seconds + margin for p in processes]
            processStatus[cluster_name]["total_gpus"] = self.edgeclusters[cluster_name].gpus
            processStatus[cluster_name]["processes"] = processes

        for tick in forecast_ticks:
            for cluster_name in self.edgeclusters.keys():
                available_gpus = available_gpus_at_tick(tick, tick + process.total_length_seconds + margin, processStatus[cluster_name])

                if available_gpus:
                    energy = self.calculate_energy(process, cluster_name, tick)
                    possible_assignments.append((energy, cluster_name, tick))

        best_assignment = self.find_smallest_energy(possible_assignments)

        if best_assignment is None:
            print(f"Unable to schedule process {process.name} within its deadline using bin-packing.")
            return -1.0

        energy, _, _ = best_assignment

        return energy


    def add_process(self, process):
        print("Adding process:", process.name, "Deadline:", process.deadline, "Length:", process.total_length_seconds)
        deadline_ticks = process.deadline
        forecast_ticks = range(self.t, self.t + deadline_ticks, 1)
       
        margin = 5

        possible_assignments = []
        preemptive_assignments = []
        processStatus = {}
        for cluster_name in self.edgeclusters.keys():
            processStatus[cluster_name] = {}
            processes = self.reservation[cluster_name]

            processStatus[cluster_name]["start_times"] = [p.planned_start_time for p in processes]
            processStatus[cluster_name]["end_times"] = [p.planned_start_time + p.total_length_seconds + margin for p in processes]
            processStatus[cluster_name]["total_gpus"] = self.edgeclusters[cluster_name].gpus
            processStatus[cluster_name]["processes"] = processes

        for tick in forecast_ticks:
            for cluster_name in self.edgeclusters.keys():
                available_gpus = available_gpus_at_tick(tick, tick + process.total_length_seconds + margin, processStatus[cluster_name])

                if available_gpus:
                    energy = self.calculate_energy(process, cluster_name, tick)
                    possible_assignments.append((energy, cluster_name, tick))
                else:
                    energy = self.calculate_energy(process, cluster_name, tick)
                    preemptive_assignments.append((energy, cluster_name, tick))

        # for i in range(len(possible_assignments)):
        #     print(f"Benefit: {possible_assignments[i][0]: .2f}, Cluster: {possible_assignments[i][1]}, Tick: {possible_assignments[i][2]}")
        #

        if len(possible_assignments) == 0 and len(preemptive_assignments) == 0:
            print(f"Unable to schedule process {process.name} within its deadline, all clusters are busy.")
            return False
        
        if len(preemptive_assignments) > 0:
            best_possible_assignment = self.find_smallest_energy(possible_assignments)
            best_preemptive_assignment = self.find_smallest_energy(preemptive_assignments)

            if best_preemptive_assignment is not None and best_possible_assignment is not None:
                best_possible_energy, _, _ = best_possible_assignment
                best_preemptive_energy, best_preemptive_cluster_name, best_preemptive_best_tick = best_preemptive_assignment

                if best_preemptive_energy < best_possible_energy:
                    active_procsses = find_processes_at_tick(best_preemptive_best_tick, processStatus[best_preemptive_cluster_name])
     
                    best_energy_gain = 0.0
                    best_active_process = None
                    for p in active_procsses:
                        energy = self.simulate_rescheduling(p)
                        if energy > 0.0:
                            gain = best_preemptive_energy - energy
                            if gain > best_energy_gain:
                                best_energy_gain = gain
                                best_active_process = p
    
                    if best_active_process is not None:
                        print("Best active process to reschedule", best_active_process.name, best_energy_gain)
                        process.planned_start_time = best_preemptive_best_tick
                        process.planned_cluster_name = best_preemptive_cluster_name
                        process.predicted_energy = best_preemptive_energy
                        self.reservation[best_preemptive_cluster_name].append(process)
                        estimated_co2_at_start_time = self.edgeclusters[process.planned_cluster_name].carbon_intensity_future(process.planned_start_time)
                        print(f"{self.t}: Assigned {process.name} with length {process.total_length_seconds}s to {best_preemptive_cluster_name} cluster at tick {best_preemptive_best_tick} with energy{best_preemptive_energy: .2f}.", f"Estimated CO2 intensity at start time:{estimated_co2_at_start_time: .2f} gCO2/kWh.")

                        # remove the active process from the reservation
                        self.remove_process(best_active_process.name)
                        #os._exit(1)
                        return self.add_process(best_active_process)

     


                    # lowest_energy_active_process = possibe_rescheduling[0]
                    #
                    #
                    # # best possible is energy is where we can schedule the process without re-scheduling any active process
                    # # best preemptive energy is where we can schedule the process by re-scheduling an active process
                    # # Rule 1: We ony want to re-schedule an active process if the energy of the preemptive assignment is lower than the best possible assignment
                    # # Rule 2: We only want to re-schedule an active process if the energy of the active process is higher than the energy of the preemptive assignment
                    #
                    #
                    #
                    # if best_possible_assignment is not None: 
                    #     best_possible_energy, best_possible_cluster_name, best_possible_tick = best_possible_assignment
                    #
                    #     if best_preemptive_energy < best_possible_energy and best_preemptive_energy > lowest_energy_active_process.predicted_energy and lowest_energy_active_process.power_draw_mean < process.power_draw_mean:
                    #
                    #         print("Preemptive assignment is possible")
                    #         print("This process: Best possible assignment", process.name, best_possible_cluster_name, best_possible_tick, best_possible_energy, process.power_draw_mean)
                    #         print("This process: Lowest premptive energy", process.name, best_preemptive_cluster_name, best_preemptive_best_tick, best_preemptive_energy, process.power_draw_mean)
                    #         print("Active process:", lowest_energy_active_process.name, lowest_energy_active_process.planned_cluster_name, lowest_energy_active_process.planned_start_time, lowest_energy_active_process.predicted_energy, lowest_energy_active_process.power_draw_mean)
                    #
                    #         process.planned_start_time = best_preemptive_best_tick
                    #         process.planned_cluster_name = best_preemptive_cluster_name
                    #         process.predicted_energy = best_preemptive_energy
                    #         self.reservation[best_preemptive_cluster_name].append(process)
                    #         estimated_co2_at_start_time = self.edgeclusters[process.planned_cluster_name].carbon_intensity_future(process.planned_start_time)
                    #         print(f"{self.t}: Assigned {process.name} with length {process.total_length_seconds}s to {best_preemptive_cluster_name} cluster at tick {best_preemptive_best_tick} with energy{best_preemptive_energy: .2f}.", f"Estimated CO2 intensity at start time:{estimated_co2_at_start_time: .2f} gCO2/kWh.")
                    #
                    #         # remove the active process from the reservation
                    #         self.remove_process(lowest_energy_active_process.name)
                    #         #os._exit(1)
                    #         return self.add_process(lowest_energy_active_process)

                # find the process with the lowest predicted energy

                #for p in active_procsses:
                #    print(f"Active process {p.name} at tick {best_tick}, predicted energy {p.predicted_energy} on cluster {p.planned_cluster_name}")
                #os._exit(1)

        best_assignment = self.find_smallest_energy(possible_assignments)

        if best_assignment is None:
            print(f"Unable to schedule process {process.name} within its deadline using bin-packing.")
            return False

        energy, best_cluster_name, best_tick = best_assignment

        process.planned_start_time = best_tick
        process.planned_cluster_name = best_cluster_name
        process.predicted_energy = energy
        self.reservation[best_cluster_name].append(process)
        estimated_co2_at_start_time = self.edgeclusters[process.planned_cluster_name].carbon_intensity_future(process.planned_start_time)
    
        print(f"{self.t}: Assigned {process.name} with length {process.total_length_seconds}s to {best_cluster_name} cluster at tick {best_tick} with energy{energy: .2f}.", f"Estimated CO2 intensity at start time:{estimated_co2_at_start_time: .2f} gCO2/kWh.")

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

    def tick(self):
        self.t += 1
        for cluster_name in self.reservation:
            for process in self.reservation[cluster_name]:
                process.decrease_deadline()

    def set_tick(self, tick):
        self.t = tick

    def increase_tick(self, tick):
        self.t += tick

    def planned_processes(self):
        total = 0
        for clustername in self.reservation:
            total += len(self.reservation[clustername])
        return total
