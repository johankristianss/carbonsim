from simulator import Simulator

max_processes = 10000
max_days = 20 
cluster_utilization_threshold = 1.0
random_wait = True 
wait_time = 30  # seconds
rate = 1.0  # Rate parameter (lambda) for the exponential distribution
workload_dir = "./filtered_workloads_1s"
workloads_stats_dir = "./filtered_workloads_1s_stats"
#workload_dir = "/.filtered_workloads"
cluster_config = "./edge-clusters-small.json"

def main():
    simulator = Simulator("bigjobfirst",
                          max_processes,
                          max_days,
                          cluster_utilization_threshold, 
                          random_wait,
                          wait_time,
                          rate, 
                          workload_dir, 
                          workloads_stats_dir,
                          cluster_config, 
                          "./results/bigjobfirst")
    simulator.start()

if __name__ == "__main__":
    main()
