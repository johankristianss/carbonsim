from simulator import Simulator

max_processes = 10000
max_days = 0.1
cluster_utilization_threshold = 1.0
random_wait = True 
wait_time = 20  # seconds
rate = 1.0  # Rate parameter (lambda) for the exponential distribution
workload_dir = "./filtered_workloads_1s"
workloads_stats_dir = "./filtered_workloads_1s_stats"
#workload_dir = "/.filtered_workloads"
cluster_config = "./edge-clusters-test.json"

def main():
    simulator = Simulator("lookahead",
                          max_processes,
                          max_days,
                          cluster_utilization_threshold, 
                          random_wait,
                          wait_time,
                          rate, 
                          workload_dir,
                          workloads_stats_dir,
                          cluster_config, 
                          "./results/dev")
    simulator.start()

if __name__ == "__main__":
    main()
