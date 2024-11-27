from simulator import Simulator

max_processes = 10000
max_days = 2000 
cluster_utilization_threshold = 1.0
workload_dir = "./filtered_workloads_1s"
workloads_stats_dir = "./filtered_workloads_1s_stats"
cluster_config = "./edge-clusters-small.json"
log_dir = "./logs/80"
log_file = "log_2.csv"

def main():
    simulator = Simulator("pool",
                          max_processes,
                          max_days,
                          cluster_utilization_threshold, 
                          log_dir,
                          log_file,
                          workload_dir, 
                          workloads_stats_dir,
                          cluster_config, 
                          "./results/pool")
    simulator.start()

if __name__ == "__main__":
    main()
