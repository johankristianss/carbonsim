from genetic_scheduler import GeneticScheduler

class GeneticPool:
    def __init__(self, workload_stats_dir, power_threshold=100):
        self.power_threshold = power_threshold
        self.pool = {}
        self.gs = GeneticScheduler(workload_stats_dir)

    def add_process(self, process):
        print("Adding process: ", process.name)
        self.pool[process.idx] = process
        return True

    def get_process(self, process_idx):
        return self.pool[process_idx]

    def is_empty(self):
        return len(self.pool) == 0

    def size(self):
        return len(self.pool)

    def remove_process(self, process_idx):
        if process_idx in self.pool:
            del self.pool[process_idx]
        return

    def calc_schedule(self, edge_clusters):
        if len(self.pool) == 0:
            return None
    
        self.gs.set_clusters_status(edge_clusters)
    
        # convert the pool to a list of process list
        process_list = list(self.pool.values())

        self.gs.set_workloads_processes(process_list)

        best_schedule, _ = self.gs.run()
        return best_schedule

    def tick(self):
        for process in self.pool.values():
            process.decrease_deadline()
