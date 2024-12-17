class DelayPool:
    def __init__(self):
        self.pool = []

    def add_process(self, process):
        self.pool.append(process)

    def is_empty(self):
        return len(self.pool) == 0

    def size(self):
        return len(self.pool)

    def remove_process(self, process_name):
        for process in self.pool:
            if process.name == process_name:
                self.pool.remove(process)

    def print_pool(self):
        print("------------------------------------ pool ------------------------------------")
        for process in self.pool:
            print("Process:", process.name, "Energy:", process.total_power_consumption, "Power", process.power_draw_mean, "Deadline:", process.deadline)

    def select_processes(self):
        selected_processes = []
        must_run_processes = []

        if len(self.pool) == 0:
            return selected_processes, must_run_processes
       
        for process in self.pool:
            if process.deadline <= 0:
                must_run_processes.append(process)
            else:
                selected_processes.append(process)

        selected_processes.sort(key=lambda x: x.power_draw_mean, reverse=True)
        must_run_processes.sort(key=lambda x: x.power_draw_mean, reverse=True)

        return selected_processes, must_run_processes

    def tick(self):
        for process in self.pool:
            process.decrease_deadline()
