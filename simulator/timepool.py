class ProcessTimePool:
    def __init__(self, power_threshold=100):
        self.power_threshold = power_threshold
        self.pool = []

    def add_process(self, process):
        self.pool.append(process)
        return True

    def is_empty(self):
        return len(self.pool) == 0

    def size(self):
        return len(self.pool)

    def remove_process(self, process_name):
        for process in self.pool:
            if process.name == process_name:
                self.pool.remove(process)
                return True
        return

    def select_processes(self):
        if len(self.pool) == 0:
            return None
       
        # find all process with power drawn more than threshold
        selected_processes = []
        for process in self.pool:
            if process.power_draw_mean > self.power_threshold:
                selected_processes.append(process)

        # find all process where deadline is less or equal to 0
        for process in self.pool:
            process.decrease_deadline()
            if process.deadline <= 0:
                selected_processes.append(process)
       
        if len(selected_processes) == 0:
            return None

        # sort in reverse order
        selected_processes.sort(key=lambda x: x.power_draw_mean, reverse=True)

        # print("time pool ----------- ")
        # for process in selected_processes:
        #     print(process.name, process.power_draw_mean, process.deadline)

        return selected_processes
