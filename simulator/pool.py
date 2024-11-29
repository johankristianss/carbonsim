class ProcessPool:
    def __init__(self, pool_size=10):
        self.pool_size = pool_size
        self.pool = []

    def add_process(self, process):
        if len(self.pool) < self.pool_size:
            self.pool.append(process)
            return True
        else:
            return False

    def is_full(self):
        return len(self.pool) == self.pool_size

    def is_not_empty(self):
        return len(self.pool) != 0

    def size(self):
        return len(self.pool)

    def select_process(self):
        if len(self.pool) == 0:
            return None
        
        #self.pool.sort(key=lambda x: x.total_power_consumption)
        #self.pool.sort(key=lambda x: x.power_draw_median)
        self.pool.sort(key=lambda x: x.power_draw_mean)


        print("--------------- pool ---------------")
        for process in self.pool:
            print(process.name, process.total_power_consumption, process.power_draw_mean) 


        #selected_process = random.choice(self.pool)
        selected_process = self.pool[len(self.pool) - 1]
        print("Selected process: ", selected_process.name)
        #print("before remove: ", len(self.pool))
        self.pool.remove(selected_process)
        #print("after remove: ", len(self.pool))
        return selected_process
