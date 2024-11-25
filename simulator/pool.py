import random

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

    def select_process(self):
        if len(self.pool) == self.pool_size:
            #energy = 0.0
            #energy = process.total_length_seconds * process.power_draw_median
            # sort processes by energy

            # make a copy of the pool
            pool_copy = self.pool.copy()
            pool_copy.sort(key=lambda x: x.total_length_seconds * x.power_draw_median)

            # print process name and energy
            print("--------------- pool ---------------")
            for process in self.pool:
                 print(process.name, process.total_length_seconds * process.power_draw_median)

            #selected_process = random.choice(self.pool)
            selected_process = pool_copy[len(pool_copy) - 1]
            print("Selected process: ", selected_process.name)
            print("bwfore remove: ", len(self.pool))
            self.pool.remove(selected_process)
            print("after remove: ", len(self.pool))
            return selected_process
        else:
            return None 
