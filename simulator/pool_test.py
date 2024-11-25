import unittest
from pool import ProcessPool 
from process import Process 

class TestProcessPool(unittest.TestCase):
    def test_pool(self):
        pool_size = 2 
        pool = ProcessPool(pool_size=pool_size)
        workloads_stats_dir = "../filtered_workloads_1s_stats"

        process = Process("test_process_1", 0, 0, "../filtered_workloads/0.csv", workloads_stats_dir)
        res = pool.add_process(process)
        self.assertEqual(res, True)
        
        process = Process("test_process_2", 1, 0, "../filtered_workloads/1.csv", workloads_stats_dir)
        res = pool.add_process(process)
        self.assertEqual(res, True)
        
        process = Process("test_process_3", 2, 0, "../filtered_workloads/2.csv", workloads_stats_dir)
        res = pool.add_process(process)
        self.assertEqual(res, False)  # False, pool is full

        selected_process = pool.select_process()
        print(selected_process.name)
        
        process = Process("test_process_3", 2, 0, "../filtered_workloads/2.csv", workloads_stats_dir)
        res = pool.add_process(process)
        self.assertEqual(res, True)
        
        selected_process = pool.select_process()
        print(selected_process.name)

if __name__ == "__main__":
    unittest.main()

