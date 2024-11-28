import unittest
from process import *

class TestProcess(unittest.TestCase):
    def test_process(self):
        workloads_stats_dir = "../filtered_workloads_1s_stat"
        process = Process("test_process", 0, 0, 60, "../filtered_workloads/0.csv", workloads_stats_dir)
        process.carbon_intensity = 15
        
        for _ in range(90000):
            if process.tick():
                break
       
        print("carbon emission [g]: ", process.cumulative_emission)
        print("energy [kWh]: ", process.cumulative_energy)
        print("timesteps: ", process.timestep)

if __name__ == '__main__':
    unittest.main()
