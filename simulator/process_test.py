import unittest
from process import *

class TestProcess(unittest.TestCase):
    def test_process(self):
        process = Process("test_process", 0, "/scratch/cognit/filtered_workloads/0.csv")
        process.carbon_intensity = 15
        
        for _ in range(90000):
            if process.tick():
                break
       
        print("carbon emission [g]: ", process.cumulative_emission)
        print("energy [kWh]: ", process.cumulative_energy)
        print("timesteps: ", process.timestep)

if __name__ == '__main__':
    unittest.main()
