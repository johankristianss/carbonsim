import unittest
from edge_cluster import *
from process import *

class TestEdgeCluster(unittest.TestCase):
    def test_run_process(self):
        edge_cluster = EdgeCluster("test_cluster", 1, 1, "../carbon_1s/SE-SE1.csv", 0.001101, 1.0)
        process = Process("test_process", 0, 0, "../filtered_workloads/0.csv")
        edge_cluster.run(process)

        for _ in range(90000):
            if edge_cluster.tick():
                break 
        
        print("edge_cluster: carbon emission [g]: ", edge_cluster.cumulative_emission)
        print("edge_cluster: energy [kWh]: ", edge_cluster.cumulative_energy)
        print("edge_cluster: timesteps: ", edge_cluster.timestep)
        
        print("process: carbon emission [g]: ", process.cumulative_emission)
        print("process: energy [kWh]: ", process.cumulative_energy)
        print("process: timesteps: ", process.timestep)

if __name__ == '__main__':
    unittest.main()
