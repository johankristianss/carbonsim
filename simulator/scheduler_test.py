import unittest
from edge_cluster import *
from process import *
from scheduler import *

class TestScheduler(unittest.TestCase):
    def test_scheduler(self):
        scheduler = Scheduler()
        edge_cluster = EdgeCluster("umea", 1, 1, "../carbon_1s/SE-SE1.csv", 0.001101, 1.0)
        scheduler.add_edge_cluster(edge_cluster)
        edge_cluster = EdgeCluster("stockholm", 1, 1, "../carbon_1s/SE-SE3.csv", 0.001101, 1.0)
        scheduler.add_edge_cluster(edge_cluster)
        edge_cluster = EdgeCluster("lund", 1, 1, "../carbon_1s/SE-SE4.csv", 0.001101, 1.0)
        scheduler.add_edge_cluster(edge_cluster)

        process = Process("test_process_1", 0, 0, "../filtered_workloads/0.csv")
        ok = scheduler.run(process)
        if not ok:
            print(f"failed to run process <{process.name}>")

        
        process = Process("test_process_2", 0, 0, "../filtered_workloads/1.csv")
        ok = scheduler.run(process)
        if not ok:
            print(f"failed to run process <{process.name}>")
        
        process = Process("test_process_3", 0, 0, "../filtered_workloads/3.csv")
        ok = scheduler.run(process)
        if not ok:
            print(f"failed to run process <{process.name}>")

        for _ in range(1000000):
            if scheduler.tick():
                break 
        
        print("total carbon emission [g]: ", scheduler.cumulative_emission)
        print("total energy [kWh]: ", scheduler.cumulative_energy)
if __name__ == '__main__':
    unittest.main()
