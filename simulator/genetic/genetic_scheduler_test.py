import sys
sys.path.append('/home/johan/dev/github/johankristianss/carbonsim/simulator')
import unittest
from genetic_scheduler import GeneticScheduler
from edge_cluster import EdgeCluster

class TestGeneticScheduler(unittest.TestCase):
    def test_genetic_scheduler(self):
        # clusters = {
        #     "A": {"GPUs": 2, "CO2": 10},
        #     "B": {"GPUs": 3, "CO2": 100},
        # }

        edgeclusters = []
        edge_cluster = EdgeCluster("Lulea", 1, 2, "../../carbon_1s_30d/SE-SE1.csv", 0.001101, 1.0, "./test_output/lulea_cluster_output.csv")
        edgeclusters.append(edge_cluster)
        edge_cluster = EdgeCluster("Stockholm", 1, 2, "../../carbon_1s_30d/SE-SE2.csv", 0.001101, 1.0, "./test_output/stockholm_cluster_output.csv")
        edgeclusters.append(edge_cluster)
        edge_cluster = EdgeCluster("Warsaw", 1, 1, "../../carbon_1s_30d/PL.csv", 0.001101, 1.0, "./test_output/warsaw_cluster_output.csv")
        edgeclusters.append(edge_cluster)
    
        workload_stats_dir = "../../filtered_workloads_1s_stats"
        workload_indices = [1, 20, 3, 4, 400]
    
        gs = GeneticScheduler(workload_stats_dir)
        gs.set_clusters_status(edgeclusters)
        gs.set_workloads(workload_indices)
        best_schedule, fitess = gs.run()
    
        print("==================================== INPUTS ====================================")
        print("Clusters:")
        gs.print_clusters()
        print("Workloads:")
        gs.print_workloads()
    
        print("==================================== RESULTS ====================================")
        print("Best Schedule:", best_schedule)
        print("Best Fitness:", fitess)
    
        for _ in range(100000):
            for edgecluster in edgeclusters:
                edgecluster.tick()
    
        gs.set_clusters_status(edgeclusters)
        gs.set_workloads(workload_indices)
        best_schedule, fitess = gs.run()
    
        print("==================================== INPUTS ====================================")
        print("Clusters:")
        gs.print_clusters()
        print("Workloads:")
        gs.print_workloads()
    
        print("==================================== RESULTS ====================================")
        print("Best Schedule:", best_schedule)
        print("Best Fitness:", fitess)

if __name__ == '__main__':
    unittest.main()

