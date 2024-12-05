from scheduler import *
from edge_cluster import *
import unittest

class TestSchedulerAlg(unittest.TestCase):
      def test_scheduler(self):
          pass
          # scheduler = Scheduler(workloads_stats_dir='../filtered_workloads_1s_stats')
          # edge_cluster_1 = EdgeCluster("umea", 1, 1, "../carbon_1s_30d/SE-SE1.csv", 0.001101, 1.0, "./test_output/umea.csv")
          # scheduler.add_edge_cluster(edge_cluster_1)
          # edge_cluster_2 = EdgeCluster("warsaw", 1, 1, "../carbon_1s_30d/PL.csv", 0.001101, 1.0, "./test_output/warsaw.csv")
          # scheduler.add_edge_cluster(edge_cluster_2)
          # 
          # clusters = [edge_cluster_1, edge_cluster_2] 
          # workloads = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]  # List of workload indices
          # 
          # # Minimize emissions
          # _, best_branch = scheduler.search_tree(0, workloads, clusters)
          #
          # if best_branch is None:
          #       print("No cluster found")
          #       exit(-1)
          #
          # for cluster in best_branch:
          #     if cluster is not None:
          #         print(cluster.name)

          # print("Minimize Emission:", min_emission_total)
          # print("Best Cluster:", best_cluster.name)
          
