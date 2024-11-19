def minimize_emissions(self, current_idx, next_process_idxs, clusters, gpu_availability, memo={}):
    # 1. calculate the emission for the current workload on all clusters, we have len(clusters) of choices
    # 2. for each choice in step 1, calculate the emission for the first process in next_process_idxs on all clusters, we have len(clusters) of choices
    # 3. for each choice in step 2, calculate the emission for the second process in next_process_idxs on all clusters, we have len(clusters) of choices
    # 4. ...

    # If we have 2 clusters and 10 workfloes, we have 2^10 possibilities, which is 1024 possibilities
    # This cannot be computed in a reasonable time, so we need to use memoization to store the results of the subproblems

    # Optimization goal: select the cluster that minimizes the total emission of the entire workflow, i.e. current process + next processes

    # power can be calculated using method below
    power_draw_mean, power_draw_median, total_length_seconds = self.get_process_power_draw_stat(idx)

    # current_idx is the idx of the current process
    # next_process_idxs is the list of idxs of the next processes

    # emission can be calculated using method below
    current_emission = cluster.integrate_carbon_intensity(current_process, total_length_seconds)








