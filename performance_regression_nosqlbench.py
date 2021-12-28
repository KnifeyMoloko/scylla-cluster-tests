import logging

from performance_regression_test import PerformanceRegressionTest

LOGGER = logging.getLogger(__name__)


class PerformanceRegressionNosqlBenchTest(PerformanceRegressionTest):
    #  pylint: disable=useless-super-delegation
    def __init__(self, *args):
        super().__init__(*args)

    def test_nosqlbench_perf(self):
        """
        Run a performance workload with NoSQLBench. The specifics of the
        workload should be defined in the respective test case yaml file.
        """
        stress_cmd = self.params.get("stress_cmd")
        self.create_test_stats(sub_type='mixed', doc_id_with_timestamp=True)
        stress_queue = self.run_stress_thread(stress_cmd=stress_cmd, stress_num=1, stats_aggregate_cmds=False)
        results = self.get_stress_results(queue=stress_queue)
        LOGGER.info("Raw nosqlbench run result: %s", results)
        self.update_test_details(scylla_conf=True)
        self.check_regression()
