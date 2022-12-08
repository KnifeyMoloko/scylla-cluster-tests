from typing import Tuple, NamedTuple, Callable

from sdcm.cluster import BaseNode
from sdcm.stress_thread import CassandraStressThread
from sdcm.tester import ClusterTester
from sdcm.utils.udf import UDFS
from sdcm.utils.uda import UDAS


class UDFVerification(NamedTuple):
    name: str
    query: str
    verifier_func: Callable


class UDAUDFTest(ClusterTester):
    """
    Test Scylla with User Defined Functions and User Defined Aggregates,
    using cassandra-stress.
    """
    KEYSPACE_NAME = "ks"
    CF_NAME = "uda_udf"

    def test_uda_and_udf(self) -> None:
        self.log.info("Starting UDA/UDF test...")
        self.prewrite_db_with_data()
        node: BaseNode = self.db_cluster.get_node()

        for udf in UDFS.values():
            self.log.info("Creating the following UDF: %s", udf.name)
            cmd = udf.get_create_query(ks="ks")
            with self.db_cluster.cql_connection_patient(node=node) as session:
                session.execute(cmd)
            self.log.info("UDF %s created", udf.name)

        self._verify_udf_functions()

        for uda in UDAS.values():
            cmd = uda.get_create_query_string(ks="ks")
            node.run_cqlsh(cmd=cmd)

        write_thread, uda_udf_thread = self.run_stress_threads()

        # wait for stress to complete
        self.verify_stress_thread(cs_thread_pool=write_thread)
        self.verify_stress_thread(cs_thread_pool=uda_udf_thread)
        self.log.info("Test completed")

    def run_stress_threads(self) -> Tuple[CassandraStressThread, CassandraStressThread]:
        self.log.info("Running mixed workload c-s thread alongside uda/udf stress thread...")
        stress_cmd = self.params.get('stress_cmd')[0]
        uda_udf_cmd = self.params.get('stress_cmd')[1]
        stress_thread = self.run_stress_thread(stress_cmd=stress_cmd,
                                               stats_aggregate_cmds=False,
                                               round_robin=False)
        uda_udf_thread = self.run_stress_thread(stress_cmd=uda_udf_cmd,
                                                stats_aggregate_cmds=False,
                                                round_robin=False)
        self.log.info("Stress threads started.")
        return stress_thread, uda_udf_thread

    def prewrite_db_with_data(self) -> None:
        self.log.info("Prewriting database...")
        stress_cmd = self.params.get('prepare_write_cmd')
        pre_thread = self.run_stress_thread(stress_cmd=stress_cmd, stats_aggregate_cmds=False, round_robin=False)
        self.verify_stress_thread(cs_thread_pool=pre_thread)
        self.log.info("Database pre write completed")

    def _verify_udf_functions(self):
        row_query = UDFVerification(name="row_query",
                                    query=f"SELECT * FROM {self.KEYSPACE_NAME}.{self.CF_NAME} LIMIT 1",
                                    verifier_func=lambda c2, c3, c7: all([c2, c3, c7]))
        verifications = [
            UDFVerification(name="lua_var_length_counter",
                            query=f"SELECT {self.KEYSPACE_NAME}.lua_var_length_counter(c7) AS result "
                                  f"FROM {self.KEYSPACE_NAME}.{self.CF_NAME} LIMIT 1",
                            verifier_func=lambda c2, c3, c7, query_response: len(c7) == query_response.result),
            UDFVerification(name="xwasm_plus",
                            query=f"SELECT {self.KEYSPACE_NAME}.xwasm_plus(c2, c3) AS result "
                                  f"FROM {self.KEYSPACE_NAME}.{self.CF_NAME} LIMIT 1",
                            verifier_func=lambda c2, c3, c7, query_response: c2 + c3 == query_response.result),
            UDFVerification(name="xwasm_div",
                            query=f"SELECT {self.KEYSPACE_NAME}.xwasm_div(c2, c3) AS result "
                                  f"FROM {self.KEYSPACE_NAME}.{self.CF_NAME} LIMIT 1",
                            verifier_func=lambda c2, c3, c7, query_response: c2 // c3 == query_response.result)
        ]
        self.log.info("Starting UDF verifications...")

        with self.db_cluster.cql_connection_patient(self.db_cluster.get_node(), verbose=False) as session:
            row_result = session.execute(row_query.query).one()
            self.log.info("Row query was: %s", row_result)
            c2_value = row_result.c2
            c3_value = row_result.c3
            c7_value = row_result.c7
            assert row_query.verifier_func(c2_value, c3_value, c7_value), \
                "Expected row values to not be None, at least one them was. " \
                "c2_value: %s, c3_value: %s, c7 value: %s" % (c2_value, c3_value, c7_value)

            for verification in verifications:
                self.log.info("Running UDF verification: %s; query: %s", verification.name, verification.query)
                query_result = session.execute(verification.query).one()
                self.log.info("Verification query result: %s", query_result)
                assert verification.verifier_func(c2_value, c3_value, c7_value, query_result)
            self.log.info("Finished running UDF verifications.")
