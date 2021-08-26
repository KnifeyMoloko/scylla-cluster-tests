#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2021 ScyllaDB
import re
import time
from typing import NamedTuple, Optional, Callable, Any, Type

from fabric.runners import Result

from sdcm.cluster import BaseNode, BaseScyllaCluster, LOGGER
from sdcm.nemesis import StartStopMajorCompaction, Nemesis
from sdcm.rest.storage_service_client import StorageServiceClient
from sdcm.tester import ClusterTester
from sdcm.utils.common import ParallelObject
from sdcm.utils.compaction_ops import CompactionOps


# class ScrubModes(NamedTuple):
#     ABORT: str = "ABORT"
#     SKIP: str = "SKIP"
#     SEGREGATE: str = "SEGREGATE"
#     VALIDATE: str = "VALIDATE"
#
#
# class NodetoolCommands(NamedTuple):
#     compact: str = "compact"
#     cfstats: str = "cfstats"
#     flush: str = "flush"
#     stop_major_compaction: str = "stop COMPACTION"
#     stop_scrub_compaction: str = "stop SCRUB"
#     stop_cleanup_compaction: str = "stop CLEANUP"
#     stop_index_build_compaction: str = "stop INDEX_BUILD"
#     stop_upgrade_compaction: str = "stop UPGRADE"
#     stop_reshape_compaction: str = "stop RESHAPE"
#
#
# class CompactionOps:
#     NODETOOL_CMD = NodetoolCommands()
#     SCRUB_MODES = ScrubModes()
#
#     def __init__(self, cluster: BaseScyllaCluster):
#         self.cluster = cluster
#         self.node: BaseNode = self.cluster.get_node()
#         self.storage_service_client = StorageServiceClient(node=self.node)
#
#     def trigger_major_compaction(self, keyspace: str = "keyspace1", cf: str = "standard1") -> Result:
#         return self.storage_service_client.compact_ks_cf(keyspace=keyspace, cf=cf)
#
#     def trigger_scrub_compaction(self,
#                                  keyspace: str = "keyspace1",
#                                  cf: str = "standard1",
#                                  scrub_mode: Optional[str] = None) -> Result:
#         params = {"keyspace": keyspace, "cf": cf}
#         if scrub_mode:
#             params.update({"scrub_mode": scrub_mode})
#
#         return self.storage_service_client.scrub_ks_cf(**params)
#
#     def trigger_cleanup_compaction(self, keyspace: str = "keyspace1", cf: str = "standard1") -> Result:
#         return self.storage_service_client.cleanup_ks_cf(keyspace=keyspace, cf=cf)
#
#     def trigger_validation_compaction(self, keyspace: str = "keyspace1", cf: str = "standard1") -> Result:
#         return self.storage_service_client.scrub_ks_cf(keyspace=keyspace,
#                                                        cf=cf,
#                                                        scrub_mode=self.SCRUB_MODES.VALIDATE)
#
#     def trigger_upgrade_compaction(self, keyspace: str = "keyspace1", cf: str = "standard1"):
#         return self.storage_service_client.upgrade_sstables(keyspace=keyspace, cf=cf)
#
#     def trigger_flush(self):
#         self.node.run_nodetool(self.NODETOOL_CMD.flush)
#
#     def stop_major_compaction(self):
#         self._stop_compaction(self.NODETOOL_CMD.stop_major_compaction)
#
#     def stop_index_build_compaction(self):
#         self._stop_compaction(self.NODETOOL_CMD.stop_index_build_compaction)
#
#     def stop_scrub_compaction(self):
#         self._stop_compaction(self.NODETOOL_CMD.stop_scrub_compaction)
#
#     def stop_cleanup_compaction(self):
#         self._stop_compaction(self.NODETOOL_CMD.stop_cleanup_compaction)
#
#     def stop_upgrade_compaction(self):
#         self._stop_compaction(self.NODETOOL_CMD.stop_upgrade_compaction)
#
#     def stop_reshape_compaction(self):
#         self._stop_compaction(self.NODETOOL_CMD.stop_reshape_compaction)
#
#     def stop_validation_compaction(self):
#         self.stop_scrub_compaction()
#
#     def disable_autocompaction_on_ks_cf(self, node: BaseNode,  keyspace: str = "", cf: Optional[str] = ""):
#         node = node if node else self.node
#         node.run_nodetool(f'disableautocompaction {keyspace} {cf}')
#
#     def _stop_compaction(self, nodetool_cmd: str):
#         LOGGER.info("Stopping compaction with nodetool %s", nodetool_cmd)
#         self.node.run_nodetool(nodetool_cmd)
#
#     @staticmethod
#     def stop_on_user_compaction_logged(node: BaseNode, watch_for: str, timeout: int,
#                                        stop_func: Callable, mark: Optional[int] = None):
#         start_time = time.time()
#         with open(node.system_log, "r") as log_file:
#             if mark:
#                 log_file.seek(mark)
#
#             while time.time() - start_time < timeout:
#                 line = log_file.readline()
#                 if watch_for in line:
#                     stop_func()
#                     LOGGER.info("Watch for expression found %s in log line %s", watch_for, line)
#                     break

class StopCompactionTest(ClusterTester):
    def setUp(self):
        super().setUp()
        self.node1: BaseNode = self.db_cluster.nodes[0]
        self.storage_service_client = StorageServiceClient(self.node1)
        self.populate_data_parallel(size_in_gb=10, blocking=True, replication_factor=1)
        self.disable_autocompaction_on_all_nodes()

    def disable_autocompaction_on_all_nodes(self):
        compaction_ops = CompactionOps(cluster=self.db_cluster)
        compaction_ops.disable_autocompaction_on_ks_cf(node=self.node1)

    def test_stop_major_compaction(self):
        """
        Test that we can stop a major compaction with <nodetool stop COMPACTION>.
        1. Running in parallel:
            1.1 Trigger the major compaction.
            1.2 Watch for the first line for user initiated compaction to show
            up in the logs and then issue the nodetool stop COMPACTION command.
        2. Get the sstable count for the keyspace/table.
        3. Grep the logs for a line informing of the major compaction being
        stopped due to a user request.
        4. Assert that we grepped a line in (3).
        """
        grep_pattern = r'Compaction for keyspace1/standard1 was stopped due to: user request'
        # trigger_func = {"func": compaction_ops.trigger_major_compaction,
        #                 "kwargs": {}}
        # watcher_func = {"func": compaction_ops.stop_on_user_compaction_logged,
        #                 "kwargs": {"node": self.node1,
        #                            "watch_for": "User initiated compaction",
        #                            "timeout": 120,
        #                            "stop_func": compaction_ops.stop_major_compaction}}

        self._stop_compaction_base_test_scenario(
            compaction_nemesis=StartStopMajorCompaction(tester_obj=self,
                                                        termination_event=self.db_cluster.nemesis_termination_event),
            grep_pattern=grep_pattern)

    # def test_stop_scrub_compaction(self):
    #     """
    #     Test that we can stop a scurb compaction with <nodetool stop SCRUB>.
    #     1. Running in parallel:
    #         1.1 Trigger the scrub compaction.
    #         1.2 Watch for the first line indicating scrubbing to show up in the
    #         logs and then issue the nodetool stop SCRUB command.
    #     2. Grep the logs for a line informing of the compaction being stopped
    #     due to a user request.
    #     3. Assert that according to the logs scrubbing was stopped due to a
    #     user request.
    #     """
    #     compaction_ops = CompactionOps(self.db_cluster)
    #     trigger_func = {"func": compaction_ops.trigger_scrub_compaction,
    #                     "kwargs": {}}
    #     watcher_func = {"func": compaction_ops.stop_on_user_compaction_logged,
    #                     "kwargs": {"node": self.node1,
    #                                "watch_for": "Scrubbing",
    #                                "timeout": 120,
    #                                "stop_func": compaction_ops.stop_scrub_compaction}}
    #     grep_pattern = r'Compaction for keyspace1/standard1 was stopped due to: user request'
    #
    #     self._stop_compaction_base_test_scenario(trigger_func=trigger_func,
    #                                              watch_func=watcher_func,
    #                                              grep_pattern=grep_pattern)
    #
    # def test_stop_cleanup_compaction(self):
    #     """
    #     Test that we can stop a cleanup compaction with <nodetool stop CLEANUP>.
    #     1. Running in parallel:
    #         1.1 Trigger the cleanup compaction.
    #         1.2 Watch for the first line indicating cleanup to show up in the
    #         logs and then issue the nodetool stop CLEANUP command.
    #     2. Grep the logs for a line informing of the compaction being stopped
    #     due to a user request.
    #     3. Assert that according to the logs compaction was stopped due to a
    #     user request.
    #     """
    #     compaction_ops = CompactionOps(self.db_cluster)
    #     trigger_func = {"func": compaction_ops.trigger_cleanup_compaction,
    #                     "kwargs": {}}
    #     watch_func = {"func": compaction_ops.stop_on_user_compaction_logged,
    #                   "kwargs": {"node": self.node1,
    #                              "timeout": 120,
    #                              "watch_for": "Cleaning",
    #                              "stop_func": compaction_ops.stop_cleanup_compaction}}
    #     grep_pattern = r'Compaction for keyspace1/standard1 was stopped due to: user request'
    #
    #     self._stop_compaction_base_test_scenario(trigger_func=trigger_func,
    #                                              watch_func=watch_func,
    #                                              grep_pattern=grep_pattern)
    #
    # def test_stop_validation_compaction(self):
    #     """
    #     Test that we can stop a validation compaction with
    #     <nodetool stop VALIDATION>.
    #     1. Running in parallel:
    #         1.1 Trigger the validation compaction.
    #         1.2 Watch for the first line indicating validation to show up in
    #         the logs and then issue the nodetool stop VALIDATION command.
    #     2. Grep the logs for a line informing of the compaction being stopped
    #     due to a user request.
    #     4. Assert that according to the logs compaction was stopped due to a
    #     user request.
    #     """
    #     compaction_ops = CompactionOps(self.db_cluster)
    #     trigger_func = {"func": compaction_ops.trigger_validation_compaction, "kwargs": {}}
    #     watch_func = {"func": compaction_ops.stop_on_user_compaction_logged,
    #                   "kwargs": {"node": self.node1,
    #                              "watch_for": "Scrubbing in validate mode",
    #                              "timeout": 120,
    #                              "stop_func": compaction_ops.stop_validation_compaction}}
    #     grep_pattern = r'Compaction for keyspace1/standard1 was stopped due to: user request'
    #
    #     self._stop_compaction_base_test_scenario(trigger_func=trigger_func,
    #                                              watch_func=watch_func,
    #                                              grep_pattern=grep_pattern)
    #
    # def test_stop_upgrade_compaction(self):
    #     """
    #     Test that we can stop an upgrade compaction with <nodetool stop UPGRADE>.
    #     1. Initialize cluster with 1 node with "enable_sstables_mc_format" set
    #     to True, disabled autocompaction and some data rows inserted.
    #     2. Stop the cluster.
    #     3. Reset the configurations options for the cluster to include:
    #     "enable_sstables_mc_format" set to False.
    #     4. Running in parallel:
    #         4.1 Trigger the upgrade compaction.
    #         4.2 Watch for the first line indicating upgrade to show up in the
    #         logs and then issue the nodetool stop UPGRADE command.
    #     5. Grep the logs for a line informing of the compaction being stopped
    #     due to a user request.
    #     6. Assert that according to the logs scrubbing was stopped due to a
    #     user request.
    #     """
    #     downgraded_configuration_options = {"enable_sstables_mc_format": True,
    #                                         "enable_sstables_md_format": False}
    #     self.node1.run_nodetool("flush")
    #     LOGGER.info("Downgrading sstables format...")
    #     self.node1.stop_scylla_server()
    #     with self.node1.remote_scylla_yaml() as scylla_yaml:
    #         scylla_yaml.update(downgraded_configuration_options)
    #
    #     self.node1.start_scylla_server()
    #     self.node1.wait_db_up()
    #     self.node1.wait_jmx_up()
    #
    #     compaction_ops = CompactionOps(self.db_cluster)
    #     trigger_func = {"func": compaction_ops.trigger_upgrade_compaction, "kwargs": {}}
    #     watch_func = {"func": compaction_ops.stop_on_user_compaction_logged,
    #                   "kwargs": {"node": self.node1,
    #                              "watch_for": "Upgrade keyspace1.standard1",
    #                              "timeout": 300,
    #                              "stop_func": compaction_ops.stop_upgrade_compaction}}
    #     grep_pattern = r'Compaction for keyspace1/standard1 was stopped due to: user request'
    #
    #     self._stop_compaction_base_test_scenario(trigger_func=trigger_func,
    #                                              watch_func=watch_func,
    #                                              grep_pattern=grep_pattern)
    #
    # def test_stop_reshape_compaction(self):
    #     """
    #     Test that we can stop a reshape compaction with <nodetool stop RESHAPE>.
    #     1. Initialize cluster with 1 node.
    #     2. Running in parallel:
    #         2.1 Run refresh and restart. This will populate the db with data
    #         and flush to sstables using STCS as the compaction mode. Then it
    #         will alter the compaction mode to TWCS and refresh to trigger the
    #         reshape compaction.
    #         2.2 Watch for the first line indicating reshaping to show up in the
    #         logs and then issue the nodetool stop RESHAPE command.
    #     3. Grep the logs for a line informing of the compaction being stopped
    #     due to a user request.
    #     4. Assert that according to the logs scrubbing was stopped due to a
    #     user request.
    #     """
    #     compaction_ops = CompactionOps(self.db_cluster)
    #     trigger_func = {"func": self._reshape_scenario, "kwargs": {}}
    #     watch_func = {"func": compaction_ops.stop_on_user_compaction_logged,
    #                   "kwargs": {"node": self.node1,
    #                              "watch_for": "Reshape",
    #                              "timeout": 300,
    #                              "stop_func": compaction_ops.stop_reshape_compaction}}
    #     grep_pattern = r'Compaction for keyspace1/standard1 was stopped due to: user request'
    #     self._stop_compaction_base_test_scenario(
    #         trigger_func=trigger_func,
    #         watch_func=watch_func,
    #         grep_pattern=grep_pattern)
    #
    # def _reshape_scenario(self):
    #     twcs = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_size': 1,
    #             'compaction_window_unit': 'MINUTES', 'max_threshold': 1, 'min_threshold': 1}
    #     self.node1.run_nodetool(sub_cmd="flush")
    #     self.wait_no_compactions_running()
    #     self._copy_files()
    #     cmd = f"ALTER TABLE standard1 WITH compaction={twcs}"
    #     self.node1.run_cqlsh(cmd=cmd, keyspace="keyspace1")
    #     self.node1.run_nodetool("refresh -- keyspace1 standard1")
    #
    # def _copy_files(self, keyspace: str = "keyspace1"):
    #     LOGGER.info("Copying data files to ./staging and ./upload directories...")
    #     keyspace_dir = f'/var/lib/scylla/data/{keyspace}'
    #     cf_data_dir = self.node1.remoter.run(f"ls {keyspace_dir}").stdout.splitlines()[0]
    #     full_dir_path = f"{keyspace_dir}/{cf_data_dir}"
    #     upload_dir = f"{full_dir_path}/upload"
    #     staging_dir = f"{full_dir_path}/staging"
    #     # cp_cmd_upload = "find %s -type f | xargs -I {} cp -v -f {} %s" % (full_dir_path, upload_dir)
    #     # cp_cmd_staging = "find %s -type f | xargs -I {} cp -v -f {} %s" % (full_dir_path, staging_dir)
    #     cp_cmd_upload = f"cp -p {full_dir_path}/md-* {upload_dir}"
    #     cp_cmd_staging = f"cp -p {full_dir_path}/md-* {staging_dir}"
    #     self.node1.remoter.sudo(cp_cmd_staging)
    #     self.node1.remoter.sudo(cp_cmd_upload)
    #     LOGGER.info("Finished copying data files to ./staging and ./upload directories.")
    #
    def _stop_compaction_base_test_scenario(self,
                                            compaction_nemesis,
                                            # trigger_func: dict[str, Any],
                                            # watch_func: dict[str, Any],
                                            grep_pattern: str,):
        compaction_nemesis.disrupt()
        # self.wait_no_compactions_running()
        mark = self.node1.mark_log()
        # self.run_in_parallel([trigger_func, watch_func])
        found_grepped_expression = False
        with open(self.node1.system_log, "r") as logfile:
            pattern = re.compile(grep_pattern)
            logfile.seek(mark)
            for line in logfile.readlines():
                if pattern.findall(line):
                    found_grepped_expression = True

        self.assertTrue(found_grepped_expression, msg=f'Did not find the expected "{grep_pattern}" '
                                                      f'expression in the logs.')

    # @staticmethod
    # def run_in_parallel(funcs_with_kwargs: list[dict[str, Any]]):
    #     def _run_func(func: Callable, kwargs: Any):
    #         return func(**kwargs)
    #
    #     parallel_obj = ParallelObject(funcs_with_kwargs, timeout=300)
    #
    #     return parallel_obj.run(_run_func, unpack_objects=True)
