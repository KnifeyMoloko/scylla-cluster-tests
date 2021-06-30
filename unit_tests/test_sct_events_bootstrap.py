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
import os
import time
import unittest
import uuid
from pathlib import Path
from threading import Thread
from typing import List

from sdcm.sct_events.database import BootstrapEvent
from sdcm.utils.common import FileFollowerThread

LOGS = [
            "Disabling 'apt-daily' and 'apt-daily-upgrade' services...",
            "Waiting for preinstalled Scylla",
            "Waiting for Scylla Machine Image setup to finish...",
            "Done waiting for preinstalled Scylla",
            "Found ScyllaDB version with details: 4.5.rc3-0.20210620.706de00ef "
            "with build-id d28ca2bf35abaa8bbf9a2b5b401a0bbe20ad506e",
            "Found ScyllaDB version: 4.5.rc3",
            "Installing Scylla debug info...",
            'io.conf right after reboot: SEASTAR_IO = "--io-properties-file=/etc/scylla.d/io_properties.yaml"',
            "Starting Scylla Server...",
            "Setup in BaseLoaderSet",
            "(1/1) nodes ready, node Node "
            "longevity-ndbench-100gb-4h-ndbench--loader-node-4d77cb84-1 "
            "[54.247.62.34 | 10.0.2.165] (seed: False). Time elapsed: 245 s",
            "Verifying Scylla repo file",
            "Running: ./gradlew appRun",
            "Starting a Gradle Daemon, 1 incompatible and 1 stopped Daemons "
            "could not be reused, use --status for details",
            "Configure project :ndbench-api",
        ]


class MockEventsPublisher(FileFollowerThread):
    def __init__(self, node: str, mock_log_filename: str, event_id: str = None):
        super().__init__()

        self.node = str(node)
        self.mock_log_filename = mock_log_filename
        self.event_id = event_id
        self.start_time = time.time()

    def run(self) -> None:
        while not self.stopped() and (time.time() - self.start_time) < 30:
            if not os.path.isfile(self.mock_log_filename):
                time.sleep(0.5)
                continue

            for line_number, line in enumerate(self.follow_file(self.mock_log_filename)):
                if self.stopped():
                    break

                print(f"{line_number} : {line}")
                # for pattern, event in CS_ERROR_EVENTS_PATTERNS:
                #     if self.event_id:
                #         # Connect the event to the stress load
                #         event.event_id = self.event_id
                #
                #     if pattern.search(line):
                #         event.add_info(node=self.node, line=line, line_number=line_number).publish()
                #         break  # Stop iterating patterns to avoid creating two events for one line of the log


class TestBootstrapEvent(unittest.TestCase):
    temp_log_file_path = Path(f"/tmp/{time.time()}.log")

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_log_file_path.touch()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.temp_log_file_path.unlink()

    def setUp(self) -> None:
        self.event_id = uuid.uuid4()
        self.node = "1.2.4.5.6"
        self.bootstrap_event = BootstrapEvent(node=self.node,
                                              log_file_name=str(self.temp_log_file_path),
                                              publish_event=False)
        self.bootstrap_event.event_id = self.event_id

    def test_bootstrap_event_begin(self):
        self.bootstrap_event.begin_event()
        actual = str(self.bootstrap_event)
        expected = f"(BootstrapEvent Severity.NORMAL) period_type=begin event_id={self.event_id} " \
                   f"node={self.node}"

        self.assertEqual(actual, expected)

    def test_bootstrap_event_duration(self):
        duration = 60
        duration_fmt = "1m0s"
        self.bootstrap_event.duration = duration
        actual = str(self.bootstrap_event)
        expected = f"(BootstrapEvent Severity.NORMAL) period_type=not-set " \
                   f"event_id={self.event_id} duration={duration_fmt} node={self.node}"

        self.assertEqual(actual, expected)

    def test_bootstrap_as_ctx_manager(self):
        duration = 10

        with self.bootstrap_event:
            self.assertEqual(self.bootstrap_event.period_type, "begin")
            time.sleep(duration)

        self.assertEqual(self.bootstrap_event.duration, duration)
        self.assertEqual(self.bootstrap_event.period_type, "end")

    def test_bootstrap_event_failure(self):
        duration = 605
        duration_fmt = "10m5s"
        errors = ["Failed with status 1"]
        self.bootstrap_event.add_error(errors)
        self.bootstrap_event.duration = duration
        self.bootstrap_event.end_event()

        actual = str(self.bootstrap_event)
        expected = f"(BootstrapEvent Severity.NORMAL) period_type=end event_id={self.event_id} " \
                   f"duration={duration_fmt} node={self.node} errors=['{errors[0]}']"

        self.assertEqual(actual, expected)

    def test_trigger_bootstrap_begin_with_file_follower(self):
        from pprint import pprint
        writer_thread = self._get_logs_writer_thread()
        writer_thread.start()
        print(f"Writer thread status before: {writer_thread.is_alive()}")
        time.sleep(20)
        writer_thread.join()
        print(f"Writer thread status after: {writer_thread.is_alive()}")
        # with self.temp_log_file_path.open(mode="r") as infile:
        #     print("I'm inside")
        #     for line in infile:
        #         print("I'm even more inside")
        #         print(line)
        print(self.temp_log_file_path.read_text())

        # with some_file_follower_ctx as publisher:
        #     run_for_a_while()

        # check the event registry/queue

    def _write_logs(self, log_list: List[str]):
        with self.temp_log_file_path.open(mode="w") as infile:
            while log_list:
                infile.write(log_list.pop() + "\n")
                time.sleep(10)

    def _get_logs_writer_thread(self) -> Thread:
        t = Thread(target=self._write_logs, args=(), kwargs={"log_list": LOGS})
        return t
