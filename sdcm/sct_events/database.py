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
# Copyright (c) 2020 ScyllaDB
import os
import re
import logging
import time
from re import Pattern
from typing import Type, List, Tuple, Generic, Optional, Any

from sdcm.sct_events import Severity, SctEventProtocol
from sdcm.sct_events.base import SctEvent, LogEvent, LogEventProtocol, T_log_event, InformationalEvent, \
    DatabaseEvent
from sdcm.test_config import TestConfig
from sdcm.utils.common import FileFollowerThread

TOLERABLE_REACTOR_STALL: int = 1000  # ms

LOGGER = logging.getLogger(__name__)


class DatabaseLogEvent(LogEvent, abstract=True):
    NO_SPACE_ERROR: Type[LogEventProtocol]
    UNKNOWN_VERB: Type[LogEventProtocol]
    CLIENT_DISCONNECT: Type[LogEventProtocol]
    SEMAPHORE_TIME_OUT: Type[LogEventProtocol]
    SYSTEM_PAXOS_TIMEOUT: Type[LogEventProtocol]
    RESTARTED_DUE_TO_TIME_OUT: Type[LogEventProtocol]
    EMPTY_NESTED_EXCEPTION: Type[LogEventProtocol]
    DATABASE_ERROR: Type[LogEventProtocol]
    BAD_ALLOC: Type[LogEventProtocol]
    SCHEMA_FAILURE: Type[LogEventProtocol]
    RUNTIME_ERROR: Type[LogEventProtocol]
    FILESYSTEM_ERROR: Type[LogEventProtocol]
    STACKTRACE: Type[LogEventProtocol]

    # REACTOR_STALLED must be above BACKTRACE as it has "Backtrace" in its message
    REACTOR_STALLED: Type[LogEventProtocol]
    BACKTRACE: Type[LogEventProtocol]
    ABORTING_ON_SHARD: Type[LogEventProtocol]
    SEGMENTATION: Type[LogEventProtocol]
    INTEGRITY_CHECK: Type[LogEventProtocol]
    BOOT: Type[LogEventProtocol]
    STOP: Type[LogEventProtocol]
    SUPPRESSED_MESSAGES: Type[LogEventProtocol]
    stream_exception: Type[LogEventProtocol]
    POWER_OFF: Type[LogEventProtocol]


MILLI_RE = re.compile(r"(\d+) ms")


# pylint: disable=too-few-public-methods
class ReactorStalledMixin(Generic[T_log_event]):
    tolerable_reactor_stall: int = TOLERABLE_REACTOR_STALL

    def add_info(self: T_log_event, node, line: str, line_number: int) -> T_log_event:
        try:
            # Dynamically handle reactor stalls severity.
            if int(MILLI_RE.findall(line)[0]) >= self.tolerable_reactor_stall:
                self.severity = Severity.ERROR
        except (ValueError, IndexError, ):
            LOGGER.warning("failed to read REACTOR_STALLED line=[%s] ", line)
        return super().add_info(node=node, line=line, line_number=line_number)


DatabaseLogEvent.add_subevent_type("NO_SPACE_ERROR", severity=Severity.ERROR,
                                   regex="No space left on device")
DatabaseLogEvent.add_subevent_type("UNKNOWN_VERB", severity=Severity.WARNING,
                                   regex="unknown verb exception")
DatabaseLogEvent.add_subevent_type("CLIENT_DISCONNECT", severity=Severity.WARNING,
                                   regex=r"\!INFO.*cql_server - exception while processing connection:.*")
DatabaseLogEvent.add_subevent_type("SEMAPHORE_TIME_OUT", severity=Severity.WARNING,
                                   regex="semaphore_timed_out")
# This scylla WARNING includes "exception" word and reported as ERROR. To prevent it I add the subevent below and locate
# it before DATABASE_ERROR. Message example:
# storage_proxy - Failed to apply mutation from 10.0.2.108#8: exceptions::mutation_write_timeout_exception
# (Operation timed out for system.paxos - received only 0 responses from 1 CL=ONE.)
DatabaseLogEvent.add_subevent_type("SYSTEM_PAXOS_TIMEOUT", severity=Severity.WARNING,
                                   regex=".*mutation_write_*|.*Operation timed out for system.paxos.*|"
                                         ".*Operation failed for system.paxos.*")
DatabaseLogEvent.add_subevent_type("RESTARTED_DUE_TO_TIME_OUT", severity=Severity.WARNING,
                                   regex="scylla-server.service.*State 'stop-sigterm' timed out.*Killing")
DatabaseLogEvent.add_subevent_type("EMPTY_NESTED_EXCEPTION", severity=Severity.WARNING,
                                   regex=r"cql_server - exception while processing connection: "
                                         r"seastar::nested_exception \(seastar::nested_exception\)$")
DatabaseLogEvent.add_subevent_type("DATABASE_ERROR", severity=Severity.ERROR,
                                   regex="Exception ")
DatabaseLogEvent.add_subevent_type("BAD_ALLOC", severity=Severity.ERROR,
                                   regex="std::bad_alloc")
DatabaseLogEvent.add_subevent_type("SCHEMA_FAILURE", severity=Severity.ERROR,
                                   regex="Failed to load schema version")
DatabaseLogEvent.add_subevent_type("RUNTIME_ERROR", severity=Severity.ERROR,
                                   regex="std::runtime_error")
DatabaseLogEvent.add_subevent_type("FILESYSTEM_ERROR", severity=Severity.ERROR,
                                   regex="filesystem_error")
DatabaseLogEvent.add_subevent_type("STACKTRACE", severity=Severity.ERROR,
                                   regex="stacktrace")

# REACTOR_STALLED must be above BACKTRACE as it has "Backtrace" in its message
DatabaseLogEvent.add_subevent_type("REACTOR_STALLED", mixin=ReactorStalledMixin, severity=Severity.DEBUG,
                                   regex="Reactor stalled")
DatabaseLogEvent.add_subevent_type("BACKTRACE", severity=Severity.ERROR,
                                   regex="backtrace")
DatabaseLogEvent.add_subevent_type("ABORTING_ON_SHARD", severity=Severity.ERROR,
                                   regex="Aborting on shard")
DatabaseLogEvent.add_subevent_type("SEGMENTATION", severity=Severity.ERROR,
                                   regex="segmentation")
DatabaseLogEvent.add_subevent_type("INTEGRITY_CHECK", severity=Severity.ERROR,
                                   regex="integrity check failed")
DatabaseLogEvent.add_subevent_type("BOOT", severity=Severity.NORMAL,
                                   regex="Starting Scylla Server")
DatabaseLogEvent.add_subevent_type("STOP", severity=Severity.NORMAL,
                                   regex="Stopping Scylla Server")
DatabaseLogEvent.add_subevent_type("SUPPRESSED_MESSAGES", severity=Severity.WARNING,
                                   regex="journal: Suppressed")
DatabaseLogEvent.add_subevent_type("stream_exception", severity=Severity.ERROR,
                                   regex="stream_exception")
DatabaseLogEvent.add_subevent_type("POWER_OFF", severity=Severity.CRITICAL, regex="Powering Off")


SYSTEM_ERROR_EVENTS = (
    DatabaseLogEvent.NO_SPACE_ERROR(),
    DatabaseLogEvent.UNKNOWN_VERB(),
    DatabaseLogEvent.CLIENT_DISCONNECT(),
    DatabaseLogEvent.SEMAPHORE_TIME_OUT(),
    DatabaseLogEvent.SYSTEM_PAXOS_TIMEOUT(),
    DatabaseLogEvent.RESTARTED_DUE_TO_TIME_OUT(),
    DatabaseLogEvent.EMPTY_NESTED_EXCEPTION(),
    DatabaseLogEvent.DATABASE_ERROR(),
    DatabaseLogEvent.BAD_ALLOC(),
    DatabaseLogEvent.SCHEMA_FAILURE(),
    DatabaseLogEvent.RUNTIME_ERROR(),
    DatabaseLogEvent.FILESYSTEM_ERROR(),
    DatabaseLogEvent.STACKTRACE(),

    # REACTOR_STALLED must be above BACKTRACE as it has "Backtrace" in its message
    DatabaseLogEvent.REACTOR_STALLED(),
    DatabaseLogEvent.BACKTRACE(),
    DatabaseLogEvent.ABORTING_ON_SHARD(),
    DatabaseLogEvent.SEGMENTATION(),
    DatabaseLogEvent.INTEGRITY_CHECK(),
    DatabaseLogEvent.BOOT(),
    DatabaseLogEvent.STOP(),
    DatabaseLogEvent.SUPPRESSED_MESSAGES(),
    DatabaseLogEvent.stream_exception(),
    DatabaseLogEvent.POWER_OFF(),
)
SYSTEM_ERROR_EVENTS_PATTERNS: List[Tuple[re.Pattern, LogEventProtocol]] = \
    [(re.compile(event.regex, re.IGNORECASE), event) for event in SYSTEM_ERROR_EVENTS]
BACKTRACE_RE = re.compile(r'(?P<other_bt>/lib.*?\+0x[0-f]*\n)|(?P<scylla_bt>0x[0-f]*\n)', re.IGNORECASE)


class ScyllaHelpErrorEvent(SctEvent, abstract=True):
    duplicate: Type[SctEventProtocol]
    filtered: Type[SctEventProtocol]
    message: str

    def __init__(self, message: Optional[str] = None, severity=Severity.ERROR):
        super().__init__(severity=severity)

        # Don't include `message' to the state if it's None.
        if message is not None:
            self.message = message

    @property
    def msgfmt(self):
        fmt = super().msgfmt + ": type={0.type}"
        if hasattr(self, "message"):
            fmt += " message={0.message}"
        return fmt


ScyllaHelpErrorEvent.add_subevent_type("duplicate")
ScyllaHelpErrorEvent.add_subevent_type("filtered")


class FullScanEvent(SctEvent, abstract=True):
    start: Type[SctEventProtocol]
    finish: Type[SctEventProtocol]

    message: str

    def __init__(self, db_node_ip: str, ks_cf, message: Optional[str] = None, severity=Severity.NORMAL):
        super().__init__(severity=severity)

        self.db_node_ip = db_node_ip
        self.ks_cf = ks_cf

        # Don't include `message' to the state if it's None.
        if message is not None:
            self.message = message

    @property
    def msgfmt(self):
        fmt = super().msgfmt + ": type={0.type} select_from={0.ks_cf} on db_node={0.db_node_ip}"
        if hasattr(self, "message"):
            fmt += " message={0.message}"
        return fmt


FullScanEvent.add_subevent_type("start")
FullScanEvent.add_subevent_type("finish")


class IndexSpecialColumnErrorEvent(InformationalEvent):
    def __init__(self, message: str, severity: Severity = Severity.ERROR):
        super().__init__(severity=severity)

        self.message = message

    @property
    def msgfmt(self) -> str:
        return super().msgfmt + ": message={0.message}"


class BootstrapEvent(DatabaseEvent):
    ...


class DBLogReaderThread(FileFollowerThread):
    def __init__(self,
                 base_node: Any,
                 log_file_path: str,
                 test_config: TestConfig = None,
                 start_from_beginning: bool = False,
                 exclude_from_logging: List[Tuple[Pattern, LogEventProtocol]] = None,
                 last_log_position: int = 0,
                 last_line_num: int = 0):
        self.base_node = base_node
        self._log_file_path = log_file_path
        self.test_config = test_config
        self._start_from_beginning = start_from_beginning
        self._exclude_from_logging = exclude_from_logging
        self._start_search_from_byte = last_log_position
        self._last_line_no = last_line_num
        self._backtraces = []
        self._system_log_errors_index = []
        self.file_iter = self.follow_file(self._log_file_path)
        super().__init__()

    @property
    def is_alive(self):
        return not self.stopped()

    def run(self):
        while not self.stopped():
            # if not os.path.isfile(self._log_file_path):
            #     time.sleep(0.1)
            #     continue
            time.sleep(30)
            DatabaseLogEvent.BOOT().clone().add_info(node=self.base_node,
                                                     line_number=999,
                                                     line=f"Next line: {self._get_line()}")

            # self._read_file()

    def _get_line(self):
        yield next(self.file_iter)


    def _read_file(self):
        if self._start_from_beginning:
            self._start_search_from_byte = 0
            self._last_line_no = 0

        with open(self._log_file_path, mode="r") as db_file:
            if self._start_search_from_byte:
                db_file.seek(self._start_search_from_byte)

            for index, line in enumerate(self.follow_file(self._log_file_path)):
                #  the "continuation branch"
                # if not self._start_from_beginning and self.test_config.RSYSLOG_ADDRESS:
                line = line.strip()
                for pattern in self._exclude_from_logging:
                    if pattern in line:
                        LOGGER.debug(f"Found pattern: {pattern} in: {line}")
                        # self._handle_backtraces(line)
                        self._filter_line(index=index, line=line)

    def _handle_backtraces(self, line: str):
        match = BACKTRACE_RE.search(line)
        one_line_backtrace = []
        if match and self._backtraces:
            data = match.groupdict()
            if data['other_bt']:
                self._backtraces[-1]['backtrace'] += [data['other_bt'].strip()]
            if data['scylla_bt']:
                self._backtraces[-1]['backtrace'] += [data['scylla_bt'].strip()]
            elif "backtrace:" in line.lower() and "0x" in line:
                # This part handles the backtrases are printed in one line.
                # Example:
                # [shard 2] seastar - Exceptional future ignored: exceptions::mutation_write_timeout_exception
                # (Operation timed out for system.paxos - received only 0 responses from 1 CL=ONE.),
                # backtrace:   0x3316f4d#012  0x2e2d177#012  0x189d397#012  0x2e76ea0#012  0x2e770af#012
                # 0x2eaf065#012  0x2ebd68c#012  0x2e48d5d#012  /opt/scylladb/libreloc/libpthread.so.0+0x94e1#012
                split_line = re.split("backtrace:", line, flags=re.IGNORECASE)
                for trace_line in split_line[1].split():
                    if trace_line.startswith('0x') or 'scylladb/lib' in trace_line:
                        one_line_backtrace.append(trace_line)

                    if one_line_backtrace and self._backtraces:
                        self._backtraces[-1]['backtrace'] = one_line_backtrace

    def _filter_line(self, index: int, line: str):
        # actual filter and decision for the line
        if index not in self._system_log_errors_index or self._start_from_beginning:
            # for each line use all regexes to match, and if found send an event
            for pattern, event in SYSTEM_ERROR_EVENTS_PATTERNS:
                match = pattern.search(line)
                if match:
                    self._system_log_errors_index.append(index)
                    cloned_event = event.clone().add_info(node=self, line_number=index, line=line)
                    self._backtraces.append(dict(event=cloned_event, backtrace=[]))
                    break  # Stop iterating patterns to avoid creating two events for one line of the log
