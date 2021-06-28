from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Union
from pprint import pprint
from dataclasses import dataclass, fields, field

from yaml import safe_load


class StressTestConfigParserError(Exception):
    pass


@dataclass
class NdBenchConfig:
    cli_run_cmd: str = field(default="run", init=False)
    web_app_run_cmd: str = field(default="appRun", init=False)
    web_app_extra_docker_opts: str = field(default="", init=False)
    type: str
    driver: str
    numKeys: int
    numValues: int
    dataSize: int
    readEnabled: bool
    numReaders: int
    numWriters: int
    cass_colsPerRow: int
    cass_writeConsistencyLevel: str
    generateCheckusm: bool
    timeoutMillis: int
    writeRateLimit: int

    def __post_init__(self):
        pass

    def __build_cli_cmd(self):
        pass

    def __build_docker_extra_opts(self):
        pass


class StressTestConfigParser(ABC):
    def __init__(self):
        self._config = None

    def load_config(self, config_yaml_path: Union[str, Path]):
        try:
            with open(file=config_yaml_path, mode="r", encoding='utf-8') as infile:
                self._config = safe_load(infile)
        except Exception as e:
            raise StressTestConfigParserError(e)

    @property
    def stress_config(self) -> Dict[str, Any]:
        return self._config["stress_config"]

    @abstractmethod
    def parse_config(self):
        return NotImplemented


class NdBenchConfigParser(StressTestConfigParser):
    def parse_config(self) -> Dict[str, Any]:
        return self._config

    def parse_config_as_cli_command(self) -> str:
        command_tokens = ["./gradlew"]
        common_prefix = "-Dndbench.config"
        for key, value in self.stress_config.items():
            if key == "type":
                pass
            if "cass" in key:
                command_tokens.append(f"{common_prefix}.{key.replace('_', '.')}={value}")
            else:
                command_tokens.append(f"{common_prefix}.cli.{key}={value}")
        command_tokens.append("run")

        return " ".join(command_tokens)

    def parse_config_as_web_app_command(self) -> str:
        ndb_config = NdBenchConfig(**self.stress_config)
        return ndb_config



if __name__ == "__main__":
    cfg_path = '/home/mc/scylla-cluster-tests/test-cases/longevity/mc-longevity-ndbench-100gb-4h_alt.yaml'
    parser = NdBenchConfigParser()
    parser.load_config(cfg_path)
    pprint(parser.parse_config_as_cli_command())
    pprint(parser.parse_config_as_web_app_command())
