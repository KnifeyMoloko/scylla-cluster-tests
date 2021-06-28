from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Union
from dataclasses import dataclass, fields

from yaml import safe_load


class StressTestConfigParserError(Exception):
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


class NdBenchCLIConfigParser(StressTestConfigParser):
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


if __name__ == "__main__":
    cfg_path = '/home/mc/scylla-cluster-tests/test-cases/longevity/mc-longevity-ndbench-100gb-4h_alt.yaml'
    parser = NdBenchCLIConfigParser()
    parser.load_config(cfg_path)
    print(parser.parse_config_as_cli_command())
