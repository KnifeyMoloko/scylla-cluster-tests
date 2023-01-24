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
# Copyright (c) 2022 ScyllaDB
from __future__ import annotations
from pathlib import Path
from typing import Literal, Any

import yaml
from pydantic import BaseModel

from sdcm.remote import LocalCmdRunner


class UDF(BaseModel):
    """
    Provides the representation for User Defined Functions in SCT,
    and the interface for creating queries for them.

    User Defined Functions work very much the same as regular CQL functions, and can be defined via CQL,
    e.g. :

        CREATE FUNCTION accumulate_len(acc tuple<bigint,bigint>, a text)
        RETURNS NULL ON NULL INPUT
        RETURNS tuple<bigint,bigint>
        LANGUAGE lua as 'return {acc[1] + 1, acc[2] + #a}';

    The scripting languages supported as of 2022.1 / 5.0 are:
    - Lua
    - WASM

    UDF/UDA presentation from 2019 summit: https://www.scylladb.com/tech-talk/udf-uda-and-whats-in-the-future/
    UDFs initial commit: https://github.com/scylladb/scylladb/commit/1fe062aed4b1ceb5a97b4333ae6c1901854a7f39
    WASM support design doc: https://github.com/scylladb/scylladb/blob/master/docs/dev/wasm.md
    WASM blog post: https://www.scylladb.com/2022/04/14/wasmtime/
    UDF/UDA testing desing doc: https://docs.google.com/document/d/16GTe1bLmMBC5IVCjC_nY-UNnMCLr_3V2C6K6tiYPstQ/edit?usp=sharing
    """
    name: str
    args: str
    called_on_null_input_returns: str
    return_type: str
    language: Literal["lua", "xwasm"]
    script_name: str
    script: str = ""

    def __init__(self, **data: Any):
        super().__init__(**data)
        self._compile_script_from_c_code()

    def get_create_query(self, ks: str, create_or_replace: bool = True) -> str:
        create_part = "CREATE OR REPLACE FUNCTION" if create_or_replace else "CREATE FUNCTION"
        return f"{create_part} {ks}.{self.name}{self.args} " \
               f"RETURNS {self.called_on_null_input_returns} ON NULL INPUT " \
               f"RETURNS {self.return_type} " \
               f"LANGUAGE {self.language} " \
               f"AS '{self.script}'"

    @classmethod
    def from_yaml(cls, udf_yaml_filename: str) -> UDF:
        with Path(udf_yaml_filename).open(mode="r", encoding="utf-8") as udf_yaml:
            return cls(**yaml.safe_load(udf_yaml))

    def _compile_script_from_c_code(self):
        source_code_path = Path("sdcm/utils/udf_scripts") / self.script_name
        wasm_output_path = source_code_path.parent / self.script_name.replace(".c", ".wasm")
        wat_output_path = source_code_path.parent / self.script_name.replace(".c", ".wat")
        compilation_cmd = f"clang -O2 --target=wasm32 --no-standard-libraries -Wl,--export-all -Wl,--no-entry " \
                          f"{source_code_path}" \
                          f" -o {wasm_output_path}"
        wasm2wat_cmd = f"sdcm/utils/udf_scripts/wabt/bin/wasm2wat " \
                       f"{wasm_output_path.absolute()} > {wat_output_path.absolute()}"
        expected_wasm2_wat_binary_path = Path("sdcm/utils/udf_scripts/wabt/build/wasm2wat")

        assert source_code_path.is_file(), "Could not find the source code file for the script under path: " \
                                           "%s" % source_code_path.absolute()

        local_cmd_runner = LocalCmdRunner()
        local_cmd_runner.run(cmd=compilation_cmd)

        assert expected_wasm2_wat_binary_path.is_file(), "No wasm2wat file found in path: " \
                                                         "%s" % expected_wasm2_wat_binary_path.absolute()

        local_cmd_runner.run(wasm2wat_cmd)

        assert wat_output_path.is_file(), "Could not find output .wat file in path: %s" % wat_output_path.absolute()

        self.script = wat_output_path.read_text(encoding="utf-8")
        print(f"Script in the wat output path:\n{self.script}")


def _load_all_udfs() -> dict[str, UDF]:
    """Convenience functions for loading all the existing UDF scripts from /sdcm/utils/udf_scripts"""
    udfs = {}
    yaml_file_paths = Path("sdcm/utils/udf_scripts").glob("*.yaml")
    for script in yaml_file_paths:
        udf = UDF.from_yaml(str(script))
        udfs.update({script.stem: udf})
    return udfs


def _clone_and_install_wabt() -> None:
    expected_wasm2wat_binary_path = Path("sdcm/utils/udf_scripts/wabt/bin/wasm2wat")

    if expected_wasm2wat_binary_path.is_file():
        return

    local_cmd_runner = LocalCmdRunner()
    git_clone_cmd = "cd sdcm/utils/udf_scripts && " \
                    "git clone --recursive https://github.com/WebAssembly/wabt " \
                    "&& cd wabt " \
                    "&& git submodule update --init"
    libuv_apt_install_command = "sudo apt-get install libuv1"
    cmake_cmd = "cd sdcm/utils/udf_scripts/wabt && " \
                "mkdir build && " \
                "cd build && " \
                "cmake .. && " \
                "cmake --build ."

    local_cmd_runner.run(git_clone_cmd)
    local_cmd_runner.run(libuv_apt_install_command)
    local_cmd_runner.run(cmake_cmd)


UDFS = _load_all_udfs()
