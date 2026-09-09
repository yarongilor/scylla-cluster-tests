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
# Copyright (c) 2026 ScyllaDB

import pytest

from sdcm.remote.libssh2_client import Result
from sdcm.rest.storage_service_client import StorageServiceClient


class CannedStdoutRemoter:
    def __init__(self, stdout: str):
        self._stdout = stdout

    def run(self, cmd: str, timeout: int, retry: int) -> Result:
        return Result(stdout=self._stdout, stderr="", exited=0)


class CannedStdoutNode:
    def __init__(self, stdout: str):
        self.remoter = CannedStdoutRemoter(stdout)


def test_force_terminate_repair(fake_node):
    client = StorageServiceClient(fake_node)
    result = client.force_terminate_repair()

    assert result.stdout == (
        "curl -v --retry 5 --retry-max-time 300 --connect-timeout 10"
        ' -X POST "http://localhost:10000/storage_service/force_terminate_repair"'
    )


@pytest.mark.parametrize(
    "stdout,expected",
    [
        ("[1]", [1]),
        ("[]", []),
        ("", []),
        ("[1, 2]\n", [1, 2]),
    ],
)
def test_active_repairs(stdout, expected):
    client = StorageServiceClient(CannedStdoutNode(stdout))

    assert client.active_repairs() == expected
