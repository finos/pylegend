# Copyright 2023 Goldman Sachs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from random import Random
import socket


def generate_dynamic_port() -> int:
    min_port = 50352
    retry_count = 0
    random = Random()
    while retry_count < 10:
        retry_count += 1
        next_port = random.randint(min_port, min_port + 10000)
        if _is_port_not_in_use(next_port):
            return next_port

    raise RuntimeError("Unable to obtain a free port")  # pragma: no cover


def _is_port_not_in_use(port: int) -> bool:
    with socket.socket() as s:
        try:
            s.bind(("127.0.0.1", port))
            return True
        except socket.error:  # pragma: no cover
            return False  # pragma: no cover
