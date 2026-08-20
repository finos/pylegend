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

import os
import pathlib
import subprocess
import sys
import textwrap


# A name that no legacy single-byte codec can represent: not in ascii, not in
# cp1252 (the en-US Windows default) and not in latin-1. Whatever locale the
# child interpreter ends up with, this value forces the question.
NON_ASCII_NAME = "謝敏奇"

# Run the export in a child interpreter with the legacy locale forced on. The
# parent's encoding is irrelevant: this has to fail on Linux CI too, where the
# locale is UTF-8 and the defect would otherwise stay invisible.
LEGACY_LOCALE_ENV = {
    "PYTHONUTF8": "0",
    "PYTHONCOERCECLOCALE": "0",
    "LC_ALL": "C",
    "LANG": "C",
}

EXPORT_SCRIPT = textwrap.dedent(
    """
    import json, sys
    from pylegend.core.request.response_reader import ResponseReader
    from pylegend.core.tds.result_handler import ToCsvFileResultHandler

    class _Column:
        def __init__(self, name):
            self._name = name

        def get_name(self):
            return self._name

    class _Frame:
        def __init__(self, names):
            self._columns = [_Column(n) for n in names]

        def columns(self):
            return self._columns

    out_file, name = sys.argv[1], sys.argv[2]
    payload = json.dumps(
        {"result": {"rows": [{"values": ["Client", name, "Firm X"]}]}}
    ).encode("utf-8")

    ToCsvFileResultHandler(out_file).handle_result(
        _Frame(["First Name", "Last Name", "Firm/Legal Name"]),
        ResponseReader(iter([payload])),
    )
    """
)


class TestToCsvFileResultHandlerEncoding:

    def test_csv_export_is_utf8_regardless_of_locale(self, tmp_path: pathlib.Path) -> None:
        """CSV export must produce UTF-8 on every host.

        The file used to be opened without an encoding, so results were
        re-encoded with locale.getpreferredencoding(): silently non-UTF-8 on a
        cp950/cp1252 host, and a UnicodeEncodeError that aborts the export
        where the locale cannot represent the value at all. Every other result
        handler is explicitly UTF-8, so the same result serialised differently
        depending only on the machine that ran it.
        """
        script = tmp_path / "export.py"
        script.write_text(EXPORT_SCRIPT, encoding="utf-8")
        out_file = tmp_path / "result.csv"

        env = {**os.environ, **LEGACY_LOCALE_ENV}
        completed = subprocess.run(  # noqa: S603 - fixed argv, no shell; see tests/conftest.py
            [sys.executable, str(script), str(out_file), NON_ASCII_NAME],
            capture_output=True,
            text=True,
            env=env,
        )

        # Catches the abort: on an ASCII locale the writer raises and the
        # export dies part-written.
        assert completed.returncode == 0, completed.stderr

        # Catches the silent corruption. Asserting on decoded bytes is the
        # point: reading the file back with the same locale default round-trips
        # the mojibake and passes with or without the fix.
        assert NON_ASCII_NAME in out_file.read_bytes().decode("utf-8")

    def test_ascii_export_is_byte_identical(self, tmp_path: pathlib.Path) -> None:
        """The ASCII path, which the existing fixtures cover, is unchanged."""
        script = tmp_path / "export.py"
        script.write_text(EXPORT_SCRIPT, encoding="utf-8")
        out_file = tmp_path / "ascii.csv"

        completed = subprocess.run(  # noqa: S603 - fixed argv, no shell
            [sys.executable, str(script), str(out_file), "Smith"],
            capture_output=True,
            text=True,
            env={**os.environ, **LEGACY_LOCALE_ENV},
        )

        assert completed.returncode == 0, completed.stderr
        assert out_file.read_bytes() == (
            b"First Name,Last Name,Firm/Legal Name\r\nClient,Smith,Firm X\r\n"
        )
