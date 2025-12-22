# Copyright 2025 Goldman Sachs
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

from io import StringIO
from textwrap import dedent
import pytest

from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers.test_legend_service_frames import simple_person_service_frame_pandas_api
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
from pylegend.core.request.legend_client import LegendClient


class TestInfoFunction:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_info_error(self) -> None:
        columns = [
            PrimitiveTdsColumn.integer_column("col1"),
            PrimitiveTdsColumn.integer_column("col2")
        ]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(['test_schema', 'test_table'], columns)

        # memory
        with pytest.raises(NotImplementedError) as n:
            frame.info(memory_usage=True)
        assert n.value.args[0] == "memory_usage parameter is not implemented yet in Pandas API"

        # max_cols
        with pytest.raises(TypeError) as t:
            frame.info(max_cols='10')  # type: ignore
        assert t.value.args[0] == "max_cols must be an integer, but got <class 'str'>"

        # buffer
        with pytest.raises(TypeError) as t:
            frame.info(buf="not a buffer")  # type: ignore
        assert t.value.args[0] == "buf is not a writable buffer"

    # flake8: noqa
    def test_e2e_info_function(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        frame: PandasApiTdsFrame = simple_person_service_frame_pandas_api(legend_test_server["engine_port"])

        # verbose True
        buffer_verbose = StringIO()
        frame.info(buf=buffer_verbose)
        output_verbose = buffer_verbose.getvalue()
        expected_verbose = dedent("""\
                    <class 'pylegend.extensions.tds.pandas_api.frames.pandas_api_legend_service_input_frame.PandasApiLegendServiceInputFrame'>
                    RangeIndex: 7 entries
                    Data columns (total 4 columns):
                    #  Column           Non-Null Count  Dtype  
                    -  ---------------  --------------  -------
                    0  First Name       7 non-null      String 
                    1  Last Name        7 non-null      String 
                    2  Age              7 non-null      Integer
                    3  Firm/Legal Name  7 non-null      String 
                    dtypes: Integer(1), String(3)
                    """)
        assert output_verbose == expected_verbose

        # max_cols
        buffer_concise = StringIO()
        frame.info(verbose=False, buf=buffer_concise)
        output_concise = buffer_concise.getvalue()

        buffer_cols = StringIO()
        frame.info(max_cols=2, buf=buffer_cols)
        output_cols = buffer_cols.getvalue()
        assert output_cols == output_concise

        expected_concise = dedent("""\
                    <class 'pylegend.extensions.tds.pandas_api.frames.pandas_api_legend_service_input_frame.PandasApiLegendServiceInputFrame'>
                    RangeIndex: 7 entries
                    Columns: 4 entries, First Name to Firm/Legal Name
                    dtypes: Integer(1), String(3)
                    """)
        assert output_concise == expected_concise

        buffer_comb = StringIO()
        frame.info(verbose=True, max_cols=2, buf=buffer_comb)
        output_comb = buffer_comb.getvalue()
        assert output_comb == output_verbose

        # show counts
        buffer_no_counts = StringIO()
        frame.info(show_counts=False, buf=buffer_no_counts)
        output_no_counts = buffer_no_counts.getvalue()
        expected_no_counts = dedent("""\
                    <class 'pylegend.extensions.tds.pandas_api.frames.pandas_api_legend_service_input_frame.PandasApiLegendServiceInputFrame'>
                    RangeIndex: 7 entries
                    Data columns (total 4 columns):
                    #  Column           Dtype  
                    -  ---------------  -------
                    0  First Name       String 
                    1  Last Name        String 
                    2  Age              Integer
                    3  Firm/Legal Name  String 
                    dtypes: Integer(1), String(3)
                    """)
        assert output_no_counts == expected_no_counts
