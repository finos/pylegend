import json
from textwrap import dedent

import numpy as np
from pylegend._typing import (
    PyLegendDict,
    PyLegendUnion,
)
import pytest
from pylegend.core.request.legend_client import LegendClient
from pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame import PandasApiTdsFrame
from pylegend.core.tds.tds_column import PrimitiveTdsColumn
from pylegend.core.tds.tds_frame import FrameToPureConfig, FrameToSqlConfig
from pylegend.extensions.tds.pandas_api.frames.pandas_api_table_spec_input_frame import PandasApiTableSpecInputFrame
from tests.test_helpers import generate_pure_query_and_compile
from tests.test_helpers.test_legend_service_frames import (
    simple_person_service_frame_pandas_api,
    simple_trade_service_frame_pandas_api
)



class TestPandasApiGroupbyAndAggregateErrors:

    def test_groupby_error_invalid_level(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.string_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.groupby('col1', level=1)
        assert v.value.args[0] == (
            "The 'level' parameter of the groupby function is not supported yet. "
            "Please specify groupby column names using the 'by' parameter."
        )

    def test_groupby_error_as_index_true(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.groupby('col1', as_index=True)
        assert v.value.args[0] == (
            "The 'as_index' parameter of the groupby function must be False, "
            "but got: True (type: bool)"
        )

    def test_groupby_error_group_keys_true(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.groupby('col1', group_keys=True)
        assert v.value.args[0] == (
            "The 'group_keys' parameter of the groupby function must be False, "
            "but got: True (type: bool)"
        )

    def test_groupby_error_observed_true(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.groupby('col1', observed=True)
        assert v.value.args[0] == (
            "The 'observed' parameter of the groupby function must be False, "
            "but got: True (type: bool)"
        )

    def test_groupby_error_dropna_true(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.groupby('col1', dropna=True)
        assert v.value.args[0] == (
            "The 'dropna' parameter of the groupby function must be False, "
            "but got: True (type: bool)"
        )

    def test_groupby_error_invalid_by_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(TypeError) as v:
            frame.groupby(by=123)  # type: ignore
        assert v.value.args[0] == (
            "The 'by' parameter in groupby function must be a string or a list of strings."
            "but got: 123 (type: int)"
        )

    def test_groupby_error_empty_by_list(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(ValueError) as v:
            frame.groupby(by=[])
        assert v.value.args[0] == (
            "The 'by' parameter in groupby function must contain at least one column name."
        )

    def test_groupby_error_missing_column(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(KeyError) as v:
            frame.groupby(by=['col1', 'missing_col'])
        assert v.value.args[0] == (
            "Column(s) ['missing_col'] in groupby function's provided columns list do not exist in the current frame. "
            "Current frame columns: ['col1']"
        )

    def test_groupby_getitem_error_invalid_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby('col1')
        with pytest.raises(TypeError) as v:
            gb[123]  # type: ignore
        assert v.value.args[0] == (
            "Column selection after groupby function must be a string or a list of strings, "
            "but got: 123 (type: int)"
        )

    def test_groupby_getitem_error_empty_list(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby('col1')
        with pytest.raises(ValueError) as v:
            gb[[]]
        assert v.value.args[0] == (
            "When performing column selection after groupby, at least one column must be selected."
        )

    def test_groupby_getitem_error_missing_column(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"), PrimitiveTdsColumn.integer_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        gb = frame.groupby('col1')
        with pytest.raises(KeyError) as v:
            gb[['col2', 'missing_col']]
        assert v.value.args[0] == (
            "Column(s) ['missing_col'] selected after groupby do not exist in the current frame. "
            "Current frame columns: ['col1', 'col2']"
        )

    def test_aggregate_error_invalid_axis(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.aggregate({'col1': 'sum'}, axis=1)
        assert v.value.args[0] == (
            "The 'axis' parameter of the aggregate function must be 0 or 'index', but got: 1"
        )

    def test_aggregate_error_extra_args(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.aggregate({'col1': 'sum'}, 0, "extra_arg")
        assert v.value.args[0] == (
            "AggregateFunction currently does not support additional positional "
            "or keyword arguments. Please remove extra *args/**kwargs."
        )

    def test_aggregate_error_extra_kwargs(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.aggregate({'col1': 'sum'}, extra_kwarg=1)
        assert v.value.args[0] == (
            "AggregateFunction currently does not support additional positional "
            "or keyword arguments. Please remove extra *args/**kwargs."
        )

    def test_aggregate_dict_error_key_not_string(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(TypeError) as v:
            frame.aggregate({1: 'sum'})  # type: ignore
        assert v.value.args[0] == (
            "Invalid `func` argument for the aggregate function.\n"
            "When a dictionary is provided, all keys must be strings.\n"
            "But got key: 1 (type: int)\n"
        )

    def test_aggregate_dict_error_key_missing_column(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(ValueError) as v:
            frame.aggregate({'missing': 'sum'})
        assert v.value.args[0] == (
            "Invalid `func` argument for the aggregate function.\n"
            "When a dictionary is provided, all keys must be column names.\n"
            "Available columns are: ['col1']\n"
            "But got key: 'missing' (type: str)\n"
        )

    def test_aggregate_dict_error_list_invalid_element(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(TypeError) as v:
            frame.aggregate({'col1': ['sum', 123]})
        assert v.value.args[0] == (
            "Invalid `func` argument for the aggregate function.\n"
            "When a list is provided for a column, all elements must be callable, str, or np.ufunc.\n"
            "But got element at index 1: 123 (type: int)\n"
        )

    def test_aggregate_dict_error_scalar_invalid_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(TypeError) as v:
            frame.aggregate({'col1': 123})
        assert v.value.args[0] == (
            "Invalid `func` argument for the aggregate function.\n"
            "When a dictionary is provided, the value must be a callable, str, or np.ufunc "
            "(or a list containing these).\n"
            "But got value for key 'col1': 123 (type: int)\n"
        )

    def test_aggregate_list_error_invalid_element(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(TypeError) as v:
            frame.aggregate(['sum', 123])
        assert v.value.args[0] == (
            "Invalid `func` argument for the aggregate function.\n"
            "When a list is provided as the main argument, all elements must be callable, str, or np.ufunc.\n"
            "But got element at index 1: 123 (type: int)\n"
        )

    def test_aggregate_scalar_error_invalid_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(TypeError) as v:
            frame.aggregate(123)
        assert v.value.args[0] == (
            "Invalid `func` argument for aggregate function. "
            "Expected a callable, str, np.ufunc, a list containing exactly one of these, "
            "or a mapping[str -> callable/str/ufunc/a list containing exactly one of these]. "
            "But got: 123 (type: int)"
        )

    def test_normalize_agg_func_error_unsupported_string(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.aggregate({'col1': 'unsupported_func'})
        assert v.value.args[0] == (
            "Invalid `func` argument for the aggregate function.\n"
            "The string 'unsupported_func' does not correspond to any supported aggregation.\n"
            "Available string functions are: ['amax', 'amin', 'average', 'count', 'len', 'length', 'max', 'maximum'"
            ", 'mean', 'median', 'min', 'minimum', 'nanmax', 'nanmean', 'nanmedian', 'nanmin', 'nanstd', 'nansum', 'nanvar',"
            " 'size', 'std', 'std_dev', 'sum', 'var', 'variance']"
        )

    def test_normalize_agg_func_error_unsupported_ufunc(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        with pytest.raises(NotImplementedError) as v:
            frame.aggregate({'col1': np.sin})
        assert v.value.args[0] == (
            "Invalid `func` argument for the aggregate function.\n"
            "The NumPy function 'sin' is not supported.\n"
            "Supported aggregate functions are: ['amax', 'amin', 'average', 'count', 'len', 'length', 'max', 'maximum',"
            " 'mean', 'median', 'min', 'minimum', 'nanmax', 'nanmean', 'nanmedian', 'nanmin', 'nanstd', 'nansum',"
            " 'nanvar', 'size', 'std', 'std_dev', 'sum', 'var', 'variance']"
        )

    def test_aggregate_custom_lambda_invalid_return_type(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        with pytest.raises(TypeError) as v:
            frame.aggregate(lambda x: 0)
            
        assert v.value.args[0] == (
            "Custom aggregation function must return a PyLegendPrimitive (Expression).\n"
            "But got type: int\n"
            "Value: 0"
        )



class TestGroupbyFunctionality:

    @pytest.fixture(autouse=True)
    def init_legend(self, legend_test_server: PyLegendDict[str, PyLegendUnion[int,]]) -> None:
        self.legend_client = LegendClient("localhost", legend_test_server["engine_port"], secure_http=False)

    def test_groupby_simple_query_generation(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"),
                   PrimitiveTdsColumn.date_column("col2"),
                   PrimitiveTdsColumn.integer_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(by="col1").aggregate({"col2": "min", "col3": [np.sum]})
        expected = """\
                    SELECT
                        "root".col1 AS "col1",
                        MIN("root".col2) AS "col2",
                        SUM("root".col3) AS "sum(col3)"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".col1
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[col2:{r | $r.col2}:{c | $c->min()}, 'sum(col3)':{r | $r.col3}:{c | $c->sum()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->min()}, 'sum(col3)':{r | $r.col3}:{c | $c->sum()}])"
        )

    def test_groupby_column_selection_for_aggregation(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"),
                   PrimitiveTdsColumn.date_column("col2"),
                   PrimitiveTdsColumn.integer_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby("col1")[['col2', 'col3']].aggregate({'col2':["max"], 'col3': [np.sum, np.mean]})
        expected = """                    SELECT
                        "root".col1 AS "col1",
                        MAX("root".col2) AS "max(col2)",
                        SUM("root".col3) AS "sum(col3)",
                        AVG("root".col3) AS "mean(col3)"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".col1
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected)[:-1]
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~['max(col2)':{r | $r.col2}:{c | $c->max()}, 'sum(col3)':{r | $r.col3}:{c | $c->sum()}, 'mean(col3)':{r | $r.col3}:{c | $c->average()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False), self.legend_client) == (
            "#Table(test_schema.test_table)#"
            "->groupBy(~[col1], ~['max(col2)':{r | $r.col2}:{c | $c->max()}, 'sum(col3)':{r | $r.col3}:{c | $c->sum()}, 'mean(col3)':{r | $r.col3}:{c | $c->average()}])"
        )

    def test_groupby_multiple_grouping_columns(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1"),
                   PrimitiveTdsColumn.string_column("col2"),
                   PrimitiveTdsColumn.integer_column("col3"),
                   PrimitiveTdsColumn.float_column("col4")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby(["col1", "col2"]).aggregate({"col3": "sum", "col4": ["min", "max"]})
        expected_sql = """            SELECT
                "root".col1 AS "col1",
                "root".col2 AS "col2",
                SUM("root".col3) AS "col3",
                MIN("root".col4) AS "min(col4)",
                MAX("root".col4) AS "max(col4)"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1,
                "root".col2
            """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        expected_pure = dedent(
            """            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1, col2],
                ~[col3:{r | $r.col3}:{c | $c->sum()}, 'min(col4)':{r | $r.col4}:{c | $c->min()}, 'max(col4)':{r | $r.col4}:{c | $c->max()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=True), self.legend_client) == expected_pure

        expected_pure_pretty_false = (
            "#Table(test_schema.test_table)#"
            "->groupBy(~[col1, col2], ~[col3:{r | $r.col3}:{c | $c->sum()}, 'min(col4)':{r | $r.col4}:{c | $c->min()}, 'max(col4)':{r | $r.col4}:{c | $c->max()}])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False),
                                               self.legend_client) == expected_pure_pretty_false

    def test_groupby_broadcast_agg_func_string(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1"),
                   PrimitiveTdsColumn.integer_column("col2"),
                   PrimitiveTdsColumn.float_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby("col1").aggregate("sum")
        expected_sql = """            SELECT
                "root".col1 AS "col1",
                SUM("root".col2) AS "col2",
                SUM("root".col3) AS "col3"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1
            """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        expected_pure = dedent(
            """            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[col2:{r | $r.col2}:{c | $c->sum()}, col3:{r | $r.col3}:{c | $c->sum()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=True), self.legend_client) == expected_pure

        expected_pure_pretty_false = (
            "#Table(test_schema.test_table)#"
            "->groupBy(~[col1], ~[col2:{r | $r.col2}:{c | $c->sum()}, col3:{r | $r.col3}:{c | $c->sum()}])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False),
                                               self.legend_client) == expected_pure_pretty_false

    def test_groupby_broadcast_agg_func_list(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("col1"),
                   PrimitiveTdsColumn.integer_column("col2"),
                   PrimitiveTdsColumn.float_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)
        frame = frame.groupby("col1").aggregate(['sum', 'mean'])
        expected_sql = """            SELECT
                "root".col1 AS "col1",
                SUM("root".col2) AS "sum(col2)",
                AVG("root".col2) AS "mean(col2)",
                SUM("root".col3) AS "sum(col3)",
                AVG("root".col3) AS "mean(col3)"
            FROM
                test_schema.test_table AS "root"
            GROUP BY
                "root".col1
            """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        expected_pure = dedent(
            """            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~['sum(col2)':{r | $r.col2}:{c | $c->sum()}, 'mean(col2)':{r | $r.col2}:{c | $c->average()}, 'sum(col3)':{r | $r.col3}:{c | $c->sum()}, 'mean(col3)':{r | $r.col3}:{c | $c->average()}]
              )"""
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=True), self.legend_client) == expected_pure

        expected_pure_pretty_false = (
            "#Table(test_schema.test_table)#->groupBy(~[col1], "
            "~['sum(col2)':{r | $r.col2}:{c | $c->sum()}, 'mean(col2)':{r | $r.col2}:{c | $c->average()}, "
            "'sum(col3)':{r | $r.col3}:{c | $c->sum()}, 'mean(col3)':{r | $r.col3}:{c | $c->average()}])"
        )
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(pretty=False),
                                               self.legend_client) == expected_pure_pretty_false

    def test_groupby_aggregate_with_string_input(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"),
                   PrimitiveTdsColumn.integer_column("col2"),
                   PrimitiveTdsColumn.integer_column("col3")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("col1").aggregate("sum")
        
        expected_sql = """\
                    SELECT
                        "root".col1 AS "col1",
                        SUM("root".col2) AS "col2",
                        SUM("root".col3) AS "col3"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".col1
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]

        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~[col2:{r | $r.col2}:{c | $c->sum()}, col3:{r | $r.col3}:{c | $c->sum()}]
              )"""
        )

    def test_groupby_aggregate_with_list_input(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("col1"),
                   PrimitiveTdsColumn.number_column("col2")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("col1")["col2"].aggregate(["min", "max"])
        
        expected_sql = """\
                    SELECT
                        "root".col1 AS "col1",
                        MIN("root".col2) AS "min(col2)",
                        MAX("root".col2) AS "max(col2)"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".col1
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]
        
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[col1],
                ~['min(col2)':{r | $r.col2}:{c | $c->min()}, 'max(col2)':{r | $r.col2}:{c | $c->max()}]
              )"""
        )

    def test_groupby_aggregate_with_mixed_dict_input(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("Region"),
                   PrimitiveTdsColumn.integer_column("Sales"),
                   PrimitiveTdsColumn.integer_column("Profit")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("Region").aggregate({
            "Sales": ["sum", "mean"],
            "Profit": "max"
        })
        
        expected_sql = """\
                    SELECT
                        "root".Region AS "Region",
                        SUM("root".Sales) AS "sum(Sales)",
                        AVG("root".Sales) AS "mean(Sales)",
                        MAX("root".Profit) AS "Profit"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".Region
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]
        
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[Region],
                ~['sum(Sales)':{r | $r.Sales}:{c | $c->sum()}, 'mean(Sales)':{r | $r.Sales}:{c | $c->average()}, Profit:{r | $r.Profit}:{c | $c->max()}]
              )"""
        )

    def test_groupby_aggregate_with_lambdas(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("id"),
                   PrimitiveTdsColumn.float_column("val")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("id").aggregate({
            "val": [lambda x: x.min(), lambda x: x.max()]
        })
        
        expected_sql = """\
                    SELECT
                        "root".id AS "id",
                        MIN("root".val) AS "lambda_1(val)",
                        MAX("root".val) AS "lambda_2(val)"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".id
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]
        
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[id],
                ~['lambda_1(val)':{r | $r.val}:{c | $c->min()}, 'lambda_2(val)':{r | $r.val}:{c | $c->max()}]
              )"""
        )

    def test_groupby_multiple_grouping_keys(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("Category"),
                   PrimitiveTdsColumn.string_column("SubCategory"),
                   PrimitiveTdsColumn.integer_column("Amount")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby(["Category", "SubCategory"]).aggregate({"Amount": "sum"})
        
        expected_sql = """\
                    SELECT
                        "root".Category AS "Category",
                        "root".SubCategory AS "SubCategory",
                        SUM("root".Amount) AS "Amount"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".Category,
                        "root".SubCategory
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]
        
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[Category, SubCategory],
                ~[Amount:{r | $r.Amount}:{c | $c->sum()}]
              )"""
        )

    def test_groupby_explicit_aggregation_of_grouping_key(self) -> None:
        columns = [PrimitiveTdsColumn.string_column("Type"),
                   PrimitiveTdsColumn.integer_column("Value")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("Type").aggregate({"Type": "count", "Value": "sum"})
        
        expected_sql = """\
                    SELECT
                        "root".Type AS "Type",
                        COUNT("root".Type) AS "count(Type)",
                        SUM("root".Value) AS "Value"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".Type
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]
        
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[Type],
                ~['count(Type)':{r | $r.Type}:{c | $c->count()}, Value:{r | $r.Value}:{c | $c->sum()}]
              )"""
        )

    def test_groupby_numpy_functions_integration(self) -> None:
        columns = [PrimitiveTdsColumn.integer_column("id"),
                   PrimitiveTdsColumn.float_column("score")]
        frame: PandasApiTdsFrame = PandasApiTableSpecInputFrame(["test_schema", "test_table"], columns)

        frame = frame.groupby("id")["score"].aggregate([np.min, np.sum])
        
        expected_sql = """\
                    SELECT
                        "root".id AS "id",
                        MIN("root".score) AS "min(score)",
                        SUM("root".score) AS "sum(score)"
                    FROM
                        test_schema.test_table AS "root"
                    GROUP BY
                        "root".id
                    """
        assert frame.to_sql_query(FrameToSqlConfig()) == dedent(expected_sql)[:-1]
        
        assert generate_pure_query_and_compile(frame, FrameToPureConfig(), self.legend_client) == dedent(
            """\
            #Table(test_schema.test_table)#
              ->groupBy(
                ~[id],
                ~['min(score)':{r | $r.score}:{c | $c->min()}, 'sum(score)':{r | $r.score}:{c | $c->sum()}]
              )"""
        )

        

# class TestGroupbyForCoverage:
