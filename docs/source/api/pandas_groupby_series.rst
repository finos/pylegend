.. Copyright 2026 Goldman Sachs
..
.. Licensed under the Apache License, Version 2.0 (the "License");
.. you may not use this file except in compliance with the License.
.. You may obtain a copy of the License at
..
..      http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

Pandas Groupby Series
=====================

.. autoclass:: pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries
   :no-members:

A ``GroupbySeries`` is obtained by indexing a
:class:`~pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame`
with a **single** column name. It represents one column within a
grouped context and must have an operation (aggregation or window
function) applied before it can be executed.

GroupbySeries Subclasses
------------------------

The type of ``GroupbySeries`` returned depends on the column type:

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Column type
     - GroupbySeries subclass
     - Supported operations (examples)
   * - ``Integer``
     - ``IntegerGroupbySeries``
     - ``+``, ``-``, ``*``, ``/``, ``%``, comparisons, ``.abs()``
   * - ``Float``
     - ``FloatGroupbySeries``
     - Same as Integer
   * - ``Number``
     - ``NumberGroupbySeries``
     - Same as Integer
   * - ``String``
     - ``StringGroupbySeries``
     - ``+`` (concat), ``.upper()``, ``.lower()``, ``.len()``
   * - ``Date``
     - ``DateGroupbySeries``
     - ``.year()``, ``.month()``, ``.day()``
   * - ``DateTime``
     - ``DateTimeGroupbySeries``
     - ``.year()``, ``.month()``, ``.day()``, ``.hour()``
   * - ``StrictDate``
     - ``StrictDateGroupbySeries``
     - ``.year()``, ``.month()``, ``.day()``
   * - ``Boolean``
     - ``BooleanGroupbySeries``
     - ``&``, ``|``, ``~`` (limited — Boolean columns are not yet
       fully supported in PURE)

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Indexing with a single string returns a typed GroupbySeries
    gseries = frame.groupby("Ship Name")["Order Id"]
    type(gseries).__name__

    # Indexing with a list returns a narrowed PandasApiGroupbyTdsFrame
    gframe = frame.groupby("Ship Name")[["Order Id"]]
    type(gframe).__name__

Obtaining a GroupbySeries vs. a narrowed GroupbyFrame
-----------------------------------------------------

When you pass a **single string** to bracket notation on a groupby
object, you get a ``GroupbySeries``. When you pass a **list** of
strings, you get a narrowed ``PandasApiGroupbyTdsFrame``. Both
support the same aggregation methods, but only a ``GroupbySeries``
can be assigned back to the parent frame (for window functions like
``rank()``).

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # GroupbySeries — single column selection
    frame.groupby("Ship Name")["Order Id"].sum().head(5).to_pandas()

    # Narrowed GroupbyFrame — list selection (same result)
    frame.groupby("Ship Name")[["Order Id"]].sum().head(5).to_pandas()

A Bare GroupbySeries Cannot Be Executed
---------------------------------------

A ``GroupbySeries`` without an applied function (aggregation or
``rank()``) **cannot be executed**. Attempting to do so raises
``RuntimeError``:

.. code-block:: python

    gseries = frame.groupby("grp")["col"]
    gseries.to_sql_query()  # RuntimeError!

You must call an operation first:

.. code-block:: python

    gseries.sum()           # OK — aggregation
    gseries.rank()          # OK — window function

Grouped Aggregation via GroupbySeries
-------------------------------------

Call an aggregation method on a ``GroupbySeries`` to reduce each
group to a single value. The result is a
:class:`~pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame`
with the grouping columns and the aggregated column(s).

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Single aggregation
    frame.groupby("Ship Name")["Order Id"].count().head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Multiple aggregations via a list
    frame.groupby("Ship Name")["Order Id"].aggregate(
        ["min", "max"]
    ).head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Lambda aggregation
    frame.groupby("Ship Name")["Order Id"].aggregate(
        lambda x: x.average()
    ).head(5).to_pandas()

Assigning a GroupbySeries to the Frame
--------------------------------------

A ``GroupbySeries`` with an applied window function (e.g.
``rank()``) can be assigned back to the parent frame using bracket
assignment. This is the primary way to add grouped window-function
columns:

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Assign a grouped rank
    frame["Order Rank"] = frame.groupby(
        "Ship Name"
    )["Order Id"].rank()
    frame.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Percentage rank, descending
    frame["Pct Rank"] = frame.groupby(
        "Ship Name"
    )["Order Id"].rank(pct=True, ascending=False)
    frame.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Multiple rank calls — must be separate assignments
    frame["Rank 1"] = frame.groupby("Ship Name")["Order Id"].rank()
    frame["Rank 1"] += frame["Ship Name"].rank()
    frame.head(5).to_pandas()

.. warning::

    The assignment **must** target the same frame that was grouped.
    pylegend builds a single SQL query tree — cross-frame column
    references cannot be resolved without an explicit join.

.. note::

    Only **one** window-function call is allowed per expression. If
    you need to combine multiple, split them into separate steps:

    .. code-block:: python

        # NOT supported — two rank calls in one expression
        frame["r"] = (
            frame.groupby("grp")["col1"].rank()
            + frame.groupby("grp")["col2"].rank()
        )

        # Supported — separate assignments
        frame["r"]  = frame.groupby("grp")["col1"].rank()
        frame["r"] += frame.groupby("grp")["col2"].rank()

    Calling ``rank()`` or an aggregation on a **computed**
    ``GroupbySeries`` expression is also not supported:

    .. code-block:: python

        # NOT supported
        (frame.groupby("grp")["col"] + 5).rank()
        (frame.groupby("grp")["col"] + 5).sum()

        # Supported
        frame.groupby("grp")["col"].rank() + 5

Aggregation Methods
-------------------

All ``GroupbySeries`` aggregation methods return a
:class:`~pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame`
with one row per group (not a scalar, unlike pandas). The grouping
columns are always included in the result alongside the aggregated
values. Use ``.to_pandas()`` on the result to obtain a
``pandas.DataFrame``.

.. note::

    Aggregation on a **computed** GroupbySeries expression is not
    supported. Call the aggregation directly:

    .. code-block:: python

        # NOT supported
        (frame.groupby("grp")["col"] + 5).sum()

        # Supported
        frame.groupby("grp")["col"].sum()

aggregate
~~~~~~~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries.aggregate

agg
~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries.agg

sum
~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries.sum

mean
~~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries.mean

min
~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries.min

max
~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries.max

std
~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries.std

var
~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries.var

count
~~~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries.count

Here are additional end-to-end examples showing both standalone
GroupbySeries execution and frame-level usage:

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Standalone GroupbySeries aggregation — returns grouped results
    frame.groupby("Ship Name")["Order Id"].sum().head(5).to_pandas()

    frame.groupby("Ship Name")["Order Id"].count().head(5).to_pandas()

    frame.groupby("Ship Name")["Order Id"].min().head(5).to_pandas()

    frame.groupby("Ship Name")["Order Id"].max().head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Multiple aggregations via a list
    frame.groupby("Ship Name")["Order Id"].aggregate(
        ["sum", "mean", "min", "max"]
    ).head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Lambda aggregation
    frame.groupby("Ship Name")["Order Id"].aggregate(
        lambda x: x.mean()
    ).head(5).to_pandas()

Window Functions
----------------

rank
~~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_groupby_series.GroupbySeries.rank

Here are additional end-to-end examples of ``rank()`` on a
``GroupbySeries``:

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Execute a grouped ranked series directly (standalone)
    ranked = frame.groupby("Ship Name")["Order Id"].rank()
    ranked.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Assign a grouped rank back to the frame
    frame["Order Rank"] = frame.groupby(
        "Ship Name"
    )["Order Id"].rank()
    frame.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Dense rank, descending
    frame["Dense Rank"] = frame.groupby(
        "Ship Name"
    )["Order Id"].rank(method="dense", ascending=False)
    frame.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Arithmetic with a grouped ranked series
    frame["Rank Plus 10"] = frame.groupby(
        "Ship Name"
    )["Order Id"].rank() + 10
    frame.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Percentage rank
    frame["Pct Rank"] = frame.groupby(
        "Ship Name"
    )["Order Id"].rank(pct=True)
    frame.head(5).to_pandas()
