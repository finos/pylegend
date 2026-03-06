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

Pandas Series
=============

.. autoclass:: pylegend.core.language.pandas_api.pandas_api_series.Series
   :no-members:

A ``Series`` is obtained by indexing a
:class:`~pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame`
with a column name.

Series Subclasses
-----------------

The type of ``Series`` returned depends on the column type:

.. list-table::
   :header-rows: 1
   :widths: 20 25 55

   * - Column type
     - Series subclass
     - Supported operations (examples)
   * - ``Integer``
     - ``IntegerSeries``
     - ``+``, ``-``, ``*``, ``/``, ``%``, comparisons, ``.abs()``
   * - ``Float``
     - ``FloatSeries``
     - Same as Integer
   * - ``Number``
     - ``NumberSeries``
     - Same as Integer
   * - ``String``
     - ``StringSeries``
     - ``+`` (concat), ``.upper()``, ``.lower()``, ``.len()``,
       ``.startswith()``, ``.contains()``, ``.replace()``,
       ``.parse_integer()``, ``.parse_float()``
   * - ``Date``
     - ``DateSeries``
     - ``.year()``, ``.month()``, ``.day()``
   * - ``DateTime``
     - ``DateTimeSeries``
     - ``.year()``, ``.month()``, ``.day()``, ``.hour()``,
       ``.minute()``, ``.second()``
   * - ``StrictDate``
     - ``StrictDateSeries``
     - ``.year()``, ``.month()``, ``.day()``
   * - ``Boolean``
     - ``BooleanSeries``
     - ``&``, ``|``, ``~``, comparisons (limited — Boolean
       columns are not yet fully supported in PURE)

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Retrieving a column returns a typed Series
    series = frame["Order Id"]
    type(series).__name__

    ship_series = frame["Ship Name"]
    type(ship_series).__name__

Creating and Transforming a Series
----------------------------------

Retrieve a column as a ``Series``, then apply element-wise
transformations. Transformations always return a **new** ``Series``;
the original column is never mutated.

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Arithmetic on an IntegerSeries
    doubled = frame["Order Id"] * 2
    doubled.head(5).to_pandas()

    # String methods on a StringSeries
    upper_name = frame["Ship Name"].upper()
    upper_name.head(5).to_pandas()

    # A Series can be executed directly as a single-column query
    name_length = frame["Ship Name"].len()
    name_length.head(5).to_pandas()

Assigning to the Frame
----------------------

Use bracket assignment (``frame["col"] = ...``) to write a
``Series``, constant, or callable back into the frame. If the column
already exists it is **overwritten**; otherwise a new column is
appended.

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Overwrite an existing column with a Series expression
    frame["Order Id"] = frame["Order Id"] + 1000
    frame.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Create a new column from a string transformation
    frame["Upper Ship"] = frame["Ship Name"].upper()
    frame.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Assign a constant value
    frame["Flag"] = 1
    frame.head(3).to_pandas()

    # Assign via a lambda (receives a TDS row)
    frame["Computed"] = lambda row: row.get_integer("Order Id") * 2
    frame.head(3).to_pandas()

.. warning::

    A ``Series`` can **only** be assigned to the frame it was derived
    from. Attempting to assign a ``Series`` from a different frame
    raises ``ValueError``:

    .. code-block:: python

        frame_a["col"] = frame_b["col"]  # ValueError!

    This restriction exists because pylegend builds a single SQL
    query tree — cross-frame column references cannot be resolved
    without an explicit join.

Window Functions on a Series
----------------------------

Certain OLAP window functions (currently ``rank()``) can be called on
a ``Series``. The result is a new ``Series`` that can be assigned
back to the frame:

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Append a rank column
    frame["Order Rank"] = frame["Order Id"].rank()
    frame.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Rank with options (percentage, descending)
    frame["Order Pct Rank"] = frame["Order Id"].rank(pct=True, ascending=False)
    frame.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Arithmetic combined with a window function
    frame["Rank Plus 5"] = frame["Order Id"].rank() + 5
    frame.head(5).to_pandas()

.. note::

    Only **one** window-function call is allowed per expression. If
    you need to combine multiple, split them into separate steps:

    .. code-block:: python

        # NOT supported — two rank calls in one expression
        frame["r"] = frame["col1"].rank() + frame["col2"].rank()

        # Supported — separate assignments
        frame["r"]  = frame["col1"].rank()
        frame["r"] += frame["col2"].rank()

    Applying a window function on a **computed** series expression is
    also not supported. Do the window call first, then add arithmetic:

    .. code-block:: python

        # NOT supported
        (frame["col"] + 5).rank()

        # Supported
        frame["col"].rank() + 5

Data Type Changes
-----------------

Some transformations change the ``Series`` type. For example,
calling ``.len()`` on a ``StringSeries`` returns an
``IntegerSeries``, and calling ``.parse_float()`` returns a
``FloatSeries``. After assigning back, subsequent reads reflect
the new type:

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # .len() converts StringSeries -> IntegerSeries
    name_len = frame["Ship Name"].len()
    type(name_len).__name__

    frame["Ship Name"] = frame["Ship Name"].len()
    type(frame["Ship Name"]).__name__

    frame.head(3).to_pandas()

Aggregation Methods
-------------------

All Series aggregation methods return a **single-row**
:class:`~pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame`
(not a scalar, unlike pandas). Use ``.to_pandas()`` on the result to
obtain a ``pandas.DataFrame`` with the aggregated value.

.. note::

    Aggregation on a **computed** Series expression is not supported.
    Assign the expression to the frame first, then aggregate:

    .. code-block:: python

        # NOT supported
        (frame["col"] + 5).sum()

        # Supported
        frame["new_col"] = frame["col"] + 5
        frame["new_col"].sum()

aggregate
~~~~~~~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_series.Series.aggregate

agg
~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_series.Series.agg

sum
~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_series.Series.sum

mean
~~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_series.Series.mean

min
~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_series.Series.min

max
~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_series.Series.max

std
~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_series.Series.std

var
~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_series.Series.var

count
~~~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_series.Series.count

Here are additional end-to-end examples showing both standalone Series
execution and frame-level usage:

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Standalone Series aggregation — returns a single-row frame
    frame["Order Id"].sum().to_pandas()

    frame["Order Id"].count().to_pandas()

    frame["Order Id"].min().to_pandas()

    frame["Order Id"].max().to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Multiple aggregations via a list
    frame["Order Id"].aggregate(["sum", "mean", "min", "max"]).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Aggregate on a string column (lexicographic min/max)
    frame["Ship Name"].min().to_pandas()

    frame["Ship Name"].max().to_pandas()

Window Functions
----------------

rank
~~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_series.Series.rank

Here are additional end-to-end examples of ``rank()`` on a Series:

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Execute a ranked Series directly (standalone, single-column result)
    ranked_series = frame["Order Id"].rank()
    ranked_series.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Assign a rank column back to the frame
    frame["Order Rank"] = frame["Order Id"].rank()
    frame.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Dense rank, descending
    frame["Dense Rank"] = frame["Order Id"].rank(method="dense", ascending=False)
    frame.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Arithmetic with a ranked Series
    frame["Rank Plus 10"] = frame["Order Id"].rank() + 10
    frame.head(5).to_pandas()

.. ipython:: python

    import pylegend
    frame = pylegend.samples.pandas_api.northwind_orders_frame()

    # Combining multiple rank calls — must be separate assignments
    frame["Rank 1"] = frame["Order Id"].rank()
    frame["Rank 1"] += frame["Ship Name"].rank()
    frame.head(5).to_pandas()
