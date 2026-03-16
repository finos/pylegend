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

Window Functions
----------------

rank
~~~~

.. automethod:: pylegend.core.language.pandas_api.pandas_api_series.Series.rank
