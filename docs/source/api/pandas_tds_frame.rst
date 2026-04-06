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

Pandas TDS Frame
================

The ``PandasApiTdsFrame`` class provides a Pandas-like interface for working with TDS (Tabular Data Store) frames.
It offers methods for data manipulation, filtering, aggregation, joins, and window functions.

head
~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.head

truncate
~~~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.truncate

sort_values
~~~~~~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.sort_values

filter
~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.filter

drop
~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.drop

assign
~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.assign

apply
~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.apply

iloc
~~~~

.. autoproperty:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.iloc

loc
~~~

.. autoproperty:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.loc

merge
~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.merge

join
~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.join

rename
~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.rename

aggregate
~~~~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.aggregate

agg
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.agg

sum
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.sum

mean
~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.mean

min
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.min

max
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.max

std
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.std

var
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.var

count
~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.count

groupby
~~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.groupby

.. note::

   The returned :class:`~pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame`
   object has its own set of aggregation and window methods whose signatures
   may differ from the frame-level equivalents. See :doc:`pandas_groupby_tds_frame`
   for the full API reference.

dropna
~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.dropna

fillna
~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.fillna

rank
~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.rank

shift
~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.shift

shape
~~~~~

.. autoproperty:: pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.shape
