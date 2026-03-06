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

Pandas Groupby TDS Frame
=========================

.. autoclass:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame
   :no-members:

The ``PandasApiGroupbyTdsFrame`` class is returned by
:meth:`PandasApiTdsFrame.groupby <pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.groupby>`
and provides methods for applying aggregation and window operations within each group.
The groupby columns also serve as the ``PARTITION BY`` clause for OLAP window functions such as ``rank``.

Aggregation Methods
-------------------

aggregate
~~~~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.aggregate

agg
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.agg

sum
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.sum

mean
~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.mean

min
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.min

max
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.max

std
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.std

var
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.var

count
~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.count

