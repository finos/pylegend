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
========================

The ``PandasApiGroupbyTdsFrame`` class is returned by
:meth:`PandasApiTdsFrame.groupby <pylegend.core.tds.pandas_api.frames.pandas_api_tds_frame.PandasApiTdsFrame.groupby>`
and provides methods for applying aggregation and window operations within each group.
The groupby columns also serve as the ``PARTITION BY`` clause for OLAP window functions such as ``rank``.

agg
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.agg

aggregate
~~~~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.aggregate

count
~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.count

cume_dist_legend_ext
~~~~~~~~~~~~~~~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.cume_dist_legend_ext

expanding
~~~~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.expanding

max
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.max

mean
~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.mean

median
~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.median

min
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.min

ntile_legend_ext
~~~~~~~~~~~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.ntile_legend_ext

rank
~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.rank

rolling
~~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.rolling

shift
~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.shift

std
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.std

sum
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.sum

var
~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.var

window_frame_legend_ext
~~~~~~~~~~~~~~~~~~~~~~~

.. automethod:: pylegend.core.tds.pandas_api.frames.pandas_api_groupby_tds_frame.PandasApiGroupbyTdsFrame.window_frame_legend_ext
