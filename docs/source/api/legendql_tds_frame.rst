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

LegendQL TDS Frame
==================

The ``LegendQLApiTdsFrame`` class provides a Python-like interface for working with TDS (Tabular Data Store) frames.
It offers methods for data manipulation, filtering, aggregation, joins, and window functions.

Row Selection Methods
---------------------

head
~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.head

limit
~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.limit

drop
~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.drop

slice
~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.slice

Column Selection Methods
------------------------

select
~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.select

distinct
~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.distinct

Sorting and Filtering
---------------------

sort
~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.sort

filter
~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.filter

Column Transformation Methods
-----------------------------

rename
~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.rename

extend
~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.extend

project
~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.project

Combining Frames
----------------

concatenate
~~~~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.concatenate

join
~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.join

inner_join
~~~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.inner_join

left_join
~~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.left_join

right_join
~~~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.right_join

Aggregation
-----------

group_by
~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.group_by

Window Functions
----------------

rows
~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.rows

range
~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.range

window
~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.window

window_extend
~~~~~~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_tds_frame.LegendQLApiTdsFrame.window_extend
