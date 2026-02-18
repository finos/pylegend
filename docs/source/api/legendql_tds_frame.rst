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

The ``LegendQLApiBaseTdsFrame`` class provides a Python-like interface for working with TDS (Tabular Data Store) frames.
It offers methods for data manipulation, filtering, aggregation, joins, and window functions.

Row Selection Methods
---------------------

head
~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.head

limit
~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.limit

drop
~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.drop

slice
~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.slice

Column Selection Methods
------------------------

select
~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.select

distinct
~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.distinct

Sorting and Filtering
---------------------

sort
~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.sort

filter
~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.filter

Column Transformation Methods
-----------------------------

rename
~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.rename

extend
~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.extend

project
~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.project

Combining Frames
----------------

concatenate
~~~~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.concatenate

join
~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.join

inner_join
~~~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.inner_join

left_join
~~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.left_join

right_join
~~~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.right_join

Aggregation
-----------

group_by
~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.group_by

Window Functions
----------------

rows
~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.rows

range
~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.range

window
~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.window

window_extend
~~~~~~~~~~~~~

.. automethod:: pylegend.core.tds.legendql_api.frames.legendql_api_base_tds_frame.LegendQLApiBaseTdsFrame.window_extend