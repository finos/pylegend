from __future__ import annotations
from dataclasses import dataclass
from pyarrow import Table


@dataclass
class Result:
    model: str
    relation: str
    sql: str
    data: Table
