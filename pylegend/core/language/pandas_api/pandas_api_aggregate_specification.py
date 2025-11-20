import numpy as np
from pylegend._typing import (
    PyLegendSequence,
    PyLegendUnion,
    PyLegendList,
    PyLegendCallable,
    PyLegendMapping,
    PyLegendHashable,
)
from pylegend.core.language.shared.primitive_collection import PyLegendPrimitiveCollection
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitive

__all__: PyLegendSequence[str] = [
    "PyLegendAggFunc",
    "PyLegendAggList",
    "PyLegendAggDict",
    "PyLegendAggInput",
]


PyLegendAggFunc = PyLegendUnion[
    PyLegendCallable[[PyLegendPrimitiveCollection], PyLegendPrimitive],
    str,
    np.ufunc,
]

PyLegendAggList = PyLegendList[PyLegendAggFunc]

PyLegendAggDict = PyLegendMapping[
    PyLegendHashable,
    PyLegendUnion[
        PyLegendAggFunc,
        PyLegendAggList
    ]
]

PyLegendAggInput = PyLegendUnion[
    None,
    PyLegendAggFunc,
    PyLegendAggList,
    PyLegendAggDict,
]
