import numpy as np
from pylegend._typing import (
    PyLegendSequence,
    PyLegendUnion,
    PyLegendList,
    PyLegendCallable,
    PyLegendMapping,
    PyLegendHashable,
)
from pylegend.core.language.shared.primitives.primitive import PyLegendPrimitiveOrPythonPrimitive

__all__: PyLegendSequence[str] = [
    "PyLegendAggFunc",
    "PyLegendAggList",
    "PyLegendAggDict",
    "PyLegendAggInput",
]


PyLegendAggFunc = PyLegendUnion[  # type: ignore[explicit-any]
    PyLegendCallable[..., PyLegendPrimitiveOrPythonPrimitive],
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
