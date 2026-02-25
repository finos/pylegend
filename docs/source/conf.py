# Copyright 2026 Goldman Sachs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import os
import sys
from collections import namedtuple
import pylegend
import sphinx.util.inspect as ins


def isstaticmethod(obj, cls=None, name=None) -> bool:
    return False


def isabstractmethod(obj) -> bool:
    return False


ins.isabstractmethod = isabstractmethod
ins.isstaticmethod = isstaticmethod

namedtuple.__repr__ = lambda x: x.name

sys.path.insert(0, os.path.abspath('../../'))
sys.path.insert(0, os.path.abspath('.'))

if 'PYTHONSTARTUP' in os.environ:
    del os.environ['PYTHONSTARTUP']

project = 'PyLegend'
copyright = '''\
Copyright 2026 Goldman Sachs

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''
author = 'PyLegend Maintainers <legend@finos.org>'
release = pylegend.__version__

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.coverage',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx_rtd_theme',
    'sphinx.ext.todo',
    'sphinx.ext.githubpages',
    'sphinx.ext.doctest',
    'sphinx_autodoc_typehints',
    'IPython.sphinxext.ipython_console_highlighting',
    'IPython.sphinxext.ipython_directive',
    'nbsphinx',
    'dyn_nbsphinx_ext',
]

autoclass_content = 'init'
autodoc_default_options = {
    'member-order': 'bysource',
}
ipython_savefig_dir = '.'
nbsphinx_execute = 'never'

templates_path = ['./_templates']
exclude_patterns = []

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'alabaster'
html_static_path = ['./_static']
html_favicon = './_static/img/favicon.ico'
html_show_copyright = False
html_show_sphinx = False
# html_style = 'css/style.css'
html_output_encoding = 'ascii'
html_theme_options = {
    'canonical_url': True,
    'logo': "/img/logo.png",
    'logo_name': True,
    'logo_text_align': "center",
    "show_powered_by": False,
    "page_width": "1200px",
    "sidebar_width": "300px",
    "font_size": "17px",
    "fixed_sidebar": "true",
}

html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
    ]
}