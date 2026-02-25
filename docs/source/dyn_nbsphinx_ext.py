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

import os
import re
import ast
import nbformat
from nbformat.v4 import new_notebook, new_code_cell
from nbconvert.preprocessors import ExecutePreprocessor
from jupyter_client import KernelManager
from tqdm import tqdm
from sphinx.util import logging

logger = logging.getLogger(__name__)

def builder_inited_hook(app):
    src_dir = os.path.abspath(os.path.join(app.srcdir, '../../pylegend'))
    out_dir = os.path.join(app.srcdir, 'tmp_notebooks')
    os.makedirs(out_dir, exist_ok=True)
    app.config.dyn_notebook_dir = out_dir
    
    try:
        tasks = []
        for root, _, files in os.walk(src_dir):
            for file in files:
                if file.endswith('.py'):
                    path = os.path.join(root, file)
                    try:
                        with open(path, 'r', encoding='utf-8') as f:
                            content = f.read()
                        tree = ast.parse(content)
                    except Exception:
                        continue
                    
                    for node in ast.walk(tree):
                        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
                            doc = ast.get_docstring(node)
                            if doc and '.. ipython:: python' in doc:
                                code_lines = []
                                in_block = False
                                for line in doc.split('\n'):
                                    if '.. ipython:: python' in line:
                                        in_block = True
                                        continue
                                    if in_block:
                                        if not line.strip() and not code_lines:
                                            continue
                                        if not line.strip():
                                            code_lines.append("")
                                            continue
                                            
                                        if line.startswith('    '):
                                            code_lines.append(line[4:])
                                        elif line.startswith('\t'):
                                            code_lines.append(line[1:])
                                        else:
                                            in_block = False
                                            
                                while code_lines and not code_lines[-1].strip():
                                    code_lines.pop()
                                    
                                if code_lines:
                                    tasks.append((node.name, code_lines))
                                    
        if tasks:
            ep = ExecutePreprocessor(timeout=600)
            
            # Filter tasks: only run those that don't exist or have different source code
            tasks_to_run = []
            for name, code_lines in tasks:
                nb_path = os.path.join(out_dir, f"{name}.ipynb")
                should_run = True
                if os.path.exists(nb_path):
                    try:
                        with open(nb_path, 'r', encoding='utf-8') as f:
                            existing_nb = nbformat.read(f, as_version=4)
                        
                        existing_source = ""
                        for cell in existing_nb.cells:
                            if cell.cell_type == 'code':
                                existing_source += cell.source.strip() + "\n\n"
                        
                        new_source = ""
                        for block in re.split(r'\n\s*\n', '\n'.join(code_lines)):
                            if block.strip():
                                new_source += block.strip() + "\n\n"
                        
                        if existing_source.strip() == new_source.strip():
                            should_run = False
                    except Exception:
                        pass
                
                if should_run:
                    tasks_to_run.append((name, code_lines))

            if not tasks_to_run:
                logger.info("[nbsphinx] All notebooks are up to date. Skipping execution.")
                return

            km = KernelManager(kernel_name='python3')
            km.start_kernel()
            try:
                # Disable logs inside the kernel
                setup_nb = new_notebook()
                setup_code = (
                    "import logging\n"
                    "logging.getLogger('pylegend').setLevel(logging.CRITICAL)\n"
                    "logging.getLogger('testcontainers').setLevel(logging.CRITICAL)"
                )
                setup_nb.cells.append(new_code_cell(setup_code))
                try:
                    ep.preprocess(setup_nb, {'metadata': {'path': out_dir}}, km=km)
                except Exception:
                    pass
                    
                for name, code_lines in tqdm(tasks_to_run, desc="[nbsphinx] Generating notebooks"):
                    nb = new_notebook()
                    code = '\n'.join(code_lines)
                    for block in re.split(r'\n\s*\n', code):
                        if block.strip():
                            nb.cells.append(new_code_cell(block.strip()))
                    
                    try:
                        ep.preprocess(nb, {'metadata': {'path': out_dir}}, km=km)
                    except Exception as e:
                        logger.error(f"[nbsphinx] Error executing notebook {name}: {e}")
                        
                    for cell in nb.cells:
                        if cell.cell_type == 'code':
                            cell.execution_count = 1
                            cell.metadata = {} # Clear execution metadata
                            filtered_outputs = []
                            for out in cell.outputs:
                                if 'execution_count' in out:
                                    out['execution_count'] = 1
                                if out.output_type == 'stream':
                                    valid_lines = []
                                    for l in out.text.split('\n'):
                                        # Filter out testcontainer/low-level logs
                                        if any(x in l for x in ['pylegend.samples.local_legend_env', 'testcontainers', 'Pulling image ', 'Container started: ', 'Waiting for container ']):
                                            continue
                                        valid_lines.append(l)
                                    if not valid_lines or all(not l.strip() for l in valid_lines):
                                        continue
                                    out.text = '\n'.join(valid_lines)
                                filtered_outputs.append(out)
                            cell.outputs = filtered_outputs
                    
                    nb_path = os.path.join(out_dir, f"{name}.ipynb")
                    nb.metadata = {'nbsphinx': {'orphan': True}}
                    nb_content = nbformat.writes(nb)
                    
                    # Double check if content actually changed before writing
                    should_write = True
                    if os.path.exists(nb_path):
                        with open(nb_path, 'r', encoding='utf-8') as f:
                            if f.read().strip() == nb_content.strip():
                                should_write = False
                    
                    if should_write:
                        with open(nb_path, 'w', encoding='utf-8') as f:
                            f.write(nb_content)
            finally:
                km.shutdown_kernel()
    except Exception as e:
        logger.error(f"Error in builder_inited_hook: {e}")


def process_docstrings(app, what, name, obj, options, lines):
    new_lines = []
    has_block = False
    in_block = False
    block_indent = -1
    
    for line in lines:
        if not in_block:
            if '.. ipython:: python' in line:
                has_block = True
                in_block = True
                block_indent = len(line) - len(line.lstrip())
                short_name = name.split('.')[-1]
                indent = line[:block_indent]
                
                nb_path = os.path.join(app.config.dyn_notebook_dir, f"{short_name}.ipynb")
                
                new_lines.append(indent + f":download:`Download Interactive Notebook </tmp_notebooks/{short_name}.ipynb>`")
                new_lines.append("")
                
                if os.path.exists(nb_path):
                    with open(nb_path, 'r', encoding='utf-8') as f:
                        nb = nbformat.read(f, as_version=4)
                    
                    for cell in nb.cells:
                        if cell.cell_type == 'code':
                            new_lines.append(indent + ".. code-block:: python")
                            new_lines.append("")
                            for l in cell.source.split('\n'):
                                new_lines.append(indent + "    " + l)
                            new_lines.append("")
                            
                            for out in cell.outputs:
                                if out.output_type == 'stream':
                                    if out.text.strip():
                                        new_lines.append(indent + ".. testoutput::")
                                        new_lines.append(indent + "    :options: +ELLIPSIS")
                                        new_lines.append("")
                                        for l in out.text.split('\n'):
                                            new_lines.append(indent + "    " + l)
                                        new_lines.append("")
                                elif out.output_type in ('execute_result', 'display_data'):
                                    if 'text/html' in out.data:
                                        if out.data['text/html'].strip():
                                            new_lines.append(indent + ".. raw:: html")
                                            new_lines.append("")
                                            for l in out.data['text/html'].split('\n'):
                                                new_lines.append(indent + "    " + l)
                                            new_lines.append("")
                                    elif 'text/plain' in out.data:
                                        if out.data['text/plain'].strip():
                                            new_lines.append(indent + ".. testoutput::")
                                            new_lines.append(indent + "    :options: +ELLIPSIS")
                                            new_lines.append("")
                                            for l in out.data['text/plain'].split('\n'):
                                                new_lines.append(indent + "    " + l)
                                            new_lines.append("")
            else:
                new_lines.append(line)
        else:
            if not line.strip():
                continue
            
            current_indent = len(line) - len(line.lstrip())
            if current_indent > block_indent:
                continue
            else:
                in_block = False
                new_lines.append(line)
            
    if has_block:
        lines[:] = new_lines

def setup(app):
    app.add_config_value('dyn_notebook_dir', '', 'env')
    app.connect('builder-inited', builder_inited_hook)
    app.connect('autodoc-process-docstring', process_docstrings)

