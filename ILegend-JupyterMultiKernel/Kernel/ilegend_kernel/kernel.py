from ipykernel.kernelbase import Kernel
from jupyter_client import KernelManager
from queue import Empty
import traceback
import re
import textwrap
import ast
import requests
import datetime
import json
from ipykernel.kernelbase import Kernel
from .magics import CELL_MAGICS, LINE_MAGICS
from ipykernel.kernelbase import Kernel
import pandas as pd
import os
import re
from dash import Dash, html, dcc,Output,Input,State
from dash_ag_grid import AgGrid
import threading
import socket




class ILegendRouterKernel(Kernel):
    implementation         = "ilegend"
    implementation_version = "1.2"
    banner = "ILegend kernel: route to python3 when #kernel: python is used"
    language_info = {
        "name":           "ilegend",
        "file_extension": ".ilgd",
        "mimetype":       "text/x-ilegend",
        "codemirror_mode": "ilegend",
    }
    tables = []
    details = {}
    user_ns={}
    check = False



    def _launch_python_kernel(self):
        km = KernelManager(kernel_name="python3")
        km.start_kernel()
        kc = km.client()
        kc.start_channels()
        return {"manager": km, "client": kc}





    def _extract_kernel_choice(self, code: str) -> str:
        """Return 'python' only when first line starts '#kernel: python'."""
        lines = code.lstrip().splitlines()
        if lines and lines[0].lower().startswith("#kernel:"):
            if lines[0].split(":", 1)[1].strip().lower().startswith("python"):
                return "python"
        return ""




    def _strip_first_line(self,code):
        lines = code.splitlines()
        return "\n".join(lines[2:]) if (lines and lines[0].lower().startswith("#kernel:")) else code




    def to_json(self,df):
        return df.to_json(orient="records")    




    def get_columns(self):
        if self.tables == []:
            return
        for x in self.tables:
            response = requests.post("http://127.0.0.1:9095/api/server/execute",json={"line":"get_attributes " + "local::DuckDuckConnection."+x})
            output = response.json()
            self.details[x] = [y for y in output["attributes"]]




    def initiate(self):
        from IPython.display import HTML
        import threading, time
        from IPython.display import clear_output
        stop_event = threading.Event()
        def show_running_time():
            start = time.time()
            while not stop_event.is_set():
                elapsed = time.time() - start
                s = HTML(f"<div style='color: #AAAAAA;font-family:monospace;'>Kernel Warming up... {elapsed:.2f} seconds elapsed\n</div>")
                self.send_response(self.iopub_socket,
                    'display_data',
                    {
                        'data': {
                            'text/html': str(s.data)
                        },
                        'metadata': {}
                    }
                )
                self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                time.sleep(0.01)
        timer_thread = threading.Thread(target=show_running_time)
        timer_thread.start()
        try:
            response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": "get_tables " + "local::DuckDuckConnection"})
            output = response.json()
            self.tables = [x for x in output["tables"]]
            self.get_columns()
        finally:
            stop_event.set()
            timer_thread.join()




    def parse_db_output(self, text: str):
        lines = [line.strip() for line in text.strip().splitlines() if line.strip()]
        result = {
            'database': None,
            'tables': []
        }
        current_table = None
        inside_table = False
        for line in lines:
            if line.startswith("Database"):
                result['database'] = line.split("Database", 1)[1].strip()
            elif line.startswith("Table"):
                table_name = line.split("Table", 1)[1].strip()
                current_table = {'name': table_name, 'columns': []}
                inside_table = True
            elif line == ')':
                if inside_table and current_table:
                    result['tables'].append(current_table)
                    current_table = None
                    inside_table = False
            elif inside_table:
                if ',' in line:
                    line = line[:-1]  # remove trailing comma
                if ' ' in line:
                    col_name, col_type = line.split(None, 1)
                    current_table['columns'].append({'name': col_name, 'type': col_type})
        return result
    








    def render_database_ui(self,data):
        import html
        db_name = data.get("database", "Unknown")
        tables = data.get("tables", [])
        html_parts = [f"<details open><summary><b>Database: {html.escape(db_name)}</b></summary><div style='margin-left: 20px;'>"]
        for table in tables:
            table_name = table.get("name", "Unnamed Table")
            columns = table.get("columns", [])
            html_parts.append(f"<details><summary><b>Table: {html.escape(table_name)}</b></summary><div style='margin-left: 20px;'>")
            # Table of columns
            html_parts.append("<table border='1' style='border-collapse: collapse;'>")
            html_parts.append("<tr><th>Column Name</th><th>Type</th></tr>")
            for col in columns:
                col_name = html.escape(col.get("name", ""))
                col_type = html.escape(col.get("type", ""))
                html_parts.append(f"<tr><td>{col_name}</td><td>{col_type}</td></tr>")
            html_parts.append("</table></div></details>")
        html_parts.append("</div></details>")
        return "\n".join(html_parts)
    





    def start_aggrid_dash(self, df, port=None):
        if port is None:
            try:
                port = self.find_free_port()
            except Exception:
                return "No port Found"
        column_defs = [
            {"field": c, "headerName": c.capitalize(), "headerClass": "custom-header"}
            for c in df.columns
        ]
        app = Dash(__name__)
        app.index_string = """<!DOCTYPE html>
    <html>
    <head>
    {%metas%}
    <title>Data Viewer</title>
    {%favicon%}
    {%css%}
    <style>
        html,body{
        height:100%;margin:0;padding:0;background:transparent;
        font-family:'Segoe UI',Roboto,'Helvetica Neue',sans-serif;
        }

        /* Base theme container ------------------------------------------ */
        .ag-theme-balham{
        height:100vh;width:100vw;
        --ag-font-size:14px;
        --ag-font-family:'Segoe UI',Roboto,sans-serif;
        background-color:var(--ag-background-color); /* follow theme */
        }
        /* Ensure *all* nested wrappers inherit theme background */
        .ag-theme-balham,
        .ag-theme-balham .ag-root-wrapper,
        .ag-theme-balham .ag-root,
        .ag-theme-balham .ag-center-cols-viewport,
        .ag-theme-balham .ag-center-cols-container,
        .ag-theme-balham .ag-body-viewport{
        background-color:var(--ag-background-color)!important;
        }

        /* █ Light theme -------------------------------------------------- */
        .light .ag-theme-balham{
        --ag-background-color:#ffffff;
        --ag-foreground-color:#000;
        --ag-header-background-color:#f0f0f0;
        --ag-header-foreground-color:#000;
        --row-even:#ffffff;
        --row-odd:#fafafa;
        }
        .light .custom-header{
        background:#f0f0f0!important;color:#000!important;
        font-weight:bold;text-transform:uppercase;
        }

        /* █ Dark theme --------------------------------------------------- */
        .dark .ag-theme-balham{
        --ag-background-color:#1e1e2f;
        --ag-foreground-color:#fff;
        --ag-header-background-color:#001f3f;
        --ag-header-foreground-color:#fff;
        --row-even:#1e1e2f;
        --row-odd:#22223a;
        }
        .dark .custom-header{
        background:#001f3f!important;color:#fff!important;
        font-weight:bold;text-transform:uppercase;
        }

        /* Row striping & hover ------------------------------------------ */
        .ag-theme-balham .ag-row-even{background-color:var(--row-even);}
        .ag-theme-balham .ag-row-odd {background-color:var(--row-odd);}
        .dark .ag-theme-balham .ag-row-hover .ag-cell{
        background-color:#2e2e48!important; /* subtle dark hover */
        }

        /* First column style -------------------------------------------- */
        .ag-theme-balham .ag-row .ag-cell[col-id="0"]{
        color:#003d6f;font-weight:600;      /* navy blue field names */
        }

        /* Column‑menu icon colours -------------------------------------- */
        .ag-theme-balham .ag-header-cell-menu-button       {filter:invert(0%);}
        .ag-theme-balham .ag-header-cell-menu-button:hover {filter:invert(0%);}
        .dark .ag-theme-balham .ag-header-cell-menu-button       {filter:invert(100%);}
        .dark .ag-theme-balham .ag-header-cell-menu-button:hover {filter:invert(100%);}

        /* Theme toggle button ------------------------------------------- */
        .theme-toggle{
        position:absolute;top:8px;right:12px;z-index:30;
        background:#001f3f;color:#fff;border:none;border-radius:4px;
        padding:4px 8px;cursor:pointer;font-size:12px;
        }
        .light .theme-toggle{background:#f0f0f0;color:#000;}
    </style>
    </head>
    <body class="light">
    {%app_entry%}
    <footer>{%config%}{%scripts%}{%renderer%}</footer>
    </body>
    </html>"""
        app.layout = html.Div(
            [
                dcc.Store(id="theme-store", data="light"),
                html.Button("🌙 Dark mode", id="theme-btn", n_clicks=0,
                            className="theme-toggle"),
                AgGrid(
                    id="grid",
                    rowData=df.to_dict("records"),
                    columnDefs=column_defs,
                    defaultColDef={"sortable": True, "filter": True, "resizable": True},
                    columnSize="sizeToFit",
                    className="ag-theme-balham",
                ),
            ],
            id="page",
            style={"margin": 0, "padding": 0, "height": "100vh", "width": "100vw"},
        )
        @app.callback(
            Output("theme-store", "data"),
            Output("theme-btn", "children"),
            Input("theme-btn", "n_clicks"),
            State("theme-store", "data"),
            prevent_initial_call=True,
        )
        def toggle_theme(n, current):
            new_theme = "dark" if current == "light" else "light"
            btn_label = "☀️ Light mode" if new_theme == "dark" else "🌙 Dark mode"
            return new_theme, btn_label
        @app.callback(
            Output("page", "className"),
            Input("theme-store", "data"),
        )
        def apply_theme(theme):
            return theme
        threading.Thread(
            target=lambda: app.run(port=port, debug=False, use_reloader=False),
            daemon=True,
        ).start()
        return f"http://localhost:{port}"











    def find_free_port(self,start=8050):
        port = start
        while(True):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(('localhost', port))
                    return port
                except OSError:
                    port = port + 1




    def do_execute(self, code, silent, store_history=True, user_expressions=None, allow_stdin=False):
        if code.strip().startswith("%put"):
            return self._handle_put_command(code)
        kernel_choice = self._extract_kernel_choice(code)
        exec_code     = self._strip_first_line(code)
        if kernel_choice == "python":
            try:
                if not hasattr(self, "_python"):
                    self._python = self._launch_python_kernel()
                kc = self._python["client"]
                msg_id = kc.execute(exec_code)
                while True:
                    try:
                        msg = kc.get_iopub_msg(timeout=10)
                    except Empty:
                        break
                    if msg["parent_header"].get("msg_id") != msg_id:
                        continue
                    if msg["msg_type"] in {
                        "stream", "display_data", "execute_result",
                        "error", "clear_output"
                    }:
                        if msg["msg_type"] == "execute_result":
                            msg_type = "display_data"
                            content  = {
                                "data":     msg["content"]["data"],
                                "metadata": msg["content"].get("metadata", {}),
                            }
                        else:
                            msg_type = msg["msg_type"]
                            content  = msg["content"]
                        self.session.send(
                            self.iopub_socket, msg_type, content,
                            parent=self._parent_header
                        )
                    elif msg["msg_type"] == "status" and \
                         msg["content"].get("execution_state") == "idle":
                        break
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            except Exception as e:
                tb = traceback.format_exc()
                self.send_response(self.iopub_socket, "stream",
                                   {"name": "stderr",
                                    "text": f"[Router‑Error] {e}\n{tb}"})
                return {"status": "error",
                        "execution_count": self.execution_count,
                        "ename": type(e).__name__,
                        "evalue": str(e),
                        "traceback": tb.splitlines()}

        else:
            if(self.check == False):
                self.initiate()
                self.check = True
            magic_line, *cell_lines = code.splitlines()
            cell_code = "\n".join(cell_lines)


            if code.strip().startswith("start_legend"):
                import threading, time
                from IPython.display import clear_output
                from IPython.display import HTML
                stop_event = threading.Event()
                def show_running_time():
                    start = time.time()
                    while not stop_event.is_set():
                        elapsed = time.time() - start
                        s = HTML(f"<div style='color: #AAAAAA;font-family:monospace;'>Activating Legend Features... {elapsed:.2f} seconds elapsed\n</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                        time.sleep(0.01)
                    s = HTML(f"<div style='color: #AAAAAA;font-family:monospace;'>Legend Features Activated in - {elapsed:.2f}s\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                timer_thread = threading.Thread(target=show_running_time)
                timer_thread.start()
                connection_name = "local::DuckDuckConnection"
                try:
                    response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": "get_tables " + connection_name})
                finally:
                    stop_event.set()
                    timer_thread.join()
                output = response.json()
                self.tables = [x for x in output["tables"]]
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }





            elif code.strip().startswith("%%"):
                magic_name = magic_line[2:]
                if magic_name in CELL_MAGICS:
                    output = CELL_MAGICS[magic_name](cell_code)
                    stream_content = {'name': 'stdout', 'text': output}
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    return {
                        'status': 'ok',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    } 
                



                
            elif code.strip().startswith("%"):
                magic_line = code.strip().split()
                magic_name = magic_line[0][1:].strip()
                if magic_name == 'date':
                    t = datetime.datetime.now()
                    t = t.strftime("%Y-%m-%d %H:%M:%S")
                    stream_content = {'name': 'stdout', 'text': t}
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    return {
                        'status': 'ok',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                elif magic_name in LINE_MAGICS:
                    output = LINE_MAGICS[magic_name](magic_line[1])
                    stream_content = {'name': 'stdout', 'text': output}
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    return {
                        'status': 'ok',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
        

                



                
            elif code.strip().startswith("sql_to_json_line"):
                from IPython.display import HTML
                headers = {"Content-Type": "text/plain"}
                response = requests.post("http://127.0.0.1:9095/api/sql/v1/grammar/grammarToJson",data=cell_code, headers=headers)
                output = response.json()
                if("code" in output and output["code"]==-1):
                    s = HTML(f"<div style='color: red;'>Error: {output["message"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                stream_content = {'name': 'stdout', 'text': json.dumps(output, indent=2)}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            




            elif code.strip().startswith("sql_to_json_batch"):
                from IPython.display import HTML
                headers = {"Content-Type": "application/json"}
                queries = [q.strip() for q in cell_code.split(";") if q.strip()]
                payload ={
                    f"query{i+1}": {"value": query + ";"}
                    for i, query in enumerate(queries)
                }
                response = requests.post(
                    "http://127.0.0.1:9095/api/sql/v1/grammar/grammarToJson/batch",
                    data=json.dumps(payload),
                    headers=headers
                )
                output = response.json()
                if("code" in output and output["code"]==-1):
                    s = HTML(f"<div style='color: red;'>Error: {output["message"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                stream_content = {'name': 'stdout', 'text': json.dumps(output, indent=2)}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            



            elif code.strip().startswith("show_func_activators"):
                from IPython.display import HTML
                headers = {"Content-Type": "text/plain"}
                response = requests.get("http://127.0.0.1:9095/api/functionActivator/list")
                output = response.json()
                if("code" in output and output["code"]==-1):
                    s = HTML(f"<div style='color: red;'>Error: {output["message"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                stream_content = {'name': 'stdout', 'text': json.dumps(output, indent=2)}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            



            elif code.strip().startswith("create "):
                from IPython.display import HTML
                headers = {"Content-Type": "text/plain"}
                response = requests.post("http://127.0.0.1:9095/api/data/createtable",data=magic_line, headers=headers)
                output = response.json()
                if("error" in output):
                    s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                s='-----------------Table Created-----------------'+'\n' + "-----Ware House: "+output["warehouse"]+"-----"+'\n' + "-----DataBase: "+output["database"]+"-----"+'\n' + "-----Schema: "+output["schema"]+"-----"+'\n'
                s = s+ "-----Columns-----"+'\n'
                p = output["columns"]
                for column in p:
                    s = s + column["name"] + f"[{column["type"]}]"
                    if(column["primaryKey"]==True):
                        s = s + " is a Primary Key"+"\n"
                    else:
                        s = s+'\n'
                stream_content = {'name': 'stdout', 'text': s}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            





            
            elif code.strip().startswith("insertrow"):
                from IPython.display import HTML
                headers = {"Content-Type": "application/json"}
                if '->' not in cell_code:
                    s = HTML("<div style='color: red;'>Error: Improper usage of command</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                row_part, path_part = cell_code.split('->', 1)
                row_part = row_part.strip()
                path_part = path_part.strip()
                if not (row_part.startswith('[') and row_part.endswith(']')):
                    s = HTML("<div style='color: red;'>Error: Improper usage of command</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                row_part = row_part[1:-1].strip()
                row_dict = {}
                for pair in row_part.split(','):
                    if ':' not in pair:
                        s = HTML("<div style='color: red;'>Error: Improper usage of command</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        return {
                            'status': 'error',
                            'execution_count': self.execution_count,
                            'payload': [],
                            'user_expressions': {}
                        }
                    key, val = map(str.strip, pair.split(':', 1))
                    try:
                        parsed_val = ast.literal_eval(val)
                    except Exception:
                        parsed_val = val
                    row_dict[key] = parsed_val
                payload = {"path": path_part,"row": row_dict}
                response = requests.post("http://127.0.0.1:9095/api/data/insertrow",data=json.dumps(payload), headers=headers)
                output = response.json()
                if "error" in output:
                    s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                else:
                    s = 'Row Added.' + '\n' + '('
                    p = output["row"]
                    for fields in p:
                        s = s +  f"{fields}: {p[fields]}"  + ', '
                    s = s[0:len(s)-2] + ')'
                    stream_content = {'name': 'stdout', 'text': s}
                    self.send_response(self.iopub_socket, 'stream', stream_content)
                    return {
                        'status': 'ok',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                









            elif code.strip().startswith("delete_row"):
                from IPython.display import HTML
                headers = {"Content-Type": "text/plain"}
                response = requests.post("http://127.0.0.1:9095/api/data/deleterow", data=cell_code, headers=headers)
                output = response.json()
                if "error" in output:
                    s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                deleted = output["deletedRow"]
                s = 'Row deleted successfully:\n('
                for key, val in deleted.items():
                    s += f"{key}: {val}, "
                s = s.rstrip(", ") + ')'
                stream_content = {'name': 'stdout', 'text': s}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            









            elif code.strip().startswith("show_table"):
                from IPython.display import HTML
                headers = {"Content-Type": "text/plain"}
                response = requests.post("http://127.0.0.1:9095/api/data/fetchtable",data=cell_code,headers=headers)
                output = response.json()
                if "error" in output:
                    s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                try:
                    df = pd.DataFrame(output)
                except Exception as e:
                    s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                display_content = {
                    'data': {
                        'text/plain': str(df),
                        'text/html': df.to_html()
                    },
                    'metadata': {}
                }
                self.send_response(self.iopub_socket, 'display_data', display_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            





            elif code.strip().startswith("show_all_tables"):
                from IPython.display import HTML
                response = requests.get("http://127.0.0.1:9095/api/data/showtables")
                output = response.json()
                if "error" in output:
                    s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                s = (f"Number of tables: {output['count']}") + "\n"
                s = s + "Tables:" + "\n"
                for table in output['tables']:
                    s = s + table + '\n'
                stream_content = {'name': 'stdout', 'text': s}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            






            elif code.strip().startswith("load duckdb"):
                from IPython.display import HTML
                headers = {"Content-Type": "text/plain"}
                response = requests.post("http://127.0.0.1:9095/api/data/duckdb/load",data=cell_code, headers=headers)
                output = response.text
                stream_content = {'name': 'stdout', 'text': output}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            







            elif code.strip().startswith("query duckdb"):
                from IPython.display import HTML
                headers = {"Content-Type": "application/json"}
                magic_line_new = magic_line.split()
                payload = {
                    "dbPath": f"{magic_line_new[2]}",
                    "query": f"{cell_code}"
                }
                response = requests.post("http://127.0.0.1:9095/api/data/duckdb/query", data=json.dumps(payload), headers=headers)
                output = response.json()
                try:
                    df = pd.DataFrame(output)
                except Exception as e:
                    s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                display_content = {
                    'data': {
                        'text/plain': str(df),
                        'text/html': df.to_html()
                    },
                    'metadata': {}
                }
                self.send_response(self.iopub_socket, 'display_data', display_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                } 
            







            elif code.strip().startswith("load "):
                from IPython.display import HTML
                import threading, time
                from IPython.display import clear_output
                stop_event = threading.Event()
                def show_running_time():
                    start = time.time()
                    while not stop_event.is_set():
                        elapsed = time.time() - start
                        s = HTML(f"<div style='color: #AAAAAA;font-family:monospace;'>Loading csv data into table in DuckDB connection... {elapsed:.2f} seconds elapsed\n</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                        time.sleep(0.01)
                    s = HTML(f"<div style='color:  #AAAAAA;font-family:monospace;'>Total Time Taken - {elapsed:.2f}s\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )    
                timer_thread = threading.Thread(target=show_running_time)
                timer_thread.start()
                try:
                    response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
                    response2 = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": "get_tables " + "local::DuckDuckConnection"})
                    output2 = response2.json()
                    self.tables = [x for x in output2["tables"]]
                    self.get_columns()
                finally:
                    stop_event.set()
                    timer_thread.join()
                if(response.headers.get('Content-Type') == 'application/json'):
                    output = response.json()
                    s = HTML(f"<div style='color: red;'>{output["error"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                output = response.text
                stream_content = {'name': 'stdout', 'text': output}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            










            elif code.strip().startswith("db"):
                from IPython.display import HTML
                import threading, time
                from IPython.display import clear_output
                stop_event = threading.Event()
                def show_running_time():
                    start = time.time()
                    while not stop_event.is_set():
                        elapsed = time.time() - start
                        s = HTML(f"<div style='color: #AAAAAA;font-family:monospace;'>Exploring Database... {elapsed:.2f} seconds elapsed\n</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                        time.sleep(0.01)
                    s = HTML(f"<div style='color: #AAAAAA;font-family:monospace;'>Total Time Taken - {elapsed:.2f}s\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                timer_thread = threading.Thread(target=show_running_time)
                timer_thread.start()
                try:
                    response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
                finally:
                    stop_event.set()
                    timer_thread.join()
                if(response.headers.get('Content-Type') == 'application/json'):
                    output = response.json()
                    s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                output = response.text
                try:
                    structured = self.parse_db_output(output)
                    html_str = self.render_database_ui(structured)
                    self.send_response(self.iopub_socket, 'display_data', {
                        'data': {'text/html': html_str},
                        'metadata': {}
                    })
                    return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                    }
                except Exception as e:
                    from IPython.display import HTML
                    import html
                    s = HTML(f"<div style='color: red;'>Parsing/Rendering failed: {html.escape(str(e))}</div>")
                    self.send_response(self.iopub_socket, 'display_data', {
                        'data': {'text/html': str(s.data)},
                        'metadata': {}
                    })
                    return {
                    'status': 'error',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                    }
            











            elif code.strip().startswith("drop_all_tables"):
                from IPython.display import HTML
                import threading, time
                from IPython.display import clear_output
                stop_event = threading.Event()
                def show_running_time():
                    start = time.time()
                    while not stop_event.is_set():
                        elapsed = time.time() - start
                        s = HTML(f"<div style='color:  #AAAAAA;font-family:monospace;'>Dropping tables from Connection... {elapsed:.2f} seconds elapsed\n</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                        time.sleep(0.01)
                    s = HTML(f"<div style='color:  #AAAAAA;font-family:monospace;'>Total Time Taken - {elapsed:.2f}s\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                timer_thread = threading.Thread(target=show_running_time)
                timer_thread.start()
                try:
                    response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
                    response2 = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": "get_tables " + "local::DuckDuckConnection"})
                    output2 = response2.json()
                    self.tables = [x for x in output2["tables"]]
                    self.get_columns()
                finally:
                    stop_event.set()
                    timer_thread.join()
                if(response.headers.get('Content-Type') == 'application/json'):
                    output = response.json()
                    s = HTML(f"<div style='color: red;'>{output["error"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                output = response.text
                stream_content = {'name': 'stdout', 'text': output}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            














            elif code.strip().startswith("macro "):
                from IPython.display import HTML
                import threading, time
                from IPython.display import clear_output
                stop_event = threading.Event()
                def show_running_time():
                    start = time.time()
                    while not stop_event.is_set():
                        elapsed = time.time() - start
                        s = HTML(f"<div style='color:  #AAAAAA;font-family:monospace;'>Establishing Macro... {elapsed:.2f} seconds elapsed\n</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                        time.sleep(0.01)
                    s = HTML(f"<div style='color:  #AAAAAA;font-family:monospace;'>Macro Established in - {elapsed:.2f}s\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                timer_thread = threading.Thread(target=show_running_time)
                timer_thread.start()
                try:
                    response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
                finally:
                    stop_event.set()
                    timer_thread.join()
                output = response.text
                if(output.startswith("Invalid")):
                    s = HTML(f"<div style='color: red;'>Error: {output}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                stream_content = {'name': 'stdout', 'text': output}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            










            elif code.strip().startswith("show_macros"):
                from IPython.display import HTML
                import threading, time
                from IPython.display import clear_output
                stop_event = threading.Event()
                def show_running_time():
                    start = time.time()
                    while not stop_event.is_set():
                        elapsed = time.time() - start
                        s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>Fetching all Macros... {elapsed:.2f} seconds elapsed\n</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                        time.sleep(0.01)
                    s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>Macros fetched in- {elapsed:.2f}s\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                timer_thread = threading.Thread(target=show_running_time)
                timer_thread.start()
                try:
                    response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
                finally:
                    stop_event.set()
                    timer_thread.join()
                output = response.text
                stream_content = {'name': 'stdout', 'text': output}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            













            
            elif code.strip().startswith("clear_macros"):
                from IPython.display import HTML
                import threading, time
                from IPython.display import clear_output
                stop_event = threading.Event()
                def show_running_time():
                    start = time.time()
                    while not stop_event.is_set():
                        elapsed = time.time() - start
                        s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>Clearing all Macros... {elapsed:.2f} seconds elapsed\n</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                        time.sleep(0.01)
                    s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>Macros cleared in- {elapsed:.2f}s\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                timer_thread = threading.Thread(target=show_running_time)
                timer_thread.start()
                try:
                    response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
                finally:
                    stop_event.set()
                    timer_thread.join()
                output = response.text
                stream_content = {'name': 'stdout', 'text': output}
                self.send_response(self.iopub_socket, 'stream', stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            




            elif code.strip().startswith("get_tables "):
                from IPython.display import HTML
                import threading, time
                from IPython.display import clear_output
                stop_event = threading.Event()
                def show_running_time():
                    start = time.time()
                    while not stop_event.is_set():
                        elapsed = time.time() - start
                        s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>Fetching tables... {elapsed:.2f} seconds elapsed\n</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                        time.sleep(0.01)
                    s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>Tables Fetched in - {elapsed:.2f}s\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                timer_thread = threading.Thread(target=show_running_time)
                timer_thread.start()
                try:
                    response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
                finally:
                    stop_event.set()
                    timer_thread.join()
                output = response.json()
                s = ""
                for x in output["tables"]:
                    s = s+x+"\n"
                stream_content = {'name': 'stdout', 'text': s}
                self.send_response(self.iopub_socket, 'stream',stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            



            
            elif code.strip().startswith("get_attributes "):
                from IPython.display import HTML
                import threading, time
                from IPython.display import clear_output
                stop_event = threading.Event()
                def show_running_time():
                    start = time.time()
                    while not stop_event.is_set():
                        elapsed = time.time() - start
                        s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>Fetching attributes... {elapsed:.2f} seconds elapsed\n</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                        time.sleep(0.01)
                    s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>Attributes Fetched in - {elapsed:.2f}s\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                timer_thread = threading.Thread(target=show_running_time)
                timer_thread.start()
                try:
                    response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
                finally:
                    stop_event.set()
                    timer_thread.join()
                output = response.json()
                s=""
                for x in output["attributes"]:
                    s = s + x + "\n"
                stream_content = {'name': 'stdout', 'text': s}
                self.send_response(self.iopub_socket, 'stream',stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }
            



            elif code.strip().startswith("get_all"):
                from IPython.display import HTML
                import threading, time
                from IPython.display import clear_output
                stop_event = threading.Event()
                def show_running_time():
                    start = time.time()
                    while not stop_event.is_set():
                        elapsed = time.time() - start
                        s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>Fetching attributes... {elapsed:.2f} seconds elapsed\n</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                        time.sleep(0.01)
                    s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>Attributes Fetched in - {elapsed:.2f}s\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                timer_thread = threading.Thread(target=show_running_time)
                timer_thread.start()
                try:
                    response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": magic_line})
                finally:
                    stop_event.set()
                    timer_thread.join()
                output = response.json()
                s=""
                for x in output["connections"]:
                    s = s + x + "\n"
                stream_content = {'name': 'stdout', 'text': s}
                self.send_response(self.iopub_socket, 'stream',stream_content)
                return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                }







            elif code.strip().startswith("#>"):
                code = code.replace('\n', '')
                match1 = re.search(r'(.*?)--file\s+(\w+)\s*$', code.strip())
                match2 = re.search(r'(.*?)--var\s+(\w+)\s*$', code.strip())
                if match1:
                    query_part = match1.group(1).strip()
                    filename = match1.group(2).strip()
                    variable = None
                elif match2:
                    query_part = match2.group(1).strip()
                    variable = match2.group(2).strip()
                    filename = None
                else:
                    query_part = code.strip()
                    variable = None
                    filename = None
                from IPython.display import HTML
                import threading, time
                from IPython.display import clear_output
                stop_event = threading.Event()
                def show_running_time():
                    start = time.time()
                    while not stop_event.is_set():
                        elapsed = time.time() - start
                        s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>Data Quereying... {elapsed:.2f} seconds elapsed\n</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        self.send_response(self.iopub_socket, 'clear_output', {'wait': True})
                        time.sleep(0.01)
                    s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>Total Time Taken - {elapsed:.2f}s\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                timer_thread = threading.Thread(target=show_running_time)
                timer_thread.start()
                try:
                    response = requests.post("http://127.0.0.1:9095/api/server/execute", json={"line": query_part})
                finally:
                    stop_event.set()
                    timer_thread.join()
                output = response.json()
                try:
                    df = pd.DataFrame(output)
                except Exception:
                    s = HTML(f"<div style='color: red;'>Error: {output["error"]}</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                if filename!=None:
                    df.to_csv(filename,index=False)
                    s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>DataFrame saved in the file - {filename}\n</div>")
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                    }
                elif variable!=None:
                    self._python = self._launch_python_kernel()
                    python_kc = self._python["client"]
                    json_string = df.to_json(orient='split') + "\n"
                    escaped_json = repr(json_string)
                    import textwrap
                    escaped_json = repr(json_string)
                    inject_code = textwrap.dedent(f"""
                        import pandas as pd
                        import io
                        {variable} = pd.read_json(io.StringIO({escaped_json}), orient='split')
                    """)
                    msg_id = python_kc.execute(inject_code)
                    # Wait until Python kernel finishes executing
                    while True:
                        msg = python_kc.get_iopub_msg(timeout=5)
                        if msg['parent_header'].get('msg_id') != msg_id:
                            continue
                        if msg['msg_type'] == 'status' and msg['content'].get('execution_state') == 'idle':
                            break
                    s = HTML(f"<div style='color:#AAAAAA;font-family:monospace;'>DataFrame variable '{variable}' stored in the Python Environment</div>")
                    self.user_ns[variable] = df
                    self.send_response(self.iopub_socket,
                        'display_data',
                        {
                            'data': {
                                'text/html': str(s.data)
                            },
                            'metadata': {}
                        }
                    )
                    return {
                        'status': 'ok',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                    }
                else:
                    if(self.start_aggrid_dash(df)=="No port Found"):
                        s = HTML(f"<div style='color:  red;'>AgGrid haven't generated. Please try again</div>")
                        self.send_response(self.iopub_socket,
                            'display_data',
                            {
                                'data': {
                                    'text/html': str(s.data)
                                },
                                'metadata': {}
                            }
                        )
                        return {
                        'status': 'error',
                        'execution_count': self.execution_count,
                        'payload': [],
                        'user_expressions': {}
                        }
                    url = self.start_aggrid_dash(df)
                    html_output = f'''
                        <iframe src="{url}" 
                                width="100%" 
                                height="450" 
                                style="border: none;"></iframe>
                    '''
                    self.send_response(self.iopub_socket, 'display_data', {
                        'data': {
                            'text/html': html_output,
                            'text/plain': str(df)
                        },
                        'metadata': {}
                    })
                    return {
                    'status': 'ok',
                    'execution_count': self.execution_count,
                    'payload': [],
                    'user_expressions': {}
                    }


    def do_complete(self, code, cursor_pos):
        kernel_choice = self._extract_kernel_choice(code)
        if kernel_choice == "python":
            code_without_header = self._strip_first_line(code)
            lines = code.splitlines()
            adjustment = sum(len(line) + 1 for line in lines[:2]) if (lines and lines[0].lower().startswith("#kernel:")) else 0
            adjusted_cursor = max(cursor_pos - adjustment, 0)
            try:
                if not hasattr(self, "_python"):
                    self._python = self._launch_python_kernel()
                kc = self._python["client"]
            except Exception:
                return {
                    'status': 'ok',
                    'matches': [],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                }
            msg_id = kc.complete(code_without_header, adjusted_cursor)
            while True:
                try:
                    msg = kc.get_shell_msg(timeout=5)
                except Empty:
                    break
                if msg['parent_header'].get('msg_id') != msg_id:
                    continue
                if msg['msg_type'] == 'complete_reply':
                    content = msg['content']
                    # Adjust back to original cursor positions
                    content['cursor_start'] += adjustment
                    content['cursor_end'] += adjustment
                    return content
            return {
                'status': 'ok',
                'matches': [],
                'cursor_start': cursor_pos,
                'cursor_end': cursor_pos,
                'metadata': {},
            }
        else:
            suggestions = ["load ", "db ", "#>"]
            if code is None or code.strip() == "":
                start = len(prefix)
                return {
                    'matches': suggestions,
                    'cursor_start': start,
                    'cursor_end': start,
                    'metadata': {},
                    'status': 'ok'
                }
            prefix = code[:cursor_pos]
            tokens = prefix.strip().split()
            if tokens and tokens[0] == "load" and prefix.rstrip() == "load":
                match = "~/"
                start = len(prefix)
                return {
                    'matches': [match],
                    'cursor_start': start,
                    'cursor_end': start,
                    'metadata': {},
                    'status': 'ok'
                }
            if prefix.startswith("load ~/"):
                path_prefix = prefix[len("load "):]
                if path_prefix.endswith('/'):
                    dir_path = os.path.expanduser(path_prefix)
                    partial = ''
                else:
                    dir_path, partial = os.path.split(os.path.expanduser(path_prefix))
                try:
                    entries = os.listdir(dir_path)
                except Exception:
                    entries = []
                matches = [e for e in entries if e.startswith(partial)]
                matches = [
                    e + '/' if os.path.isdir(os.path.join(dir_path, e)) else e
                    for e in matches
                ]
                if not matches:
                    matches = [
                        e + '/' if os.path.isdir(os.path.join(dir_path, e)) else e
                        for e in entries
                    ]
                completed_file = (
                    len(matches) == 1 and
                    not matches[0].endswith('/') and
                    os.path.isfile(os.path.join(dir_path, matches[0]))
                )
                if completed_file:
                    matches = [' local::DuckDuckConnection']
                    cursor_start = cursor_end = cursor_pos
                else:
                    cursor_start = len("load ") + len(path_prefix) - len(partial)
                return {
                    'matches': matches,
                    'cursor_start': cursor_start,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            if prefix.endswith("#>"):
                return {
                    'matches': ["{local::DuckDuckDatabase."],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            if prefix.endswith("db "):
                return {
                    'matches': ["local::DuckDuckConnection"],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            if prefix.endswith("]"):
                return {
                    'matches': [")"],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            if prefix.endswith("}#") or prefix.endswith(")"):
                return {
                    'matches': ["->"],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            if prefix.strip().endswith(" ->") or prefix.strip().endswith("->"):
                return {
                    'matches': ["filter(", "groupBy(", "select(", "extend(","from(" , "pivot(", "asofjoin(", "join(", "distinct(","rename(", "concatenate(",
                                "sort(","size(","drop("],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            if prefix.strip().endswith("x|$x."):
                match = re.search(r"#>\{local::DuckDuckDatabase\.([A-Za-z0-9_]+)}#", prefix)
                if match:
                    result = match.group(1)
                p = self.details[result]
                return {
                    'matches': p,
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            if prefix.strip().endswith("|"):
                if(cursor_pos>=2):
                    var = code[cursor_pos-2]
                    return {
                        'matches': ["$"+var+"."],
                        'cursor_start': cursor_pos,
                        'cursor_end': cursor_pos,
                        'metadata': {},
                        'status': 'ok'
                    }
                else:
                    return {
                        'matches': [],
                        'cursor_start': cursor_pos,
                        'cursor_end': cursor_pos,
                        'metadata': {},
                        'status': 'ok'
                    }
            if prefix.strip().endswith("~") or prefix.strip().endswith(",") or prefix.strip().endswith("[") or (prefix.strip().endswith(".") and cursor_pos>=3 and code[cursor_pos-3]=="$"):
                match = re.search(r"#>\{local::DuckDuckDatabase\.([A-Za-z0-9_]+)}#", prefix)
                if match:
                    result = match.group(1)
                p = self.details[result]
                return {
                    'matches': p,
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            if prefix.strip().endswith("filter("):
                return {
                    'matches': ["x|$x."],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            if prefix.strip().endswith("from("):
                return {
                    'matches': ["local::DuckDuckRuntime)"],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            if prefix.strip().endswith("--"):
                return {
                    'matches': ["file", "var"],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            if prefix.strip().endswith("select("):
                return {
                    'matches': ["~["],
                    'cursor_start': cursor_pos,
                    'cursor_end': cursor_pos,
                    'metadata': {},
                    'status': 'ok'
                }
            if "#>{local::DuckDuckDatabase" in prefix:
                match = re.search(r"#>\{local::DuckDuckDatabase\.([A-Za-z0-9_]*)$", prefix)
                if match:
                    typed = match.group(1)
                    matches = [
                        table +  "}#"
                        for table in self.tables
                        if table.lower().startswith(typed.lower())
                    ]
                    cursor_start = cursor_pos - len(typed)
                    cursor_end = cursor_pos

                    return {
                        'matches': matches,
                        'cursor_start': cursor_start,
                        'cursor_end': cursor_end,
                        'metadata': {},
                        'status': 'ok'
                    }
            matches = [s for s in suggestions if s.startswith(prefix)]
            # if not matches and cursor_pos==0:
            #     matches = suggestions
            return {
                'matches': matches,
                'cursor_start': 0,
                'cursor_end': cursor_pos,
                'metadata': {},
                'status': 'ok'
            }
        return {"status": "ok",
                "matches": [],
                "cursor_start": cursor_pos,
                "cursor_end": cursor_pos,
                "metadata": {}}



    def do_shutdown(self, restart):
        if hasattr(self, "_python"):
            try:
                self._python["client"].stop_channels()
                self._python["manager"].shutdown_kernel(now=True)
            except Exception:
                pass
        return super().do_shutdown(restart)
