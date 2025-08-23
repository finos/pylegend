#Cell Magics
def magic_uppercase(cell_code, **kwargs):
    return cell_code.upper()

def magic_lowercase(cell_code, **kwargs):
    return cell_code.lower()

def evaluate_python(cell_code, **kwargs):
    import ast
    import io
    import contextlib
    try:
        tree = ast.parse(cell_code)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            exec(compile(tree, filename="<ast>", mode="exec"))
        output = buf.getvalue()
        return f"Python code executed successfully.\n------Output-----\n   {output}"
    except SyntaxError as e:
        return f"Syntax Error: {str(e)}"
    except Exception as e:
        return f"Error executing Python code: {str(e)}"


#Line Magics
def magic_start(line, **kwargs):
    return f"Hello {line}! Welcome to the Pure Kernel."

def magic_date(line, **kwargs): #If want to add as a line magic
    import datetime
    t = datetime.datetime.now()
    return f"Current date and time: {t.strftime("%Y-%m-%d %H:%M:%S")}"

def load_csv(line, **kwargs):
    import pandas as pd
    try: 
        df = pd.read_csv(line)
        return f"Loaded CSV file {line}"
    except Exception as e:
        return f"Error loading CSV: {str(e)}"
    
def show_stats(line, **kwargs):
    import pandas as pd
    try:
        df = pd.read_csv(line)
        stats = df.describe().to_string()
        return f"Statistics for {line}:\n{stats}"
    except Exception as e:
        return f"Error showing stats: {str(e)}"


# Register magics here
CELL_MAGICS = {
    'uppercase': magic_uppercase,
    'lowercase': magic_lowercase,
    'python': evaluate_python,
}

LINE_MAGICS = {
    'start': magic_start,
    'date': magic_date,
    'load_csv':load_csv,
    'show_stats':show_stats,
}