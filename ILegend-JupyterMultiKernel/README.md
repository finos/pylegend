### DESCRIPTION
- ILegend - Jupyter Kernel for Legend


### FEATURES
- Users can query data using ILegend Kernel by using legend language in jupyter notebooks.
- It is Python implementation of Legend REPL.
- Users can load local csv files and operate on them in a similar fashion as remote data sources.
- Users can get full benefit of legend database pushdown for queries being executed.
- Integrated IPython Kernel features into ILegend Kernel, giving capability to users to switch between languages in the same notebook session.
- Query results from legend notebook cells will be avaialbe as pandas dataframes in subsequent python cells to continue further data analysis.
- Syntax highlighting and auto-completion are availabe for both python and legend code within notebooks. Both light and dark theme are supported.


### REQUIREMENTS
- JupyterLab >= 4.0.0


### USER INSTALLATION
For ILegend-Kernel to install in your virtual environment, follow the steps:
- Activate your virtual environment
- ```
  pip install ilegend-kernel
  python -m ilegend_kernel.install
  ```
For KernelSwitch-UI lab extenstion to install, follow the steps:
- ```
  pip install ilegend-cell-ui
  jupyter lab build
  ```
For Legend-SyntaxHighlighting lab extenstion to install, follow the steps:
- ```
  pip install ilegend-syntax-coloring
  jupyter lab build
  ```


To uninstall ILegend Kernel:
- ```
  pip uninstall ilegend-kernel
  jupyter kernelspec remove ilegend-kernel
  ```



  
Now you can see the features below!!
You can start using the ILegend Kernel now!!


### HOME INTERFACE
<img width="855" height="542" alt="image" src="https://github.com/user-attachments/assets/03fd7bf0-e071-405a-91f4-c9729c6d5b2e" />


### NOTEBOOK INTERFACE
<img width="928" height="349" alt="image" src="https://github.com/user-attachments/assets/037a7230-96e8-4073-a983-41e9c3c0a77d" />



