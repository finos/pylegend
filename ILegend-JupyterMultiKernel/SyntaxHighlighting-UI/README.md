# ILegend-coloring using CodeMirrorv6

A JupyterLab extension.

## Requirements

- JupyterLab >= 4.0.0

## Install

To install the extension, execute:

```bash
yarn install
yarn add --dev rollup rollup-plugin-typescript2 @rollup/plugin-node-resolve @rollup/plugin-commonjs typescript -W
yarn add --dev @rollup/plugin-url -W
yarn build
jupyter labextension install .
jupyter lab build
```

To check the extension is loaded or not:

```bash
jupyter labextension list
```

Start working upon:
```bash
jupyter lab
```

Note: Jupyter notebook don't have support for the custom syntax highlighting.
