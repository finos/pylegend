import typescript from 'rollup-plugin-typescript2';
import resolve from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';
import url from '@rollup/plugin-url'; // <== add this

export default {
  input: 'src/index.ts',
  output: {
    file: 'lib/index.js',
    format: 'esm',
    sourcemap: true
  },
  external: [
    '@jupyterlab/application',
    '@jupyterlab/codemirror',
    '@jupyterlab/apputils',
    '@jupyterlab/ui-components', // add this too
    '@codemirror/language',
    '@lezer/highlight'
  ],
  plugins: [
    resolve({
      preferBuiltins: true // <== suppresses 'path' warning
    }),
    commonjs(),
    url({
      include: ['**/*.svg', '**/*.png', '**/*.jpg', '**/*.gif'],
      limit: 8192,       // inline assets under 8 KB
      emitFiles: true
    }),
    typescript({ tsconfig: './tsconfig.json' })
  ]
};
