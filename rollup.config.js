import { terser } from "rollup-plugin-terser"
import resolve from "@rollup/plugin-node-resolve"
import commonjs from "@rollup/plugin-commonjs"
import babel from "@rollup/plugin-babel"

const inputFile = "app/javascript/hotwire/spark/index.js"
const outputDir = "app/assets/javascripts"

export default {
  input: inputFile,
  output: [
    {
      file: `${outputDir}/hotwire_spark.js`,
      name: "HotwireSpark",
      format: "iife",
      inlineDynamicImports: true
    },
    {
      file: `${outputDir}/hotwire_spark.min.js`,
      format: "iife",
      name: "HotwireSpark",
      sourcemap: true,
      inlineDynamicImports: true,
      plugins: [ terser() ]
    },
  ],
  plugins: [
    resolve(),
    commonjs(),
    babel({
      babelHelpers: "bundled",
      presets: [
        [
          "@babel/preset-env",
          {
            targets: "> 0.25%, not dead",
            exclude: [ "transform-template-literals" ],
            modules: false,
          },
        ],
      ],
      exclude: "node_modules/**",
    }),
  ],
  external: ["@hotwired/stimulus"],
}
