# How to run the benchmarks
1. Go to the root of velox repository.
2. `mkdir -p build && cp build`
3. `cmake -DVELOX_ENABLE_PARQUET=ON -DVELOX_BUILD_TESTING=ON ..`
4. `cmake --build . --target velox_parquet_benchmark`
5. `export VELOX_PARQUET_BENCHMARK_FILE=[path-to-parquet-file]`
6. `./velox/dwio/parquet/benchmarks/velox_parquet_benchmark`
   1. To test with other files you just need to change the environment variable