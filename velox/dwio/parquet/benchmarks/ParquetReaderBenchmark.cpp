/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/Benchmark.h>
#include <folly/Random.h>
#include <folly/init/Init.h>

#include "velox/common/base/BitUtil.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/type/fbhive/HiveTypeParser.h"
#include "velox/type/Type.h"
#include "velox/type/tests/FilterBuilder.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/tests/VectorMaker.h"

using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwio::type::fbhive;
using namespace facebook::velox;
using namespace facebook::velox::parquet;

namespace facebook {
namespace velox {
namespace test {

BENCHMARK(ReadIntColumn) {
  const char* file_path = std::getenv("VELOX_PARQUET_BENCHMARK_FILE");
  const std::string path_as_str(file_path);

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(path_as_str), readerOptions);

  RowReaderOptions rowReaderOpts;
  auto rowType = ROW({"f0"}, {INTEGER()});
  auto cs =
      std::make_shared<ColumnSelector>(rowType, std::vector<std::string>{"f0"});
  rowReaderOpts.select(cs);

  auto rowReader = reader.createRowReader(rowReaderOpts);

  uint64_t read_rows = 0;

  VectorPtr result;
  do {
    read_rows = rowReader->next(65000, result);
  } while (read_rows > 0);
}

BENCHMARK(ReadBigIntColumn) {
  const char* file_path = std::getenv("VELOX_PARQUET_BENCHMARK_FILE");
  const std::string path_as_str(file_path);

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(path_as_str), readerOptions);

  RowReaderOptions rowReaderOpts;
  auto rowType = ROW({"f0", "f1"}, {INTEGER(), BIGINT()});
  auto cs =
      std::make_shared<ColumnSelector>(rowType, std::vector<std::string>{"f1"});
  rowReaderOpts.select(cs);

  auto rowReader = reader.createRowReader(rowReaderOpts);

  uint64_t read_rows = 0;

  VectorPtr result;
  do {
    read_rows = rowReader->next(65000, result);
  } while (read_rows > 0);
}

BENCHMARK(ReadVarcharColumn) {
  const char* file_path = std::getenv("VELOX_PARQUET_BENCHMARK_FILE");
  const std::string path_as_str(file_path);

  ReaderOptions readerOptions;
  ParquetReader reader(
      std::make_unique<FileInputStream>(path_as_str), readerOptions);

  RowReaderOptions rowReaderOpts;
  auto rowType = ROW({"f0", "f1", "f2"}, {INTEGER(), BIGINT(), VARCHAR()});
  auto cs =
      std::make_shared<ColumnSelector>(rowType, std::vector<std::string>{"f2"});
  rowReaderOpts.select(cs);

  auto rowReader = reader.createRowReader(rowReaderOpts);

  uint64_t read_rows = 0;

  VectorPtr result;
  do {
    read_rows = rowReader->next(65000, result);
  } while (read_rows > 0);
}

} // namespace test
} // namespace velox
} // namespace facebook

// To run:
// buck run @mode/opt-clang-thinlto \
//   velox/vector/benchmarks:selectivity_vector_benchmark
int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
