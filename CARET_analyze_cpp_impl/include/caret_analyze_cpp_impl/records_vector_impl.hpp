// Copyright 2021 Research Institute of Systems Planning, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CARET_ANALYZE_CPP_IMPL__RECORDS_VECTOR_IMPL_HPP_

#include <tuple>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <string>
#include <functional>
#include <memory>
#include <map>
#include <algorithm>
#include <utility>
#include <iterator>
#include <yaml-cpp/yaml.h>

#include "caret_analyze_cpp_impl/column_manager.hpp"
#include "caret_analyze_cpp_impl/iterator_base.hpp"
#include "caret_analyze_cpp_impl/records_base.hpp"
#include "caret_analyze_cpp_impl/file.hpp"


class RecordsVectorImpl : public RecordsBase
{
public:
  RecordsVectorImpl();
  explicit RecordsVectorImpl(const RecordsVectorImpl & records);
  RecordsVectorImpl(std::vector<Record> records, std::vector<std::string> columns);
  explicit RecordsVectorImpl(std::vector<std::string> columns);
  explicit RecordsVectorImpl(RecordsVectorImpl && records) = default;

  explicit RecordsVectorImpl(std::string file_path);
  explicit RecordsVectorImpl(const File & file);

  ~RecordsVectorImpl() override;

  using DataT = std::vector<Record>;
  using Iterator = DataT::iterator;
  using ConstIterator = DataT::const_iterator;
  using ReverseIterator = DataT::reverse_iterator;
  using ConstReverseIterator = DataT::const_reverse_iterator;

  std::vector<Record> get_data() const override;
  void append(const Record & record) override;
  std::unique_ptr<RecordsBase> clone() const override;

  void filter_if(const std::function<bool(Record)> & f) override;
  void sort(std::string key, std::string sub_key = "", bool ascending = true);
  void sort_column_order(bool ascending = true, bool put_none_at_top = true);
  void bind_drop_as_delay();

  std::size_t size() const override;

  std::unique_ptr<IteratorBase> begin() override;
  std::unique_ptr<ConstIteratorBase> cbegin() const override;
  std::unique_ptr<IteratorBase> rbegin() override;
  std::unique_ptr<ConstIteratorBase> crbegin() const override;

private:
  std::unique_ptr<DataT> data_;
};


#endif  // CARET_ANALYZE_CPP_IMPL__RECORDS_VECTOR_IMPL_HPP_
#define CARET_ANALYZE_CPP_IMPL__RECORDS_VECTOR_IMPL_HPP_
