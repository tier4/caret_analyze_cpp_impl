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

#ifndef CARET_ANALYZE_CPP_IMPL__RECORD_HPP_

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

#include "caret_analyze_cpp_impl/column_manager.hpp"

class RecordBase
{
public:
  RecordBase();
  explicit RecordBase(std::unordered_map<std::string, uint64_t> dict);
  RecordBase(const RecordBase & record);
  ~RecordBase() = default;

  // pythonのproperty用インターフェース
  std::unordered_map<std::string, uint64_t> get_data() const;
  std::unordered_set<std::string> get_columns() const;

  void change_dict_key(std::string key_from, std::string key_to);
  bool equals(const RecordBase & other) const;
  void merge(const RecordBase & other);
  uint64_t get(std::string column) const;
  uint64_t get_with_default(std::string column, uint64_t default_value) const;
  void add(std::string column, uint64_t stamp);
  void drop_columns(std::vector<std::string> columns);
  bool has_column(const std::string column);

private:
  std::unordered_map<size_t, uint64_t> data_;
};


class RecordsBase
{
public:
  RecordsBase();
  RecordsBase(const RecordsBase & records);
  RecordsBase(std::vector<RecordBase> records, std::vector<std::string> columns);
  explicit RecordsBase(std::string json_path);

  ~RecordsBase() = default;

  // pythonのproperty用インターフェース
  std::vector<RecordBase> get_data() const;
  std::vector<std::string> get_columns() const;

  void append(const RecordBase & other);
  void append_column(const std::string column, const std::vector<uint64_t> values);
  void set_columns(const std::vector<std::string> columns);
  bool equals(const RecordsBase & other) const;
  RecordsBase clone();
  void drop_columns(std::vector<std::string> column_names);
  void rename_columns(std::unordered_map<std::string, std::string> renames);
  void concat(const RecordsBase & other);
  void filter_if(std::function<bool(RecordBase)> & f);
  void sort(std::string key, std::string sub_key = "", bool ascending = true);
  void sort_column_order(bool ascending = true, bool put_none_at_top = true);
  void bind_drop_as_delay();
  void reindex(std::vector<std::string> columns);
  std::map<std::tuple<uint64_t>, RecordsBase> groupby(std::string column0);
  std::map<std::tuple<uint64_t, uint64_t>, RecordsBase> groupby(
    std::string column0,
    std::string column1);
  std::map<std::tuple<uint64_t, uint64_t, uint64_t>, RecordsBase> groupby(
    std::string column0, std::string column1, std::string column2);

  int get_idx(std::string column);
  std::string get_column(int);


  RecordsBase merge(
    const RecordsBase & right_records,
    std::string join_left_key,
    std::string join_right_key,
    std::vector<std::string> columns,
    std::string how,
    std::string progress_label = ""
  );

  RecordsBase merge_sequencial(
    const RecordsBase & right_records,
    std::string left_stamp_key,
    std::string right_stamp_key,
    std::string join_left_key,
    std::string join_right_key,
    std::vector<std::string> columns,
    std::string how,
    std::string progress_label = "");

  RecordsBase merge_sequencial_for_addr_track(
    std::string source_stamp_key,
    std::string source_key,
    const RecordsBase & copy_records,
    std::string copy_stamp_key,
    std::string copy_from_key,
    std::string copy_to_key,
    const RecordsBase & sink_records,
    std::string sink_stamp_key,
    std::string sink_from_key,
    std::string progress_label = ""
  );

  std::shared_ptr<std::vector<RecordBase>> data_;
  std::shared_ptr<std::vector<std::string>> columns_;
};

#endif  // CARET_ANALYZE_CPP_IMPL__RECORD_HPP_
#define CARET_ANALYZE_CPP_IMPL__RECORD_HPP_
