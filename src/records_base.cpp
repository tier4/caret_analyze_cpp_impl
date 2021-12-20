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


#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <string>
#include <limits>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <set>
#include <memory>
#include <tuple>
#include <utility>

#include "nlohmann/json.hpp"

#include "caret_analyze_cpp_impl/record.hpp"
#include "caret_analyze_cpp_impl/common.hpp"
#include "caret_analyze_cpp_impl/progress.hpp"

enum Side {Left, Right};

class UniqueList
{
public:
  void add_columns(std::vector<std::string> columns)
  {
    for (auto & column : columns) {
      if (!has(column)) {
        columns_.push_back(column);
      }
    }
  }

  std::vector<std::string> as_list()
  {
    return columns_;
  }

private:
  bool has(std::string column)
  {
    return std::count(columns_.begin(), columns_.end(), column) > 0;
  }

  std::vector<std::string> columns_;
};

RecordsBase::RecordsBase(std::vector<RecordBase> init, std::vector<std::string> columns)
: RecordsBase()
{
  for (auto & record : init) {
    append(record);
  }
  for (auto & column : columns) {
    columns_->push_back(column);
  }
}

RecordsBase::RecordsBase()
: data_(std::make_shared<std::vector<RecordBase>>()),
  columns_(std::make_shared<std::vector<std::string>>())
{
}

RecordsBase::RecordsBase(const RecordsBase & records)
{
  data_ = std::make_shared<std::vector<RecordBase>>(*records.data_);
  columns_ = std::make_shared<std::vector<std::string>>(*records.columns_);
}

RecordsBase::RecordsBase(std::string json_path)
: RecordsBase()
{
  using json = nlohmann::json;
  std::ifstream json_file(json_path.c_str());
  json records_json;
  json_file >> records_json;
  for (auto & record_json : records_json) {
    RecordBase record;
    for (auto & elem : record_json.items()) {
      auto & key = elem.key();
      auto & value = elem.value();
      record.add(key, value);
    }
    append(record);
  }
}

void RecordsBase::bind_drop_as_delay()
{
  sort_column_order(false, false);

  std::unordered_map<std::string, uint64_t> oldest_values;

  for (auto & record : *data_) {
    for (auto & key : *columns_) {
      bool has_value = record.columns_.count(key) > 0;
      bool has_value_ = oldest_values.count(key) > 0;
      if (!has_value && has_value_) {
        record.add(key, oldest_values[key]);
      }
      if (has_value) {
        oldest_values[key] = record.data_[key];
      }
    }
  }

  sort_column_order(true, true);
}

void RecordsBase::append(const RecordBase & other)
{
  data_->push_back(other);
}

void RecordsBase::append_column(const std::string column, const std::vector<uint64_t> values)
{
  columns_->push_back(column);
  for (size_t i = 0; i < data_->size(); i++) {
    auto & record = (*data_)[i];
    auto & value = values[i];
    record.add(column, value);
  }
}

RecordsBase RecordsBase::clone()
{
  return RecordsBase(*this);
}

bool RecordsBase::equals(const RecordsBase & other) const
{
  auto size_equal = data_->size() == other.data_->size();
  if (!size_equal) {
    return false;
  }
  int data_size = static_cast<int>(data_->size());
  for (int i = 0; i < data_size; i++) {
    auto record = (*data_)[i];
    auto other_record = (*other.data_)[i];
    auto is_equal = record.equals(other_record);
    if (!is_equal) {
      return false;
    }
  }
  if (*columns_ != *other.columns_) {
    return false;
  }

  return true;
}

void RecordsBase::drop_columns(std::vector<std::string> column_names)
{
  for (auto & record : *data_) {
    record.drop_columns(column_names);
  }

  auto has_key = [&](std::string column) -> bool {
      for (auto column_name : column_names) {
        if (column == column_name) {
          return true;
        }
      }
      return false;
    };

  auto columns_tmp = *columns_;
  columns_->clear();
  for (auto & column_tmp : columns_tmp) {
    if (has_key(column_tmp)) {
      continue;
    }
    columns_->push_back(column_tmp);
  }
}

void RecordsBase::rename_columns(
  std::unordered_map<std::string, std::string> renames)
{
  for (auto & record : *data_) {
    for (auto & pair : renames) {
      record.change_dict_key(pair.first, pair.second);
    }
  }
  for (auto & column : *columns_) {
    if (renames.count(column) > 0) {
      column = renames[column];
    }
  }
}


std::vector<RecordBase> RecordsBase::get_data() const
{
  return *data_;
}

std::vector<std::string> RecordsBase::get_columns() const
{
  return *columns_;
}


template<typename Cont, typename Pred>
Cont filter(const Cont & container, Pred predicate)
{
  Cont result;
  std::copy_if(container.begin(), container.end(), std::back_inserter(result), predicate);
  return result;
}

void RecordsBase::filter_if(std::function<bool(RecordBase)> & f)
{
  *data_ = filter(*data_, f);
}

void RecordsBase::reindex(std::vector<std::string> columns)
{
  *columns_ = columns;
}

void RecordsBase::concat(const RecordsBase & other)
{
  auto other_data = *other.data_;
  data_->insert(data_->end(), other_data.begin(), other_data.end());
}

class RecordComp
{
public:
  RecordComp(std::string key, std::string sub_key, bool ascending)
  : key_(key), sub_key_(sub_key), ascending_(ascending)
  {
  }

  bool operator()(const RecordBase & a, const RecordBase & b) const noexcept
  {
    if (ascending_) {
      if (a.get(key_) != b.get(key_) || sub_key_ == "") {
        return a.get(key_) < b.get(key_);
      }
      return a.get(sub_key_) < b.get(sub_key_);
    } else {
      if (a.get(key_) != b.get(key_) || sub_key_ == "") {
        return a.get(key_) > b.get(key_);
      }
      return a.get(sub_key_) > b.get(sub_key_);
    }
  }

private:
  std::string key_;
  std::string sub_key_;
  bool ascending_;
};

void RecordsBase::sort(std::string key, std::string sub_key, bool ascending)
{
  std::sort(data_->begin(), data_->end(), RecordComp{key, sub_key, ascending});
}

class RecordCompColumnOrder
{
public:
  RecordCompColumnOrder(
    std::vector<std::string> columns,
    bool ascending,
    bool put_none_at_top
  )
  : columns_(columns), ascending_(ascending)
  {
    if (ascending_) {
      if (put_none_at_top) {
        default_value_ = UINT64_MAX;
      } else {
        default_value_ = 0;
      }
    } else {
      if (put_none_at_top) {
        default_value_ = 0;
      } else {
        default_value_ = UINT64_MAX;
      }
    }
  }

  bool operator()(const RecordBase & a, const RecordBase & b) const noexcept
  {
    if (ascending_) {
      for (auto & column : columns_) {
        auto left = a.get_with_default(column, default_value_);
        auto right = b.get_with_default(column, default_value_);
        if (left < right) {
          return true;
        } else if (left > right) {
          return false;
        }
      }
      return true;
    } else {
      for (auto & column : columns_) {
        auto left = a.get_with_default(column, default_value_);
        auto right = b.get_with_default(column, default_value_);
        if (left > right) {
          return true;
        } else if (left < right) {
          return false;
        }
      }
      return true;
    }
  }

private:
  std::vector<std::string> columns_;
  bool ascending_;
  uint64_t default_value_;
};

void RecordsBase::sort_column_order(bool ascending, bool put_none_at_top)
{
  std::sort(
    data_->begin(),
    data_->end(), RecordCompColumnOrder{*columns_, ascending, put_none_at_top});
}


RecordsBase RecordsBase::merge(
  const RecordsBase & right_records,
  std::string join_left_key,
  std::string join_right_key,
  std::vector<std::string> columns,
  std::string how,
  std::string progress_label
)
{
  // [python side implementation]
  // assert how in ["inner", "left", "right", "outer"]

  bool merge_right_record = how == "right" || how == "outer";
  bool merge_left_record = how == "left" || how == "outer";

  auto left_records_copy = RecordsBase(*this);
  auto right_records_copy = RecordsBase(right_records);

  auto column_side = "_tmp_merge_side";
  auto column_merge_stamp = "_tmp_merge_stamp";
  auto column_has_valid_join_key = "_tmp_merge_has_valid_join_key";
  auto column_join_key = "_tmp_merge_join_key";
  auto column_found_right_record = "_tmp_merge_found_right_record";


  left_records_copy.append_column(
    column_side,
    std::vector<uint64_t>(left_records_copy.data_->size(), Left)
  );

  right_records_copy.append_column(
    column_side,
    std::vector<uint64_t>(right_records_copy.data_->size(), Right)
  );

  auto concat_columns = UniqueList();
  concat_columns.add_columns(*left_records_copy.columns_);
  concat_columns.add_columns(*right_records_copy.columns_);
  concat_columns.add_columns(
  {
    column_side,
    column_has_valid_join_key,
    column_merge_stamp,
    column_join_key
  });

  RecordsBase concat_records = RecordsBase({}, concat_columns.as_list());
  concat_records.concat(left_records_copy);
  concat_records.concat(right_records_copy);

  for (auto & record : *concat_records.data_) {
    std::string join_key;
    if (record.get(column_side) == Left) {
      join_key = join_left_key;
    } else {
      join_key = join_right_key;
    }

    auto has_valid_join_key = record.columns_.count(join_key) > 0;
    record.add(column_has_valid_join_key, has_valid_join_key);

    if (has_valid_join_key) {
      record.add(column_merge_stamp, record.get(join_key));
      record.add(column_join_key, record.get(join_key));
    } else {
      record.add(column_merge_stamp, UINT64_MAX);
    }
  }

  concat_records.sort(column_merge_stamp, column_side, true);

  std::vector<RecordBase *> empty_records;
  RecordBase * left_record_ = nullptr;

  RecordsBase merged_records({}, columns);

  auto bar = Progress(concat_records.data_->size(), progress_label);
  for (uint64_t i = 0; i < (uint64_t)concat_records.data_->size(); i++) {
    bar.tick();
    auto & record = (*concat_records.data_)[i];
    if (!record.get(column_has_valid_join_key)) {
      if (record.get(column_side) == Left && merge_left_record) {
        merged_records.append(record);
      } else if (record.get(column_side) == Right && merge_right_record) {
        merged_records.append(record);
      }
      continue;
    }

    auto join_value = record.get(column_join_key);
    if (record.get(column_side) == Left) {
      if (left_record_ && !left_record_->get(column_found_right_record)) {
        empty_records.push_back(left_record_);
      }
      left_record_ = &record;
      left_record_->add(column_found_right_record, false);
    } else {
      if (left_record_ && join_value == left_record_->get(column_join_key) &&
        record.get(column_has_valid_join_key))
      {
        left_record_->add(column_found_right_record, true);
        auto merged_record = record;
        merged_record.merge(*left_record_);
        merged_records.append(merged_record);
      } else {
        empty_records.push_back(&record);
      }
    }
  }
  if (left_record_ && !left_record_->get(column_found_right_record)) {
    empty_records.push_back(left_record_);
  }
  for (auto & record_ptr : empty_records) {
    auto & record = *record_ptr;
    if (record.get(column_side) == Left && merge_left_record) {
      merged_records.append(record);
    } else if (record.get(column_side) == Right && merge_right_record) {
      merged_records.append(record);
    }
  }

  merged_records.drop_columns(
    {column_side, column_merge_stamp, column_join_key,
      column_has_valid_join_key, column_found_right_record});

  return merged_records;
}


RecordsBase RecordsBase::merge_sequencial(
  const RecordsBase & right_records,
  std::string left_stamp_key,
  std::string right_stamp_key,
  std::string join_left_key,
  std::string join_right_key,
  std::vector<std::string> columns,
  std::string how,
  std::string progress_label
)
{
  auto left_records_copy = RecordsBase(*this);
  auto right_records_copy = RecordsBase(right_records);

  bool merge_left = how == "left" || how == "outer" || how == "left_use_latest";
  bool merge_right = how == "right" || how == "outer";
  bool bind_latest_left_record = how == "left_use_latest";


  RecordsBase merged_records({}, columns);

  auto column_side = "_merge_tmp_side";
  auto column_has_valid_join_key = "merge_tmp_has_valid_join_key";
  auto column_merge_stamp = "_merge_tmp_merge_stamp";
  auto column_has_merge_stamp = "_merge_tmp_has_merge_stamp";

  left_records_copy.append_column(
    column_side,
    std::vector<uint64_t>(left_records_copy.data_->size(), Left)
  );

  right_records_copy.append_column(
    column_side,
    std::vector<uint64_t>(right_records_copy.data_->size(), Right)
  );

  auto concat_columns = UniqueList();
  concat_columns.add_columns(*left_records_copy.columns_);
  concat_columns.add_columns(*right_records_copy.columns_);
  concat_columns.add_columns(
  {
    column_has_merge_stamp,
    column_merge_stamp,
    column_has_merge_stamp,
  });
  RecordsBase concat_records = RecordsBase({}, concat_columns.as_list());
  concat_records.concat(left_records_copy);
  concat_records.concat(right_records_copy);

  for (auto & record : *concat_records.data_) {
    if (record.get(column_side) == Left) {
      record.add(
        column_has_valid_join_key,
        join_left_key == "" || record.columns_.count(join_left_key) > 0
      );
    } else {
      record.add(
        column_has_valid_join_key,
        join_right_key == "" || record.columns_.count(join_right_key) > 0
      );
    }

    if (record.get(column_side) == Left && record.columns_.count(left_stamp_key) > 0) {
      record.add(column_merge_stamp, record.get(left_stamp_key));
      record.add(column_has_merge_stamp, true);
    } else if (record.get(column_side) == Right && record.columns_.count(right_stamp_key) > 0) {
      record.add(column_merge_stamp, record.get(right_stamp_key));
      record.add(column_has_merge_stamp, true);
    } else {
      record.add(column_merge_stamp, UINT64_MAX);
      record.add(column_has_merge_stamp, false);
    }
  }


  auto get_join_value =
    [&join_left_key, &join_right_key, &column_side](RecordBase & record) -> uint64_t {
      std::string join_key;
      if (record.get(column_side) == Left) {
        join_key = join_left_key;
      } else {
        join_key = join_right_key;
      }

      if (join_key == "") {
        return 0;
      } else if (record.columns_.count(join_key) > 0) {
        return record.get(join_key);
      } else {
        return UINT64_MAX;  // use as None
      }
    };

  concat_records.sort(column_merge_stamp, column_side, true);

  // std::unordered_map<int, uint64_t> sub_empty_records;
  std::unordered_map<uint64_t, uint64_t> to_left_record_index;
  std::unordered_map<uint64_t, std::vector<uint64_t>> to_sub_record_indices;

  for (uint64_t i = 0; i < (uint64_t)concat_records.data_->size(); i++) {
    auto & record = (*concat_records.data_)[i];
    if (!record.get(column_has_merge_stamp)) {
      continue;
    }

    if (record.get(column_side) == Left) {
      to_sub_record_indices[i] = std::vector<uint64_t>();

      auto join_value = get_join_value(record);
      if (join_value == UINT64_MAX) {
        continue;
      }
      to_left_record_index[join_value] = i;
    } else if (record.get(column_side) == Right) {
      auto join_value = get_join_value(record);
      if (join_value == UINT64_MAX) {
        continue;
      }

      if (to_left_record_index.count(join_value) == 0) {
        continue;
      }
      auto left_record_index = to_left_record_index[join_value];
      to_sub_record_indices[left_record_index].push_back(i);
    }
  }

  std::unordered_set<const RecordBase *> added;

  std::size_t records_size = concat_records.data_->size();
  auto bar = Progress(records_size, progress_label);
  for (int i = 0; i < static_cast<int>(records_size); i++) {
    bar.tick();
    RecordBase & current_record = (*concat_records.data_)[i];
    bool is_recorded = added.count(&current_record) > 0;
    if (is_recorded) {
      continue;
    }

    if (!current_record.get(column_has_merge_stamp) ||
      !current_record.get(column_has_valid_join_key))
    {
      if (current_record.get(column_side) == Left && merge_left) {
        merged_records.append(current_record);
        added.insert(&current_record);
      } else if (current_record.get(column_side) == Right && merge_right) {
        merged_records.append(current_record);
        added.insert(&current_record);
      }
      continue;
    }

    if (current_record.get(column_side) == Right) {
      if (merge_right) {
        merged_records.append(current_record);
        added.insert(&current_record);
      }
      continue;
    }

    auto sub_record_indices = to_sub_record_indices[i];
    if (sub_record_indices.size() == 0) {
      if (merge_left) {
        merged_records.append(current_record);
        added.insert(&current_record);
      }
      continue;
    }

    for (uint64_t j = 0; j < sub_record_indices.size(); j++) {
      auto sub_record_index = sub_record_indices[j];
      RecordBase & sub_record = (*concat_records.data_)[sub_record_index];
      if (1 <= j && !bind_latest_left_record) {
        break;
      }

      if (added.count(&sub_record) > 0) {
        if (merge_left) {
          merged_records.append(current_record);
          added.insert(&current_record);
        }
        continue;
      }

      RecordBase merge_record = current_record;
      merge_record.merge(sub_record);
      merged_records.append(merge_record);
      added.insert(&current_record);
      added.insert(&sub_record);
    }
  }

  merged_records.drop_columns(
  {
    column_side,
    column_has_merge_stamp,
    column_merge_stamp,
    column_has_valid_join_key,
  }
  );

  return merged_records;
}


RecordsBase RecordsBase::merge_sequencial_for_addr_track(
  std::string source_stamp_key,
  std::string source_key,
  const RecordsBase & copy_records,
  std::string copy_stamp_key,
  std::string copy_from_key,
  std::string copy_to_key,
  const RecordsBase & sink_records,
  std::string sink_stamp_key,
  std::string sink_from_key,
  std::string progress_label
)
{
  enum RecordType { Copy, Sink, Source};
  // [python side implementation]
  // assert how in ["inner", "left", "right", "outer"]

  auto column_type = "_tmp_type";
  auto column_timestamp = "_tmp_timestamp";

  auto source_records_ = RecordsBase(*this);
  auto copy_records_ = RecordsBase(copy_records);
  auto sink_records_ = RecordsBase(sink_records);

  source_records_.append_column(
    column_type,
    std::vector<uint64_t>(source_records_.data_->size(), Source)
  );

  copy_records_.append_column(
    column_type,
    std::vector<uint64_t>(copy_records_.data_->size(), Copy)
  );

  sink_records_.append_column(
    column_type,
    std::vector<uint64_t>(sink_records_.data_->size(), Sink)
  );

  std::vector<uint64_t> source_stamps;
  std::vector<uint64_t> sink_stamps;
  for (auto & record : *source_records_.data_) {
    source_stamps.emplace_back(record.get(source_stamp_key));
  }
  for (auto & record : *sink_records_.data_) {
    sink_stamps.emplace_back(record.get(sink_stamp_key));
  }

  source_records_.append_column(column_timestamp, source_stamps);

  copy_records_.rename_columns(
    std::unordered_map<std::string, std::string>(
  {
    {copy_stamp_key, column_timestamp}
  })
  );

  sink_records_.append_column(column_timestamp, sink_stamps);

  auto merged_columns = UniqueList();
  merged_columns.add_columns(*source_records_.columns_);
  merged_columns.add_columns(*copy_records_.columns_);
  merged_columns.add_columns(*sink_records_.columns_);

  auto merged_records = RecordsBase({}, merged_columns.as_list());

  auto concat_records = RecordsBase({}, merged_columns.as_list());
  concat_records.concat(source_records_);
  concat_records.concat(copy_records_);
  concat_records.concat(sink_records_);
  concat_records.sort(column_timestamp, column_type, false);

  std::vector<RecordBase> processing_records;
  using StampSet = std::set<uint64_t>;
  std::unordered_map<uint64_t, std::shared_ptr<StampSet>> stamp_sets;

  auto merge_processing_record_keys =
    [&processing_records, &stamp_sets, &column_timestamp](RecordBase & processing_record) {
      auto condition = [&processing_record, &stamp_sets, &column_timestamp](const RecordBase & x) {
          std::shared_ptr<StampSet> & sink_set = stamp_sets[x.get(column_timestamp)];
          std::shared_ptr<StampSet> & processing_record_set =
            stamp_sets[processing_record.get(column_timestamp)];
          std::shared_ptr<StampSet> result = std::make_shared<StampSet>();

          std::set_intersection(
            sink_set->begin(), sink_set->end(),
            processing_record_set->begin(), processing_record_set->end(),
            std::inserter(*result, result->end())
          );
          return result->size() > 0 && sink_set.get() != processing_record_set.get();
        };
      std::vector<RecordBase> processing_records_ = filter(
        processing_records, condition
      );
      for (auto & processing_record_ : processing_records_) {
        std::shared_ptr<StampSet> & processing_record_keys = stamp_sets[processing_record.get(
              column_timestamp)];
        std::shared_ptr<StampSet> & corresponding_record_keys =
          stamp_sets[processing_record_.get(column_timestamp)];
        std::shared_ptr<StampSet> merged_set = std::make_shared<StampSet>();

        std::set_union(
          processing_record_keys->begin(), processing_record_keys->end(),
          corresponding_record_keys->begin(), corresponding_record_keys->end(),
          std::inserter(*merged_set, merged_set->end())
        );
        processing_record_keys = merged_set;
        corresponding_record_keys = merged_set;
      }
    };


  auto bar = Progress(concat_records.data_->size(), progress_label);
  for (auto & record : *concat_records.data_) {
    bar.tick();
    if (record.get(column_type) == Sink) {
      auto timestamp = record.get(column_timestamp);
      auto stamp_set = std::make_shared<StampSet>();
      stamp_set->insert(record.get(sink_from_key));
      stamp_sets.insert(std::make_pair(timestamp, stamp_set));
      processing_records.emplace_back(record);
    } else if (record.get(column_type) == Copy) {
      auto condition =
        [&stamp_sets, &copy_to_key, &record, &column_timestamp](const RecordBase & x) {
          auto timestamp = x.get(column_timestamp);
          std::shared_ptr<StampSet> stamp_set = stamp_sets[timestamp];
          bool has_same_source_addrs = stamp_set->count(record.get(copy_to_key)) > 0;
          return has_same_source_addrs;
        };
      std::vector<RecordBase> records_with_same_source_addrs =
        filter(processing_records, condition);
      for (auto & processing_record : records_with_same_source_addrs) {
        auto timestamp = processing_record.get(column_timestamp);
        std::shared_ptr<StampSet> stamp_set = stamp_sets[timestamp];
        stamp_set->insert(record.get(copy_from_key));
        merge_processing_record_keys(processing_record);
        // No need for subsequent loops since we integreted them.
        break;
      }
    } else if (record.get(column_type) == Source) {
      auto condition =
        [&stamp_sets, &source_key, &record, &column_timestamp](const RecordBase & x) {
          auto timestamp = x.get(column_timestamp);
          std::shared_ptr<StampSet> stamp_set = stamp_sets[timestamp];
          bool has_same_source_addrs = stamp_set->count(record.get(source_key)) > 0;
          return has_same_source_addrs;
        };
      std::vector<RecordBase> records_with_same_source_addrs =
        filter(processing_records, condition);
      for (auto & processing_record : records_with_same_source_addrs) {
        // remove processing_record from processing_records
        auto it = processing_records.begin();
        while (it != processing_records.end()) {
          auto & processing_record_ = *it;
          if (processing_record.equals(processing_record_)) {
            it = processing_records.erase(it);
            break;
          }
          it++;
        }

        processing_record.merge(record);
        merged_records.append(processing_record);
      }
    }
  }

  // Delete temporal columns
  merged_records.drop_columns(
  {
    column_type, column_timestamp, sink_from_key, copy_from_key, copy_to_key, copy_stamp_key});

  return merged_records;
}
