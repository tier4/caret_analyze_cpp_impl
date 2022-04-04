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
#include <map>
#include <memory>
#include <tuple>
#include <utility>
#include <iterator>

#include "nlohmann/json.hpp"

#include "caret_analyze_cpp_impl/record.hpp"
#include "caret_analyze_cpp_impl/common.hpp"
#include "caret_analyze_cpp_impl/column_manager.hpp"
#include "caret_analyze_cpp_impl/progress.hpp"
#include "caret_analyze_cpp_impl/records.hpp"

RecordsMapImpl::RecordsMapImpl(
  std::vector<Record> records,
  const std::vector<std::string> columns,
  const std::vector<std::string> key_columns,
  std::function<Key(const Record &)> make_key
)
: RecordsBase(columns),
  data_(std::make_unique<DataT>()),
  key_columns_(key_columns),
  make_key_([this, make_key](const Record & record) {
      auto key = make_key(record);
      key.add_key(this->size());
      return key;
    })
{
  if (key_columns.size() > max_key_size_) {
    throw std::exception();
  }

  for (auto & record : records) {
    append(record);
  }
}

RecordsMapImpl::~RecordsMapImpl()
{
}

RecordsMapImpl::RecordsMapImpl(
  const std::vector<std::string> key_columns,
  std::function<Key(const Record &)> make_key
)
: RecordsMapImpl({}, {}, key_columns, make_key)
{
}

RecordsMapImpl::RecordsMapImpl(
  const RecordsMapImpl & records,
  std::function<Key(const Record &)> make_key
)
: RecordsMapImpl(records, records.get_columns(), make_key)
{
}

RecordsMapImpl::RecordsMapImpl(
  const std::vector<std::string> columns,
  const std::vector<std::string> key_columns,
  std::function<Key(const Record &)> make_key
)
: RecordsMapImpl({}, columns, key_columns, make_key)
{
}

RecordsMapImpl::RecordsMapImpl(
  const RecordsMapImpl & records,
  const std::vector<std::string> key_columns,
  std::function<Key(const Record &)> make_key
)
: RecordsMapImpl(records.get_data(), records.get_columns(), key_columns, make_key)
{
}

void RecordsMapImpl::bind_drop_as_delay()
{
  throw std::exception();
}

void RecordsMapImpl::append(const Record & other)
{
  auto key = make_key_(other);
  auto pair = std::make_pair(key, other);
  data_->insert(pair);
}

// Key RecordsMapImpl::make_key(const Record & record)
// {
//   Key key;
//   for (size_t i = 0; i < key_columns_.size(); i++) {
//     auto & column = key_columns_[i];
//     key.add_key(record.get(column));
//   }

//   return key;
// }

std::unique_ptr<RecordsBase> RecordsMapImpl::clone() const
{
  return std::make_unique<RecordsMapImpl>(*this, make_key_);
}


std::vector<Record> RecordsMapImpl::get_data() const
{
  std::vector<Record> data;
  for (auto & record : *data_) {
    data.push_back(record.second);
  }
  return data;
}

void RecordsMapImpl::filter_if(const std::function<bool(Record)> & f)
{
  auto tmp = std::make_unique<DataT>();

  for (auto it = begin(); it->has_next(); it->next()) {
    auto & record = it->get_record();
    if (f(record)) {
      auto key = make_key_(record);
      tmp->insert(std::make_pair(key, record));
    }
  }
  data_ = std::move(tmp);
}

void RecordsMapImpl::sort(std::vector<std::string> keys, bool ascending)
{
  (void) keys;
  (void) ascending;

  throw std::exception();
}

std::size_t RecordsMapImpl::size() const
{
  return data_->size();
}

std::unique_ptr<IteratorBase> RecordsMapImpl::begin()
{
  return std::make_unique<MapIterator>(data_->begin(), data_->end());
}
std::unique_ptr<ConstIteratorBase> RecordsMapImpl::cbegin() const
{
  return std::make_unique<MapConstIterator>(data_->begin(), data_->end());
}

std::unique_ptr<IteratorBase> RecordsMapImpl::rbegin()
{
  return std::make_unique<MapIterator>(data_->rbegin(), data_->rend());
}

std::unique_ptr<ConstIteratorBase> RecordsMapImpl::crbegin() const
{
  return std::make_unique<MapConstIterator>(data_->rbegin(), data_->rend());
}
