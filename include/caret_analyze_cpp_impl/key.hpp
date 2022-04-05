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

#include <vector>

#ifndef CARET_ANALYZE_CPP_IMPL__KEY_HPP_

class Key
{
public:
  void add_key(uint64_t key)
  {
    keys_.emplace_back(key);
  }

  bool operator==(const Key & other) const
  {
    if (keys_.size() != other.keys_.size()) {
      return false;
    }
    for (size_t i = 0; i < keys_.size(); i++) {
      if (keys_[i] != other.keys_[i]) {
        return false;
      }
    }
    return true;
  }

  bool operator<(const Key & other) const
  {
    for (size_t i = 0; i < keys_.size(); i++) {
      if (keys_.size() < i + 1) {
        return true;
      }
      if (other.keys_.size() < i + 1) {
        return true;
      }

      if (keys_[i] < other.keys_[i]) {
        return true;
      } else if (keys_[i] > other.keys_[i]) {
        return false;
      }
    }
    return true;
  }

  size_t size() const
  {
    return keys_.size();
  }

  size_t get_hash() const
  {
    size_t hash_value = 17;
    for (auto & key_val : keys_) {
      hash_value += hash_value * 31 + std::hash<uint64_t>()(key_val);
    }
    return hash_value;
  }

  struct Hash
  {
    size_t operator()(const Key & keys) const
    {
      return keys.get_hash();
    }
  };

private:
  std::vector<uint64_t> keys_;
};

#endif  // CARET_ANALYZE_CPP_IMPL__KEY_HPP_
#define CARET_ANALYZE_CPP_IMPL__KEY_HPP_
