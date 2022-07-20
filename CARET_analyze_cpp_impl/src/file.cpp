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
#include <utility>
#include <functional>
#include <iostream>
#include <fstream>

#include "caret_analyze_cpp_impl/file.hpp"


File::File(std::string path)
{
  std::ifstream ifs(path);

  if (!ifs) {
    std::cerr << "Failed to load " << path;
    std::exit(1);
  }

  ifs >> data_;
}
File::File(const char path[])
: File(std::string(path))
{
}

File::File()
{
}

const std::string & File::get_data() const
{
  return data_;
}
