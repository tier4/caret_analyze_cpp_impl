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

#include <string>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "caret_analyze_cpp_impl/records.hpp"
#include "caret_analyze_cpp_impl/file.hpp"

using ::testing::_;
using ::testing::Return;


class FileMock : public File
{
public:
  explicit FileMock(std::string data)
  : File()
  {
    data_ = data;
  }

  ~FileMock() override
  {
  }

  explicit FileMock(const char data[])
  : FileMock(std::string(data))
  {
  }

  const std::string & get_data() const override
  {
    return data_;
  }

private:
  std::string data_;
};

class RecordsVectorImplTest : public ::testing::Test
{
protected:
  virtual void Setp()
  {
  }
};

TEST_F(RecordsVectorImplTest, test_constructor_empty_file)
{
  FileMock file_mock("--- []");

  RecordsVectorImpl records(file_mock);
  auto data = records.get_data();
  ASSERT_EQ(data.size(), (size_t) 0);
}

TEST_F(RecordsVectorImplTest, test_constructor_file)
{
  auto s = std::string(R"(
        - key: 1
        - key: 2
          key_: 3
    )");
  FileMock file_mock(s);

  RecordsVectorImpl records(file_mock);
  auto data = records.get_data();
  ASSERT_EQ(data.size(), (size_t) 2);
  ASSERT_EQ(data[0].get_data().at("key"), (uint64_t) 1);
  ASSERT_EQ(data[1].get_data().at("key"), (uint64_t) 2);
  ASSERT_EQ(data[1].get_data().at("key_"), (uint64_t) 3);
}
