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

#ifndef CARET_ANALYZE_CPP_IMPL__ITERATOR_IMPL_HPP_

#include "caret_analyze_cpp_impl/iterator_base.hpp"
#include "caret_analyze_cpp_impl/records_vector_impl.hpp"

class VectorIterator : public IteratorBase
{
public:
  explicit VectorIterator(RecordsVectorImpl::Iterator it, RecordsVectorImpl::Iterator end);
  explicit VectorIterator(RecordsVectorImpl::ReverseIterator it, RecordsVectorImpl::ReverseIterator end);

  Record & get_record() const override;
  bool has_next() const override;
  void next() override;

private:

  bool is_forward_;
  RecordsVectorImpl::Iterator it_;
  RecordsVectorImpl::Iterator end_;

  RecordsVectorImpl::ReverseIterator rit_;
  RecordsVectorImpl::ReverseIterator rend_;
};

#endif  // CARET_ANALYZE_CPP_IMPL__ITERATOR_IMPL_HPP_
#define CARET_ANALYZE_CPP_IMPL__ITERATOR_IMPL_HPP_
