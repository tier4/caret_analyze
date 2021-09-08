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

#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <string>
#include <functional>
#include <memory>

class RecordBase
{
public:
  using ColumnT = std::unordered_set<std::string>;
  using DataT = std::unordered_map<std::string, uint64_t>;

  RecordBase();
  explicit RecordBase(std::unordered_map<std::string, uint64_t> dict);
  RecordBase(const RecordBase & record);
  ~RecordBase() = default;

  // pythonのproperty用インターフェース
  DataT get_data() const;
  ColumnT get_columns() const;

  void change_dict_key(std::string key_from, std::string key_to);
  bool equals(const RecordBase & other) const;
  void _merge(const RecordBase & other);
  uint64_t get(std::string key) const;
  void add(std::string key, uint64_t stamp);
  void _drop_columns(std::vector<std::string> keys);

  DataT data_;
  ColumnT columns_;
};


class RecordsBase
{
public:
  using ColumnT = std::unordered_set<std::string>;
  using DataT = std::vector<RecordBase>;

  RecordsBase();
  RecordsBase(const RecordsBase & records);
  explicit RecordsBase(DataT records);
  explicit RecordsBase(std::string json_path);

  ~RecordsBase() = default;

  // pythonのproperty用インターフェース
  DataT get_data() const;
  ColumnT get_columns() const;

  void append(const RecordBase & other);
  bool equals(const RecordsBase & other) const;
  void _drop_columns(std::vector<std::string> column_names);
  void _rename_columns(std::unordered_map<std::string, std::string> renames);
  void _concat(const RecordsBase & other);
  void _filter(std::function<bool(RecordBase)> & f);
  void _sort(std::string key, std::string sub_key = "", bool ascending = true);

  RecordsBase _merge(
    const RecordsBase & right_records,
    std::string join_key,
    std::string how,
    std::string progress_label = ""
  );

  RecordsBase _merge_sequencial(
    const RecordsBase & right_records,
    std::string left_stamp_key,
    std::string right_stamp_key,
    std::string join_key,
    std::string how,
    std::string progress_label = "");

  RecordsBase _merge_sequencial_for_addr_track(
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

  std::shared_ptr<DataT> data_;
  std::shared_ptr<ColumnT> columns_;

private:
};

#endif  // CARET_ANALYZE_CPP_IMPL__RECORD_HPP_
#define CARET_ANALYZE_CPP_IMPL__RECORD_HPP_
