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

RecordsBase::RecordsBase(std::vector<RecordBase> init)
: RecordsBase()
{
  for (auto & record : init) {
    append(record);
  }
}

RecordsBase::RecordsBase()
: data_(std::make_shared<std::vector<RecordBase>>()),
  columns_(std::make_shared<std::unordered_set<std::string>>())
{
}

RecordsBase::RecordsBase(const RecordsBase & records)
{
  data_ = std::make_shared<DataT>(*records.data_);
  columns_ = std::make_shared<ColumnT>(*records.columns_);
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


void RecordsBase::append(const RecordBase & other)
{
  data_->push_back(other);
  for (auto & pair : other.get_data()) {
    columns_->insert(pair.first);
  }
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

void RecordsBase::_drop_columns(std::vector<std::string> column_names)
{
  for (auto & record : *data_) {
    record._drop_columns(column_names);
  }
  for (auto & column_name : column_names) {
    columns_->erase(column_name);
  }
}

void RecordsBase::_rename_columns(std::unordered_map<std::string, std::string> renames)
{
  for (auto & record : *data_) {
    for (auto & pair : renames) {
      record.change_dict_key(pair.first, pair.second);
    }
  }
  for (auto & pair : renames) {
    columns_->erase(pair.first);
    columns_->insert(pair.second);
  }
}


std::vector<RecordBase> RecordsBase::get_data() const
{
  return *data_;
}

std::unordered_set<std::string> RecordsBase::get_columns() const
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

void RecordsBase::_filter(std::function<bool(RecordBase)> & f)
{
  *data_ = filter(*data_, f);
}

void RecordsBase::_concat(const RecordsBase & other)
{
  auto other_data = *other.data_;
  *columns_ = merge_set<std::string>(*columns_, *other.columns_);
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

void RecordsBase::_sort(std::string key, std::string sub_key, bool ascending)
{
  std::sort(data_->begin(), data_->end(), RecordComp{key, sub_key, ascending});
}


RecordsBase RecordsBase::_merge(
  const RecordsBase & right_records,
  std::string join_key,
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

  for (auto & left_record : *left_records_copy.data_) {
    left_record.add("side", Left);
  }

  for (auto & right_record : *right_records_copy.data_) {
    right_record.add("side", Right);
  }

  RecordsBase & concat_records = left_records_copy;
  concat_records._concat(right_records_copy);
  for (auto & record : *concat_records.data_) {
    record.add("has_valid_join_key", record.columns_.count(join_key) > 0);

    if (record.columns_.count(join_key) > 0) {
      record.add("merge_stamp", record.get(join_key));
    } else {
      record.add("merge_stamp", UINT64_MAX);
    }
  }

  concat_records._sort("merge_stamp", "side", true);

  std::vector<RecordBase *> empty_records;
  RecordBase * left_record_ = nullptr;

  RecordsBase merged_records;
  *merged_records.columns_ = merge_set<std::string>(*columns_, *right_records.columns_);

  auto bar = Progress(concat_records.data_->size(), progress_label);
  for (uint64_t i = 0; i < (uint64_t)concat_records.data_->size(); i++) {
    bar.tick();
    auto & record = (*concat_records.data_)[i];
    if (!record.get("has_valid_join_key")) {
      if (record.get("side") == Left && merge_left_record) {
        merged_records.append(record);
      } else if (record.get("side") == Right && merge_right_record) {
        merged_records.append(record);
      }
      continue;
    }

    auto join_value = record.get(join_key);
    if (record.get("side") == Left) {
      if (left_record_ && !left_record_->get("found_right_record")) {
        empty_records.push_back(left_record_);
      }
      left_record_ = &record;
      left_record_->add("found_right_record", false);
    } else {
      if (left_record_ && join_value == left_record_->get(join_key) &&
        record.get("has_valid_join_key"))
      {
        left_record_->add("found_right_record", true);
        auto merged_record = record;
        merged_record._merge(*left_record_);
        merged_records.append(merged_record);
      } else {
        empty_records.push_back(&record);
      }
    }
  }
  if (left_record_ && !left_record_->get("found_right_record")) {
    empty_records.push_back(left_record_);
  }
  for (auto & record_ptr : empty_records) {
    auto & record = *record_ptr;
    if (record.get("side") == Left && merge_left_record) {
      merged_records.append(record);
    } else if (record.get("side") == Right && merge_right_record) {
      merged_records.append(record);
    }
  }

  merged_records._drop_columns(
    {"side", "has_merge_stamp", "merge_stamp", "has_valid_join_key",
      "found_right_record"});

  return merged_records;
}


RecordsBase RecordsBase::_merge_sequencial(
  const RecordsBase & right_records,
  std::string left_stamp_key,
  std::string right_stamp_key,
  std::string join_key,
  std::string how,
  std::string progress_label
)
{
  auto left_records = RecordsBase(*this);

  bool merge_left = how == "left" || how == "outer";
  bool merge_right = how == "right" || how == "outer";

  RecordsBase merged_records;

  for (auto & left_record : *left_records.data_) {
    left_record.add("side", Left);
  }

  for (auto & right_record : *right_records.data_) {
    right_record.add("side", Right);
  }

  RecordsBase & concat_records = left_records;
  concat_records._concat(right_records);

  for (auto & record : *concat_records.data_) {
    record.add("has_valid_join_key", join_key == "" || record.columns_.count(join_key) > 0);

    if (record.get("side") == Left && record.columns_.count(left_stamp_key) > 0) {
      record.add("merge_stamp", record.get(left_stamp_key));
      record.add("has_merge_stamp", true);
    } else if (record.get("side") == Right && record.columns_.count(right_stamp_key) > 0) {
      record.add("merge_stamp", record.get(right_stamp_key));
      record.add("has_merge_stamp", true);
    } else {
      record.add("merge_stamp", UINT64_MAX);
      record.add("has_merge_stamp", false);
    }
  }


  auto get_join_value = [&join_key](RecordBase & record) -> uint64_t {
      if (join_key == "") {
        return 0;
      } else if (record.columns_.count(join_key) > 0) {
        return record.get(join_key);
      } else {
        return UINT64_MAX;  // use as None
      }
    };

  concat_records._sort("merge_stamp", "side", true);
  std::unordered_map<int, uint64_t> sub_empty_records;
  for (uint64_t i = 0; i < (uint64_t)concat_records.data_->size(); i++) {
    auto & record = (*concat_records.data_)[i];
    if (record.get("side") == Left && record.get("has_merge_stamp")) {
      record.add("sub_record_index", UINT64_MAX);  // use MAX as None

      auto join_value = get_join_value(record);
      if (join_value == UINT16_MAX) {
        continue;
      }
      sub_empty_records[join_value] = i;
    } else if (record.get("side") == Right && record.get("has_merge_stamp")) {
      auto join_value = get_join_value(record);
      if (join_value == UINT16_MAX) {
        continue;
      }
      if (sub_empty_records.count(join_value) > 0) {
        auto pre_left_record_index = sub_empty_records[join_value];
        RecordBase & pre_left_record = (*concat_records.data_)[pre_left_record_index];
        pre_left_record.add("sub_record_index", i);
        sub_empty_records.erase(join_value);
      }
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

    if (!current_record.get("has_merge_stamp") || !current_record.get("has_valid_join_key")) {
      if (current_record.get("side") == Left && merge_left) {
        merged_records.append(current_record);
        added.insert(&current_record);
      } else if (current_record.get("side") == Right && merge_right) {
        merged_records.append(current_record);
        added.insert(&current_record);
      }
      continue;
    }

    if (current_record.get("side") == Right) {
      if (merge_right) {
        merged_records.append(current_record);
        added.insert(&current_record);
      }
      continue;
    }

    RecordBase * sub_record_ptr = nullptr;
    auto sub_record_index = current_record.get("sub_record_index");
    if (sub_record_index != UINT64_MAX) {
      sub_record_ptr = &(*concat_records.data_)[sub_record_index];
    }

    if (sub_record_ptr == nullptr || added.count(sub_record_ptr) > 0) {
      if (merge_left) {
        merged_records.append(current_record);
        added.insert(&current_record);
      }
      continue;
    }
    RecordBase merge_record = current_record;
    merge_record._merge(*sub_record_ptr);
    merged_records.append(merge_record);
    added.insert(&current_record);
    added.insert(sub_record_ptr);
  }

  merged_records._drop_columns(
    {"side", "has_merge_stamp", "merge_stamp", "has_valid_join_key",
      "sub_record_index"});

  return merged_records;
}


RecordsBase RecordsBase::_merge_sequencial_for_addr_track(
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

  auto source_records_ = RecordsBase(*this);
  auto copy_records_ = copy_records;
  auto sink_records_ = sink_records;

  for (auto & record : *source_records_.data_) {
    record.add("type", Source);
    record.add("timestamp", record.get(source_stamp_key));
  }
  for (auto & record : *copy_records_.data_) {
    record.add("type", Copy);
    record.add("timestamp", record.get(copy_stamp_key));
  }
  for (auto & record : *sink_records_.data_) {
    record.add("type", Sink);
    record.add("timestamp", record.get(sink_stamp_key));
  }

  auto & records = source_records_;
  records._concat(copy_records_);
  records._concat(sink_records_);
  records._sort("timestamp", "type", false);

  std::vector<RecordBase> processing_records;
  using StampSet = std::set<uint64_t>;
  std::unordered_map<uint64_t, std::shared_ptr<StampSet>> stamp_sets;

  auto merge_processing_record_keys =
    [&processing_records, &stamp_sets](RecordBase & processing_record) {
      auto condition = [&processing_record, &stamp_sets](const RecordBase & x) {
          std::shared_ptr<StampSet> & sink_set = stamp_sets[x.get("timestamp")];
          std::shared_ptr<StampSet> & processing_record_set =
            stamp_sets[processing_record.get("timestamp")];
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
              "timestamp")];
        std::shared_ptr<StampSet> & corresponding_record_keys =
          stamp_sets[processing_record_.get("timestamp")];
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

  RecordsBase merged_records;

  auto bar = Progress(records.data_->size(), progress_label);
  for (auto & record : *records.data_) {
    bar.tick();
    if (record.get("type") == Sink) {
      auto timestamp = record.get("timestamp");
      auto stamp_set = std::make_shared<StampSet>();
      stamp_set->insert(record.get(sink_from_key));
      stamp_sets.insert(std::make_pair(timestamp, stamp_set));
      processing_records.emplace_back(record);
    } else if (record.get("type") == Copy) {
      auto condition = [&stamp_sets, &copy_to_key, &record](const RecordBase & x) {
          auto timestamp = x.get("timestamp");
          std::shared_ptr<StampSet> stamp_set = stamp_sets[timestamp];
          bool has_same_source_addrs = stamp_set->count(record.get(copy_to_key)) > 0;
          return has_same_source_addrs;
        };
      std::vector<RecordBase> records_with_same_source_addrs =
        filter(processing_records, condition);
      for (auto & processing_record : records_with_same_source_addrs) {
        auto timestamp = processing_record.get("timestamp");
        std::shared_ptr<StampSet> stamp_set = stamp_sets[timestamp];
        stamp_set->insert(record.get(copy_from_key));
        merge_processing_record_keys(processing_record);
        // No need for subsequent loops since we integreted them.
        break;
      }
    } else if (record.get("type") == Source) {
      auto condition = [&stamp_sets, &source_key, &record](const RecordBase & x) {
          auto timestamp = x.get("timestamp");
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

        processing_record._merge(record);
        merged_records.append(processing_record);
      }
    }
  }

  // Delete temporal columns
  merged_records._drop_columns({"type", "timestamp", sink_from_key});

  return merged_records;
}
