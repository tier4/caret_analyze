#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <string>
#include <limits>
#include <iostream>
#include <algorithm>
#include <set>
#include <memory>
#include <limits>
#include <tuple>
#include <utility>

#include "trace_analysis_cpp_impl/record.hpp"
#include "trace_analysis_cpp_impl/common.hpp"
#include "trace_analysis_cpp_impl/progress.hpp"

RecordsBase::RecordsBase(std::vector<RecordBase> init)
: RecordsBase()
{
  for (auto & record : init) {
    append(record);
  }
}

RecordsBase::RecordsBase()
: data_(std::make_unique<std::vector<RecordBase>>()),
  columns_(std::make_unique<std::unordered_set<std::string>>())
{
}

RecordsBase::RecordsBase(const RecordsBase & records)
{
  data_ = std::make_shared<DataT>(*records.data_);
  columns_ = std::make_shared<ColumnT>(*records.columns_);
}

void RecordsBase::append(const RecordBase & other)
{
  data_->push_back(other);
  for (auto & pair: other.get_data()) {
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
  for (auto & record: *data_) {
    record._drop_columns(column_names);
  }
  for (auto & column_name : column_names) {
    columns_->erase(column_name);
  }
}

void RecordsBase::_rename_columns(std::unordered_map<std::string, std::string> renames)
{
  for (auto & record: *data_) {
    for (auto & pair: renames) {
      record.change_dict_key(pair.first, pair.second);
    }
  }
  for (auto & pair: renames) {
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
  RecordComp(std::string key, bool ascending)
  : key_(key), ascending_(ascending)
  {
  }

  bool operator()(const RecordBase & a, const RecordBase & b) const noexcept
  {
    if (ascending_) {
      return a.get(key_) < b.get(key_);
    } else {
      return a.get(key_) > b.get(key_);
    }
  }

private:
  std::string key_;
  bool ascending_;
};

void RecordsBase::_sort(std::string key, bool ascending)
{
  std::sort(data_->begin(), data_->end(), RecordComp{key, ascending});
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

  RecordsBase records;

  *records.columns_ = merge_set<std::string>(*columns_, *right_records.columns_);

  bool merge_right_record = how == "right" || how == "outer";
  bool merge_left_record = how == "left" || how == "outer";

  std::unordered_set<const void *> right_records_inserted;

  for (auto & left_record : *data_) {
    if (left_record.columns_.count(join_key) == 0) {
      if (merge_left_record) {
        records.data_->emplace_back();
        RecordBase & merged_record = records.data_->back();
        merged_record._merge(left_record);
      }
      continue;
    }


    bool left_record_inserted = false;
    auto bar = Progress(right_records.data_->size(), progress_label);
    for (auto & right_record : *right_records.data_) {
      bar.tick();
      if (right_record.columns_.count(join_key) == 0) {
        continue;
      }

      if (left_record.get(join_key) == right_record.get(join_key)) {
        right_records_inserted.insert(&right_record);
        left_record_inserted = true;

        records.data_->emplace_back();
        RecordBase & merged_record = records.data_->back();
        merged_record._merge(left_record);
        merged_record._merge(right_record);
        break;
      }
    }
    if (left_record_inserted) {
      continue;
    }
    if (merge_left_record) {
      records.data_->emplace_back();
      RecordBase & merged_record = records.data_->back();
      merged_record._merge(left_record);
    }
  }

  if (merge_right_record) {
    for (auto & right_record: *right_records.data_) {
      if (right_records_inserted.count(&right_record) > 0) {
        continue;
      }
      records.data_->emplace_back();
      RecordBase & merged_record = records.data_->back();
      merged_record._merge(right_record);
    }
  }

  return records;
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
  enum Side {Left, Right};

  auto left_records = RecordsBase(*this);

  bool merge_left = how == "left" || how == "outer";
  bool merge_right = how == "right" || how == "outer";

  RecordsBase merged_records;

  for (auto & left_record: *left_records.data_) {
    left_record.add("side", Left);
  }

  for (auto & right_record: *right_records.data_) {
    right_record.add("side", Right);
  }

  RecordsBase & conat_records = left_records;
  conat_records._concat(right_records);

  for (auto & record : *conat_records.data_) {
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

  conat_records._sort("merge_stamp", true);


  auto find_next_record_and_sub_record =
    [&conat_records](const RecordBase & current_record, const std::string join_key,
      const int records_offset) {

      std::pair<RecordBase *, RecordBase *> next_and_sub_records;
      RecordBase * & next_record = next_and_sub_records.first;
      RecordBase * & sub_record = next_and_sub_records.second;
      next_record = nullptr;
      sub_record = nullptr;

      int records_size = conat_records.data_->size();
      for (int i = records_offset; i < records_size; i++) {
        RecordBase & record = (*conat_records.data_)[i];
        if (next_record != nullptr && sub_record != nullptr) {
          break;
        } else if (!record.get("has_merge_stamp") || !record.get("has_valid_join_key")) {
          continue;
        }

        bool matched = true;
        if (join_key != "") {
          matched = record.get(join_key) == current_record.get(join_key);
        }

        if (matched && record.get("side") == Left && next_record == nullptr) {
          next_record = &record;
        } else if (matched && record.get("side") == Right && sub_record == nullptr) {
          sub_record = &record;
        }
      }
      return next_and_sub_records;
    };

  std::unordered_set<const RecordBase *> added;

  std::size_t records_size = conat_records.data_->size();
  auto bar = Progress(records_size, progress_label);
  for (int i = 0; i < (int)records_size; i++) {
    bar.tick();
    RecordBase & current_record = (*conat_records.data_)[i];
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

    auto next_and_sub_records_pair =
      find_next_record_and_sub_record(current_record, join_key, i + 1);
    RecordBase * next_record_ptr = next_and_sub_records_pair.first;
    RecordBase * sub_record_ptr = next_and_sub_records_pair.second;

    bool has_valid_next_record = next_record_ptr != nullptr &&
      sub_record_ptr != nullptr &&
      next_record_ptr->get("merge_stamp") < sub_record_ptr->get("merge_stamp");
    if (has_valid_next_record || sub_record_ptr == nullptr ||
      added.count(sub_record_ptr) > 0)
    {
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

  merged_records._drop_columns({"side", "has_merge_stamp", "merge_stamp", "has_valid_join_key"});

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
  enum RecordType { Source, Copy, Sink };
  // [python side implementation]
  // assert how in ["inner", "left", "right", "outer"]

  auto source_records_ = RecordsBase(*this);
  auto copy_records_ = copy_records;
  auto sink_records_ = sink_records;

  for (auto & record : *source_records_.data_) {
    record.add("type", Source);
    record.add("timestamp", record.get(source_stamp_key));
  }
  for (auto & record: *copy_records_.data_) {
    record.add("type", Copy);
    record.add("timestamp", record.get(copy_stamp_key));
  }
  for (auto & record: *sink_records_.data_) {
    record.add("type", Sink);
    record.add("timestamp", record.get(sink_stamp_key));
  }

  auto & records = source_records_;
  records._concat(copy_records_);
  records._concat(sink_records_);
  records._sort("timestamp", false);

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
      for (auto & processing_record_: processing_records_) {
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
  for (auto & record: *records.data_) {
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
      for (auto & processing_record: records_with_same_source_addrs) {
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
      for (auto & processing_record: records_with_same_source_addrs) {

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
