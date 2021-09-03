#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <string>
#include <limits>

#include "trace_analysis_cpp_impl/record.hpp"


RecordBase::RecordBase(std::unordered_map<std::string, uint64_t> init)
{
  for (auto & pair : init) {
    add(pair.first, pair.second);
  }
}

RecordBase::RecordBase()
{
}

RecordBase::RecordBase(const RecordBase & record)
: data_(record.data_), columns_(record.columns_)
{
}

std::unordered_map<std::string, uint64_t> RecordBase::get_data() const
{
  return data_;
}

uint64_t RecordBase::get(std::string key) const
{
  return data_.at(key);
}

void RecordBase::change_dict_key(std::string key_from, std::string key_to)
{
  data_.insert(std::make_pair(key_to, data_[key_from]));
  data_.erase(key_from);
  columns_.erase(key_from);
  columns_.insert(key_to);
}

void RecordBase::_drop_columns(std::vector<std::string> keys)
{
  for (auto & key : keys) {
    data_.erase(key);
    columns_.erase(key);
  }
}

bool RecordBase::equals(const RecordBase & other) const
{
  return this->data_ == other.data_;
}

void RecordBase::add(std::string key, uint64_t stamp)
{
  columns_.insert(key);
  data_.insert(std::make_pair(key, stamp));
}

void RecordBase::_merge(const RecordBase & other)
{
  for (auto & pair: other.data_) {
    add(pair.first, pair.second);
  }
}

std::unordered_set<std::string> RecordBase::get_columns() const
{
  return columns_;
}
