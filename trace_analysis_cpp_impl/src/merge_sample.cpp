#include <iostream>
#include "trace_analysis_cpp_impl/record.hpp"

void print_records(const RecordsBase & records);
void run_merge(std::string how);
void run_merge_with_drop(std::string how);
void run_merge_sequencial_for_addr_track();
void run_merge_sequencial_with_key(std::string how);
void run_merge_sequencial_with_loss(std::string how);

int main(int argc, char ** argvs)
{
  (void) argc;
  (void) argvs;
//   run_merge("inner");
//   run_merge("left");
//   run_merge("right");
//   run_merge("outer");

//   run_merge_with_drop("inner");
//   run_merge_with_drop("left");
//   run_merge_with_drop("right");
//   run_merge_with_drop("outer");

//   run_merge_sequencial_for_addr_track();

//   run_merge_sequencial_with_key("inner");
  run_merge_sequencial_with_loss("inner");
  return 0;
}

void print_records(const RecordsBase & records)
{
  for (auto & record : *records.data_) {
    for (auto & pair : record.data_) {
      std::cout << pair.first << " " << pair.second << ", ";
    }
    std::cout << std::endl;
  }
}


void run_merge(std::string how)
{
  auto left_records = RecordsBase();
  auto & left_data = *left_records.data_;
  left_data.emplace_back(RecordBase({{"stamp", 0}, {"value", 1}}));
  left_data.emplace_back(RecordBase({{"stamp", 2}, {"value", 2}}));
  left_data.emplace_back(RecordBase({{"stamp", 3}, {"value", 3}}));

  auto right_records = RecordsBase();
  auto & right_data = *right_records.data_;
  right_data.emplace_back(RecordBase({{"stamp_", 4}, {"value", 2}}));
  right_data.emplace_back(RecordBase({{"stamp_", 5}, {"value", 3}}));
  right_data.emplace_back(RecordBase({{"stamp_", 6}, {"value", 4}}));

  auto merged_records = left_records._merge(
    right_records,
    "value",
    how
  );

  print_records(merged_records);
}


void run_merge_with_drop(std::string how)
{
  auto left_records = RecordsBase();
  auto & left_data = *left_records.data_;
  left_data.emplace_back(RecordBase({{"other_stamp", 4}, {"stamp", 1}, {"value", 1}}));
  left_data.emplace_back(RecordBase({{"other_stamp", 8}}));
  left_data.emplace_back(RecordBase({{"other_stamp", 12}, {"stamp", 9}, {"value", 2}}));
  left_data.emplace_back(RecordBase({{"other_stamp", 16}}));

  auto right_records = RecordsBase();
  auto & right_data = *right_records.data_;
  right_data.emplace_back(RecordBase({{"other_stamp_", 2}, {"stamp_", 3}, {"value", 2}}));
  right_data.emplace_back(RecordBase({{"other_stamp_", 6}, {"stamp_", 7}, {"value", 1}}));
  right_data.emplace_back(RecordBase({{"other_stamp_", 10}}));
  right_data.emplace_back(RecordBase({{"other_stamp_", 14}}));

  auto merged_records = left_records._merge(
    right_records,
    "value",
    how
  );

  print_records(merged_records);
}

void run_merge_sequencial_for_addr_track()
{
  auto source_records = RecordsBase();
  auto & source_data = *source_records.data_;
  source_data.emplace_back(RecordBase({{"source_addr", 1}, {"source_stamp", 0}}));
  source_data.emplace_back(RecordBase({{"source_addr", 1}, {"source_stamp", 10}}));
  source_data.emplace_back(RecordBase({{"source_addr", 3}, {"source_stamp", 20}}));

  auto copy_records = RecordsBase();
  auto & copy_data = *copy_records.data_;
  copy_data.emplace_back(RecordBase({{"addr_from", 1}, {"addr_to", 13}, {"copy_stamp", 1}}));
  copy_data.emplace_back(RecordBase({{"addr_from", 1}, {"addr_to", 13}, {"copy_stamp", 11}}));
  copy_data.emplace_back(RecordBase({{"addr_from", 3}, {"addr_to", 13}, {"copy_stamp", 21}}));

  auto sink_records = RecordsBase();
  auto & sink_data = *sink_records.data_;
  sink_data.emplace_back(RecordBase({{"sink_addr", 13}, {"sink_stamp", 2}}));
  sink_data.emplace_back(RecordBase({{"sink_addr", 1}, {"sink_stamp", 3}}));
  sink_data.emplace_back(RecordBase({{"sink_addr", 13}, {"sink_stamp", 12}}));
  sink_data.emplace_back(RecordBase({{"sink_addr", 13}, {"sink_stamp", 22}}));

  auto merged_records = source_records._merge_sequencial_for_addr_track(
    "source_stamp",
    "source_addr",
    copy_records,
    "copy_stamp",
    "addr_from",
    "addr_to",
    sink_records,
    "sink_stamp",
    "sink_addr"
  );

  print_records(merged_records);
}

void run_merge_sequencial_with_key(std::string how)
{
  auto left_records = RecordsBase();
  auto & left_data = *left_records.data_;
  left_data.emplace_back(RecordBase({{"key", 1}, {"stamp", 0}}));
  left_data.emplace_back(RecordBase({{"key", 2}, {"stamp", 1}}));
  left_data.emplace_back(RecordBase({{"key", 1}, {"stamp", 6}}));
  left_data.emplace_back(RecordBase({{"key", 2}, {"stamp", 7}}));

  auto right_records = RecordsBase();
  auto & right_data = *right_records.data_;
  right_data.emplace_back(RecordBase({{"key", 2}, {"sub_stamp", 2}}));
  right_data.emplace_back(RecordBase({{"key", 1}, {"sub_stamp", 3}}));
  right_data.emplace_back(RecordBase({{"key", 1}, {"sub_stamp", 4}}));
  right_data.emplace_back(RecordBase({{"key", 2}, {"sub_stamp", 5}}));

  auto merged_records = left_records._merge_sequencial(
    right_records, "stamp", "sub_stamp", "key",
    how);

  print_records(merged_records);
}


void run_merge_sequencial_with_loss(std::string how)
{
  auto left_records = RecordsBase();
  auto & left_data = *left_records.data_;
  left_data.emplace_back(RecordBase({{"other_stamp", 4}, {"stamp", 1}, {"value", 1}}));
  left_data.emplace_back(RecordBase({{"other_stamp", 8}}));
  left_data.emplace_back(RecordBase({{"other_stamp", 12}, {"stamp", 9}, {"value", 1}}));
  left_data.emplace_back(RecordBase({{"other_stamp", 16}}));

  auto right_records = RecordsBase();
  auto & right_data = *right_records.data_;
  right_data.emplace_back(RecordBase({{"other_stamp_", 2}, {"stamp_", 3}, {"value", 1}}));
  right_data.emplace_back(RecordBase({{"other_stamp_", 6}, {"stamp_", 7}, {"value", 1}}));
  right_data.emplace_back(RecordBase({{"other_stamp_", 10}}));
  right_data.emplace_back(RecordBase({{"other_stamp_", 14}}));

  auto merged_records = left_records._merge_sequencial(
    right_records, "stamp", "sub_stamp", "key",
    how);

  print_records(merged_records);
}
