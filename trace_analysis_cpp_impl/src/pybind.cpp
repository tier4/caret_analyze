#include "pybind11/pybind11.h"
#include "pybind11/stl.h"
#include "pybind11/functional.h"
#include "trace_analysis_cpp_impl/record.hpp"

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)


namespace py = pybind11;

PYBIND11_MODULE(record_cpp_impl, m) {
  py::class_<RecordBase>(m, "RecordBase")
  .def(py::init())
  .def(
    py::init(
      [](std::unordered_map<std::string, uint64_t> init) {
        return new RecordBase(init);
      })
  )
  .def("change_dict_key", &RecordBase::change_dict_key)
  .def("equals", &RecordBase::equals)
  .def("_merge", &RecordBase::_merge)
  .def("add", &RecordBase::add)
  .def("_drop_columns", &RecordBase::_drop_columns)
  .def("get", &RecordBase::get)
  .def_property_readonly("data", &RecordBase::get_data)
  .def_property_readonly("columns", &RecordBase::get_columns);

  py::class_<RecordsBase>(m, "RecordsBase")
  .def(py::init())
  .def(
    py::init(
      [](const RecordsBase & init) {
        return new RecordsBase(init);
      })
  )
  .def(
    py::init(
      [](std::vector<RecordBase> init) {
        return new RecordsBase(init);
      })
  )
  .def("append", &RecordsBase::append)
  .def("equals", &RecordsBase::equals)
  .def("_drop_columns", &RecordsBase::_drop_columns)
  .def("_rename_columns", &RecordsBase::_rename_columns)
  .def("_filter", &RecordsBase::_filter)
  .def("_concat", &RecordsBase::_concat)
  .def("_sort", &RecordsBase::_sort)
  .def("_merge", &RecordsBase::_merge)
  .def("_merge_sequencial", &RecordsBase::_merge_sequencial)
  .def(
    "_merge_sequencial_for_addr_track",
    &RecordsBase::_merge_sequencial_for_addr_track)
  .def_property_readonly("_data", &RecordsBase::get_data)
  .def_property_readonly("columns", &RecordsBase::get_columns);

#ifdef VERSION_INFO
  m.attr("__version__") = MACRO_STRINGIFY(VERSION_INFO);
#else
  m.attr("__version__") = "dev";
#endif
}
