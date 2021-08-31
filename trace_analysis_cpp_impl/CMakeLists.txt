cmake_minimum_required(VERSION 3.8)
project(trace_analysis_cpp_impl)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)

add_subdirectory(pybind11)
include_directories(
  include
  pybind11/include)
pybind11_add_module(cmake_example src/pybind.cpp src/record.cpp)
install(
  TARGETS cmake_example
  EXPORT export_cmake_example
  LIBRARY DESTINATION lib/python3.8/site-packages
  ARCHIVE DESTINATION lib
  RUNTIME DESTINATION bin
  INCLUDES DESTINATION include
)


if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  ament_lint_auto_find_test_dependencies()
endif()

ament_package()
