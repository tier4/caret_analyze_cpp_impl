cmake_minimum_required(VERSION 3.8)
project(caret_analyze_cpp_impl)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

find_package(ament_cmake REQUIRED)
find_package(Python3 REQUIRED COMPONENTS Interpreter Development)

find_package(pybind11_vendor REQUIRED)
find_package(pybind11 REQUIRED)
find_package(yaml_cpp_vendor REQUIRED)

include_directories(
  include
)

set(srcs
  "src/record.cpp"
  "src/records_base.cpp"
  "src/records_vector_impl.cpp"
  "src/records_map_impl.cpp"
  "src/iterator_base.cpp"
  "src/iterator_vector_impl.cpp"
  "src/iterator_map_impl.cpp"
  "src/column_manager.cpp"
  "src/file.cpp"
)

pybind11_add_module(record_cpp_impl
  ${srcs}
  "src/pybind.cpp"
)

set(LIBRARY_DESTINATION
  "lib/python3.${Python3_VERSION_MINOR}/site-packages"
)
install(

  TARGETS record_cpp_impl
  EXPORT export_record_cpp_impl
  LIBRARY DESTINATION ${LIBRARY_DESTINATION}
  ARCHIVE DESTINATION lib
  RUNTIME DESTINATION bin
  INCLUDES DESTINATION include
)

add_library(
  ${PROJECT_NAME} SHARED
  ${srcs}
)

ament_target_dependencies(${PROJECT_NAME}
  yaml_cpp_vendor
)

install(
  TARGETS ${PROJECT_NAME}
  EXPORT ${PROJECT_NAME}
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib
  RUNTIME DESTINATION bin
)


add_executable(merge_sample
src/merge_sample.cpp
)

target_link_libraries(merge_sample
  ${PROJECT_NAME}
)

target_link_libraries(record_cpp_impl
  PRIVATE ${PROJECT_NAME}
)

ament_export_libraries(${PROJECT_NAME})


if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)

  ament_lint_auto_find_test_dependencies()

  find_package(ament_cmake_gtest REQUIRED)

  ament_add_gmock(test_vector_impl
    test/test_records_vector_impl.cpp
  )
  target_link_libraries(test_vector_impl ${PROJECT_NAME})
endif()

ament_package()
