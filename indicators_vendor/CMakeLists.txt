cmake_minimum_required(VERSION 3.8)
project(indicators_vendor)

if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# find dependencies
find_package(ament_cmake REQUIRED)
# uncomment the following section in order to fill in
# further dependencies manually.
# find_package(<dependency> REQUIRED)

include(ExternalProject)

externalproject_add(indicators-2.2
    URL https://github.com/p-ranav/indicators/archive/refs/tags/v2.2.tar.gz
    URL_HASH SHA256=b768f1b7ca64a413503f72d5460cc617c1458c17fb7a8c0ee503d753e1f20d03
    TIMEOUT 600
    CMAKE_ARGS
      -DCMAKE_INSTALL_PREFIX=${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}_install
      -Wno-dev
      -Wall
      -Wextra
      -Wpedantic
)

install(
    DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}_install/
    DESTINATION ${CMAKE_INSTALL_PREFIX}
)

ament_export_include_directories(include)

if(BUILD_TESTING)
  find_package(ament_lint_auto REQUIRED)
  # the following line skips the linter which checks for copyrights
  # uncomment the line when a copyright and license is not present in all source files
  #set(ament_cmake_copyright_FOUND TRUE)
  # the following line skips cpplint (only works in a git repo)
  # uncomment the line when this package is not in a git repo
  #set(ament_cmake_cpplint_FOUND TRUE)
  ament_lint_auto_find_test_dependencies()
endif()

ament_package()
