cmake_minimum_required(VERSION 3.10)

project(OSYHacker)

set(CMAKE_CXX_STANDARD 17)

add_library(progtest_solver SHARED IMPORTED)

set_target_properties(progtest_solver PROPERTIES IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/i686-w64-mingw32/libprogtest_solver.a)

add_executable(OSYHacker common.h solution.cpp progtest_solver.h sample_tester.cpp sample_tester.h)

target_link_libraries(OSYHacker pthread progtest_solver)
