cmake_minimum_required(VERSION 3.16)
project(FlexQL CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(FLEXQL_EXPERIMENTAL "Enable experimental query-path optimizations (OFF by default)" OFF)
if(FLEXQL_EXPERIMENTAL)
  add_compile_definitions(FLEXQL_EXPERIMENTAL=1)
endif()

# If the user enables coverage/profile instrumentation externally (common via CMAKE_CXX_FLAGS),
# the final link may require libgcov explicitly (e.g., with -flto).
string(CONCAT _FLEXQL_ALL_FLAGS " " "${CMAKE_C_FLAGS}" " " "${CMAKE_CXX_FLAGS}" " " "${CMAKE_EXE_LINKER_FLAGS}")
if(_FLEXQL_ALL_FLAGS MATCHES "(--coverage|-fprofile-arcs|-ftest-coverage)")
  link_libraries(gcov)
endif()

# -ffast-math omitted: DECIMAL / double comparisons must match SQL expectations.
add_compile_options(-O3 -march=native -mtune=native -flto -DNDEBUG -funroll-loops -fomit-frame-pointer -fno-exceptions -fno-rtti)
add_link_options(-flto)
include_directories(include)
add_library(flexql-lib OBJECT src/client/flexql.cpp)
add_executable(flexql-server src/server/main.cpp $<TARGET_OBJECTS:flexql-lib>)
add_executable(flexql-client src/client/main.cpp $<TARGET_OBJECTS:flexql-lib>)
add_executable(benchmark benchmark/benchmark_flexql.cpp $<TARGET_OBJECTS:flexql-lib>)
add_executable(microbench_exec src/client/microbench_exec.cpp $<TARGET_OBJECTS:flexql-lib>)
