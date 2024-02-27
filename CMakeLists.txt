cmake_minimum_required(VERSION 3.10)
project(BOSSVeloxEngine)

############################## Custom build modes ###############################
set(CMAKE_CXX_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
        CACHE STRING
        "Flags used by the C++ compiler during Sanitize builds."
        FORCE)
set(CMAKE_C_FLAGS_SANITIZE "-fno-omit-frame-pointer -fsanitize=address,signed-integer-overflow,null,alignment,bounds,function,return,vla-bound -O0 -g"
        CACHE STRING
        "Flags used by the C compiler during Sanitize builds."
        FORCE)
set(CMAKE_EXE_LINKER_FLAGS_SANITIZE
        ${CMAKE_EXE_LINKER_FLAGS_DEBUG} CACHE STRING
        "Flags used for linking binaries during Sanitize builds."
        FORCE)
set(CMAKE_SHARED_LINKER_FLAGS_SANITIZE
        ${CMAKE_SHARED_LINKER_FLAGS_DEBUG} CACHE STRING
        "Flags used by the shared libraries linker during Sanitize builds."
        FORCE)
mark_as_advanced(
        CMAKE_CXX_FLAGS_SANITIZE CMAKE_EXE_LINKER_FLAGS_SANITIZE
        CMAKE_C_FLAGS_SANITIZE CMAKE_SHARED_LINKER_FLAGS_SANITIZE
)

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
        "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Sanitize."
        FORCE)

############################### External Projects ###############################
if (NOT VELOX_DIRS)
    message(FATAL_ERROR "You need to pre-build Velox Project and set VELOX_DIRS")
endif ()
set(CMAKE_MODULE_PATH ${VELOX_DIRS} ${CMAKE_MODULE_PATH})

set(CMAKE_POSITION_INDEPENDENT_CODE TRUE) # make sure all dependent libraries are compiled with that option

if (MSVC)
    set(BOSS_LINK_LIBRARY_PREFIX ${CMAKE_LINK_LIBRARY_PREFIX})
    set(BOSS_LINK_LIBRARY_SUFFIX ${CMAKE_LINK_LIBRARY_SUFFIX})
else ()
    set(BOSS_LINK_LIBRARY_PREFIX ${CMAKE_SHARED_LIBRARY_PREFIX})
    set(BOSS_LINK_LIBRARY_SUFFIX ${CMAKE_SHARED_LIBRARY_SUFFIX})
endif (MSVC)

include(ExternalProject)

# Core BOSS
if (NOT DEFINED BUILD_BOSS_FROM_SOURCE)
    set(BUILD_BOSS_FROM_SOURCE ON)
endif ()
if (BUILD_BOSS_FROM_SOURCE)
    if (NOT DEFINED BOSS_SOURCE_BRANCH)
        set(BOSS_SOURCE_BRANCH main)
    endif ()
    if (NOT DEFINED BOSS_SOURCE_REPOSITORY)
        if (NOT GITHUB_TOKEN)
            message(FATAL_ERROR "You need to pass -DGITHUB_TOKEN=... to pass a github token to fetch the boss release")
        endif ()
        set(BOSS_SOURCE_REPOSITORY https://little-big-h:${GITHUB_TOKEN}@github.com/symbol-store/BOSS.git)
    endif ()
    message("getting boss sources from ${BOSS_SOURCE_REPOSITORY}")
    ExternalProject_Add(BOSS
            GIT_REPOSITORY ${BOSS_SOURCE_REPOSITORY}
            GIT_TAG ${BOSS_SOURCE_BRANCH}
            GIT_SHALLOW ON
            DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
            CMAKE_ARGS -DCMAKE_VERBOSE_MAKEFILE=${CMAKE_VERBOSE_MAKEFILE} -DCMAKE_INSTALL_PREFIX=${BOSSVeloxEngine_BINARY_DIR}/deps $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}>
            -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER} -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
            BUILD_BYPRODUCTS ${BOSSVeloxEngine_BINARY_DIR}/deps/lib/${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX}
            )
else ()
    ExternalProject_Add(BOSS
            URL https://api.github.com/repos/symbol-store/BOSS/releases/assets/48287799
            HTTP_HEADER "Authorization: token ${GITHUB_TOKEN}" "Accept: application/octet-stream"
            DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
            SOURCE_DIR ${BOSSVeloxEngine_BINARY_DIR}/deps
            CONFIGURE_COMMAND ""
            BUILD_COMMAND ""
            INSTALL_COMMAND ""
            BUILD_BYPRODUCTS ${BOSSVeloxEngine_BINARY_DIR}/deps/lib/${BOSS_LINK_LIBRARY_PREFIX}BOSS${BOSS_LINK_LIBRARY_SUFFIX}
            )
endif ()

include_directories(${BOSSVeloxEngine_BINARY_DIR}/deps/include)
link_directories(${BOSSVeloxEngine_BINARY_DIR}/deps/lib)

#################################### VeloxEngine Plugin ####################################

set(ImplementationFiles Source/BOSSVeloxEngine.cpp Source/BridgeVelox.cpp Source/BossConnector.cpp
        ${VELOX_DIRS}/velox/exec/tests/utils/PlanBuilder.cpp
        ${VELOX_DIRS}/velox/exec/tests/utils/Cursor.cpp)

if (MSVC)
    # not making any difference on Windows
    # and helps Visual Studio to parse it correctly as a target
    set(LibraryType SHARED)
else ()
    set(LibraryType MODULE)
endif (MSVC)

add_library(BOSSVeloxEngine ${LibraryType} ${ImplementationFiles})

target_include_directories(BOSSVeloxEngine PUBLIC
        ${VELOX_DIRS}
        ${VELOX_DIRS}/velox/
        ${VELOX_DIRS}/velox/external/xxhash/
        ${VELOX_DIRS}/third_party/
        ${VELOX_DIRS}/third_party/xsimd/
        ${VELOX_DIRS}/third_party/xsimd/include/)

set(VELOX_BUILD_TESTING OFF)
set(VELOX_CODEGEN_SUPPORT OFF)
set(VELOX_ENABLE_HIVE_CONNECTOR OFF)
set(VELOX_ENABLE_TPCH_CONNECTOR OFF)
set(VELOX_ENABLE_SPARK_FUNCTIONS OFF)

add_subdirectory(${VELOX_DIRS} velox)
target_link_libraries(
        BOSSVeloxEngine
        velox_aggregates
        velox_functions_prestosql
        velox_parse_parser
        atomic
)
#copy from VLDB submission artifact
target_compile_options(BOSSVeloxEngine PUBLIC -DARROW_STATIC -msse4.2 -DFOLLY_CFG_NO_COROUTINES -DFOLLY_HAS_COROUTINES=0)
if (UNIX AND NOT APPLE)
    target_link_libraries(BOSSVeloxEngine uuid)
endif ()
if (NOT WIN32)
    target_link_libraries(BOSSVeloxEngine dl)
endif (NOT WIN32)

set_property(TARGET BOSSVeloxEngine PROPERTY CXX_STANDARD 20)
if (MSVC)
    target_compile_options(BOSSVeloxEngine PUBLIC "/Zc:__cplusplus")
    target_compile_options(BOSSVeloxEngine PUBLIC "/EHsc")
    # to link against all needed symbols for loading BulKBOSS as a plugin
    target_link_libraries(${targetName} ${BOSS_BINARY_DIR}/BOSS.lib)
endif (MSVC)
target_include_directories(BOSSVeloxEngine SYSTEM PUBLIC ${BOSSVeloxEngine_BINARY_DIR}/deps/include)
add_dependencies(BOSSVeloxEngine BOSS)

################################ ITT module interface ################################

set(VTune_DIR "" CACHE PATH "Where to look for VTune installation")
find_path(ITT_NOTIFY_INCLUDE_DIR "ittnotify.h" HINTS ${VTune_DIR}/include)

add_library(ITTNotifySupport INTERFACE)
target_sources(ITTNotifySupport INTERFACE ${PROJECT_SOURCE_DIR}/Benchmarks/ITTNotifySupport.hpp)
target_include_directories(ITTNotifySupport INTERFACE ${PROJECT_SOURCE_DIR}/Benchmarks/)
if(ITT_NOTIFY_INCLUDE_DIR)
  message(VERBOSE "found itt notify header in ${ITT_NOTIFY_INCLUDE_DIR}")
  target_include_directories(ITTNotifySupport SYSTEM INTERFACE ${ITT_NOTIFY_INCLUDE_DIR})
  target_link_libraries(ITTNotifySupport INTERFACE ${ITT_NOTIFY_INCLUDE_DIR}/../lib64/libittnotify${CMAKE_STATIC_LIBRARY_SUFFIX})
  if(NOT WIN32)
    target_link_libraries(ITTNotifySupport dl)
  endif(NOT WIN32)
  if (NOT CMAKE_BUILD_TYPE STREQUAL "Release")
    target_compile_options(ITTNotifySupport INTERFACE -DWITH_ITT_NOTIFY)
  else()
    target_compile_options(ITTNotifySupport INTERFACE -DINTEL_NO_ITTNOTIFY_API)
  endif()
endif(ITT_NOTIFY_INCLUDE_DIR)

################################ Benchmark executable ################################

ExternalProject_Add(googlebenchmark
	URL "https://github.com/google/benchmark/archive/refs/tags/v1.8.3.zip"
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
		CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${BOSSVeloxEngine_BINARY_DIR}/deps -DBENCHMARK_DOWNLOAD_DEPENDENCIES=ON -DBENCHMARK_ENABLE_GTEST_TESTS=OFF $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}>
                   -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
                   -DCMAKE_CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE} -DCMAKE_CXX_FLAGS_RELWITHDEBINFO=${CMAKE_CXX_FLAGS_RELWITHDEBINFO}
  BUILD_BYPRODUCTS ${BOSSVeloxEngine_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}benchmark${CMAKE_STATIC_LIBRARY_SUFFIX} 
                   ${BOSSVeloxEngine_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}benchmark_main${CMAKE_STATIC_LIBRARY_SUFFIX}
  )
  
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

set(BenchmarkFiles Benchmarks/VeloxBenchmarks.cpp)

add_executable(VeloxBenchmarks ${BenchmarkFiles})
target_link_libraries(VeloxBenchmarks Threads::Threads)
if(WIN32)
    target_link_libraries(VeloxBenchmarks shlwapi.lib)  
endif(WIN32)
target_link_libraries(VeloxBenchmarks ITTNotifySupport)
add_dependencies(VeloxBenchmarks googlebenchmark)
target_link_libraries(VeloxBenchmarks benchmark)
target_link_libraries(VeloxBenchmarks benchmark_main)
    
target_link_libraries(VeloxBenchmarks ${BOSSVeloxEngine_BINARY_DIR}/deps/lib/${CMAKE_SHARED_LIBRARY_PREFIX}BOSS${CMAKE_SHARED_LIBRARY_SUFFIX})
add_dependencies(VeloxBenchmarks BOSS)
add_dependencies(VeloxBenchmarks BOSSVeloxEngine)

target_compile_options(VeloxBenchmarks PUBLIC -DBENCHMARK_STATIC_DEFINE)
target_link_libraries(VeloxBenchmarks Threads::Threads)
set_target_properties(VeloxBenchmarks PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON CXX_EXTENSIONS ON)
if(MSVC)
    target_compile_options(VeloxBenchmarks PUBLIC "/Zc:__cplusplus")
    target_compile_options(VeloxBenchmarks PUBLIC "/EHsc")
endif()
if(UNIX AND NOT APPLE)
    target_link_libraries(VeloxBenchmarks rt)
    target_link_libraries(VeloxBenchmarks dl)
endif()

################################ Installation ################################

set_target_properties(BOSSVeloxEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS BOSSVeloxEngine LIBRARY DESTINATION lib)
install(TARGETS VeloxBenchmarks RUNTIME DESTINATION bin)
