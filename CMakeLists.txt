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
  CMAKE_CXX_FLAGS_SANITIZE		  CMAKE_EXE_LINKER_FLAGS_SANITIZE
  CMAKE_C_FLAGS_SANITIZE		  CMAKE_SHARED_LINKER_FLAGS_SANITIZE
  )

set(CMAKE_BUILD_TYPE "${CMAKE_BUILD_TYPE}" CACHE STRING
  "Choose the type of build, options are: None Debug Release RelWithDebInfo MinSizeRel Sanitize."
  FORCE)

############################### External Projects ###############################
set(VELOX_INCLUDE_DIRS /Users/sunny/Documents/velox)
set(CMAKE_MODULE_PATH ${VELOX_INCLUDE_DIRS} ${CMAKE_MODULE_PATH})

include(ExternalProject)

if(NOT DEFINED BOSS_BUILD_FROM_SOURCE)
    set(BOSS_BUILD_FROM_SOURCE ON CACHE BOOL "should boss core be built from source rather than downloading a pre-built release")
endif()
if(BOSS_BUILD_FROM_SOURCE)
  if(NOT DEFINED BOSS_SOURCE_BRANCH)
    set(BOSS_SOURCE_BRANCH master CACHE STRING "if boss core is built from source, which thread should be fetched")
  endif()
  if(NOT DEFINED BOSS_SOURCE_REPOSITORY)
    if(NOT GITHUB_TOKEN)
      message( FATAL_ERROR "You need to pass -DGITHUB_TOKEN=... to pass a github token to fetch the boss release" )
    endif()
    set(BOSS_SOURCE_REPOSITORY https://little-big-h:${GITHUB_TOKEN}@github.com/symbol-store/BOSS.git)
  endif()
  message("getting boss sources from ${BOSS_SOURCE_REPOSITORY} branch ${BOSS_SOURCE_BRANCH}")
  ExternalProject_Add(BOSS
    GIT_REPOSITORY ${BOSS_SOURCE_REPOSITORY}
    DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
    GIT_TAG ${BOSS_SOURCE_BRANCH}
		CMAKE_ARGS -DCMAKE_INSTALL_PREFIX=${BOSSVeloxEngine_BINARY_DIR}/deps $<IF:$<CONFIG:>,,-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}> -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}  -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER} -DCMAKE_CXX_FLAGS=${CMAKE_CXX_FLAGS}
    )
else()
ExternalProject_Add(BOSS
	URL https://api.github.com/repos/symbol-store/BOSS/releases/assets/48287799
  HTTP_HEADER "Authorization: token ${GITHUB_TOKEN}" "Accept: application/octet-stream"
  DOWNLOAD_DIR $ENV{HOME}/.cmake-downloads/${CMAKE_PROJECT_NAME}
  SOURCE_DIR ${BOSSVeloxEngine_BINARY_DIR}/deps
  CONFIGURE_COMMAND ""
  BUILD_COMMAND ""
  INSTALL_COMMAND ""
  )
endif()


#################################### Targets ####################################

set(ImplementationFiles Source/BOSSVeloxEngine.cpp Source/BOSSQueryBuilder.cpp)

if(MSVC)
  # not making any difference on Windows
  # and helps Visual Studio to parse it correctly as a target
  set(LibraryType SHARED)
else()
  set(LibraryType MODULE)
endif(MSVC)

add_library(BOSSVeloxEngine ${LibraryType} ${ImplementationFiles})

target_include_directories(BOSSVeloxEngine PUBLIC
        ${VELOX_INCLUDE_DIRS}
        ${VELOX_INCLUDE_DIRS}/velox/
        ${VELOX_INCLUDE_DIRS}/velox/external/xxhash/
        ${VELOX_INCLUDE_DIRS}/third_party/
        ${VELOX_INCLUDE_DIRS}/third_party/xsimd/
        ${VELOX_INCLUDE_DIRS}/third_party/xsimd/include/)
add_subdirectory(${VELOX_INCLUDE_DIRS} velox)
target_link_libraries(
        BOSSVeloxEngine
        velox_aggregates
        velox_type
        velox_vector
        velox_vector_test_lib
        velox_exec
        velox_exec_test_lib
        velox_tpch_connector
        velox_memory
        velox_dwio_common
        velox_dwio_common_exception
        velox_dwio_parquet_reader
        velox_dwio_type_fbhive
        velox_dwio_common_test_utils
        velox_hive_connector
        velox_exception
        velox_process
        velox_serialization
        velox_encode
        velox_caching
        ${FOLLY_WITH_DEPENDENCIES}
        ${FOLLY_BENCHMARK}
        ${FMT})

target_compile_options(BOSSVeloxEngine SYSTEM PUBLIC -DARROW_STATIC -msse4.2)
if (UNIX AND NOT APPLE)
  target_link_libraries(BOSSVeloxEngine uuid)
endif ()
if (NOT WIN32)
  target_link_libraries(BOSSVeloxEngine dl)
endif (NOT WIN32)

set_property(TARGET BOSSVeloxEngine PROPERTY CXX_STANDARD 17)
if(MSVC)
  target_compile_options(BOSSVeloxEngine PUBLIC "/Zc:__cplusplus")
  target_compile_options(BOSSVeloxEngine PUBLIC "/EHsc")
endif(MSVC)
target_include_directories(BOSSVeloxEngine SYSTEM PUBLIC ${BOSSVeloxEngine_BINARY_DIR}/deps/include)
add_dependencies(BOSSVeloxEngine BOSS)

set_target_properties(BOSSVeloxEngine PROPERTIES INSTALL_RPATH_USE_LINK_PATH TRUE)
install(TARGETS BOSSVeloxEngine LIBRARY DESTINATION lib)
