#
# (c) 2019 Copyright, Real-Time Innovations, Inc.  All rights reserved.
#
#  RTI grants Licensee a license to use, modify, compile, and create derivative
#  works of the Software.  Licensee has the right to distribute object form
#  only for use with RTI products.  The Software is provided "as is", with no
#  warranty of any type, including any warranty for fitness for any purpose.
#  RTI is under no obligation to maintain or support the Software.  RTI shall
#  not be liable for any incidental or consequential damages arising out of the
#  use or inability to use the software.
#
cmake_minimum_required(VERSION 3.11)
project (KyotoCabinetEval)

# Find RTI Connext dependencies
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CONNEXTDDS_DIR}/resource/cmake")

find_package(
    RTIConnextDDS "6.0.0"
    REQUIRED
    COMPONENTS
        core
        routing_service
)

find_package(
    RTICodeGenerator "3.0.0"
    REQUIRED
)

set(KYOTOCABINET_LIB_NAME "kyotocabinet")

find_library(
    kyoto_cabinet_lib
    NAMES ${KYOTOCABINET_LIB_NAME}
    PATHS "${KYOTOCABINET_DIR}"
)

# Create the directory that's going to contain the generated code files
add_custom_target(
    generated_code_dir ALL
    COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_CURRENT_BINARY_DIR}/generated")

set(REDUCED_DCPSPUB_CPP_SOURCES
    "${CMAKE_CURRENT_BINARY_DIR}/generated/ReducedDCPSPublication.cxx"
    "${CMAKE_CURRENT_BINARY_DIR}/generated/ReducedDCPSPublicationPlugin.cxx"
)

# Generate type plug-in files for the storage type
add_custom_command(
    OUTPUT ${REDUCED_DCPSPUB_CPP_SOURCES}
    MAIN_DEPENDENCY "${CMAKE_CURRENT_SOURCE_DIR}/ReducedDCPSPublication.idl"
    COMMAND ${RTICODEGEN} -language C++03 -update typefiles -unboundedSupport
            -platform ${CONNEXTDDS_ARCH} -d "${CMAKE_BINARY_DIR}/generated" 
            "${CMAKE_CURRENT_SOURCE_DIR}/ReducedDCPSPublication.idl"
)

add_custom_target(
    generated_code 
    DEPENDS 
        ${REDUCED_DCPSPUB_CPP_SOURCES}
        generated_code_dir
)

# Define the library that will provide the storage writer plugin
add_library(
    KyotoCabinetWriter MODULE
    "${CMAKE_CURRENT_BINARY_DIR}/generated/ReducedDCPSPublication.cxx"
    "${CMAKE_CURRENT_BINARY_DIR}/generated/ReducedDCPSPublicationPlugin.cxx"
    "${CMAKE_CURRENT_SOURCE_DIR}/KyotoCabinetWriter.cxx"
)

set_target_properties(
    KyotoCabinetWriter 
    PROPERTIES CXX_STANDARD 11
)

target_link_libraries(
    KyotoCabinetWriter
    RTIConnextDDS::routing_service_infrastructure
    RTIConnextDDS::cpp2_api
    ${CONNEXTDDS_EXTERNAL_LIBS}
    "${kyoto_cabinet_lib}"
)

target_include_directories(
    KyotoCabinetWriter
    PUBLIC 
        "${CMAKE_CURRENT_BINARY_DIR}"
        "${CMAKE_CURRENT_BINARY_DIR}/generated"
)

add_dependencies(KyotoCabinetWriter generated_code)

# Define the library that will provide the storage reader plugin
add_library(
    KyotoCabinetReader MODULE
    "${CMAKE_CURRENT_BINARY_DIR}/generated/ReducedDCPSPublication.cxx"
    "${CMAKE_CURRENT_BINARY_DIR}/generated/ReducedDCPSPublicationPlugin.cxx"
    "${CMAKE_CURRENT_SOURCE_DIR}/KyotoCabinetReader.cxx"
)

set_target_properties(
    KyotoCabinetReader 
    PROPERTIES CXX_STANDARD 11
)

target_link_libraries(
    KyotoCabinetReader
    RTIConnextDDS::routing_service_infrastructure
    RTIConnextDDS::cpp2_api
    ${CONNEXTDDS_EXTERNAL_LIBS}
    "${kyoto_cabinet_lib}"
)

target_include_directories(
    KyotoCabinetReader
    PUBLIC 
        "${CMAKE_CURRENT_BINARY_DIR}"
        "${CMAKE_CURRENT_BINARY_DIR}/generated"
)

add_dependencies(KyotoCabinetReader generated_code)

