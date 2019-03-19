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

set(KYOTOCABINET_LIB_NAME "kyotocabinet")

find_library(
    kyoto_cabinet_lib
    NAMES ${KYOTOCABINET_LIB_NAME}
    PATHS "${KYOTOCABINET_DIR}"
)

# Define the library that will provide the storage writer plugin
add_library(
    KyotoCabinetWriter MODULE
    "${CMAKE_CURRENT_SOURCE_DIR}/KyotoCabinetWriter.cxx"
)

target_link_libraries(
    KyotoCabinetWriter
    RTIConnextDDS::routing_service_infrastructure
    RTIConnextDDS::cpp2_api
    ${CONNEXTDDS_EXTERNAL_LIBS}
    "${kyoto_cabinet_lib}
)

target_include_directories(
    KyotoCabinetWriter
    PUBLIC "${CMAKE_BINARY_DIR}"
)
