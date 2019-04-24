/*
 * (c) 2019 Copyright, Real-Time Innovations, Inc.  All rights reserved.
 *
 * RTI grants Licensee a license to use, modify, compile, and create derivative
 * works of the Software.  Licensee has the right to distribute object form
 * only for use with RTI products.  The Software is provided "as is", with no
 * warranty of any type, including any warranty for fitness for any purpose.
 * RTI is under no obligation to maintain or support the Software.  RTI shall
 * not be liable for any incidental or consequential damages arising out of the
 * use or inability to use the software.
 */

#include "rti/recording/storage/StorageReader.hpp"

#include <fstream>

namespace kyoto_cabinet {

/*
 * Convenience macro to forward-declare the C-style function that will be
 * called by RTI Recording Service to create your class.
 */
RTI_RECORDING_STORAGE_READER_CREATE_DECL(KyotoCabinetReader);

/**
 * [TODO]
 */
class KyotoCabinetReader : public rti::recording::storage::StorageReader {
public:
    KyotoCabinetReader(const rti::routing::PropertySet& properties);
    ~KyotoCabinetReader();

    rti::recording::storage::StorageStreamInfoReader *
            create_stream_info_reader(
                    const rti::routing::PropertySet& properties);

    void delete_stream_info_reader(
            rti::recording::storage::StorageStreamInfoReader *stream_info_reader);

    rti::recording::storage::StorageStreamReader * create_stream_reader(
            const rti::routing::StreamInfo& stream_info,
            const rti::routing::PropertySet& properties);

    void delete_stream_reader(
            rti::recording::storage::StorageStreamReader *stream_reader);
private:
    std::ifstream info_file_;
    std::ifstream data_file_;
    std::string file_name_;
};

/*
 * [TODO]
 */
class KyotoCabinetStreamReader :
        public rti::recording::storage::DynamicDataStorageStreamReader {
public:

    KyotoCabinetStreamReader(std::ifstream *data_file);

    virtual ~KyotoCabinetStreamReader();

    /*
     * Implementation of the read operation. It should interpret the selector
     * state object that expresses the specific needs of Replay/Converter about
     * the data to be provided (data not read before vs data of any kind, lower
     * and upper time limits, etc).
     */
    virtual void read(
            std::vector<dds::core::xtypes::DynamicData *>& sample_seq,
            std::vector<dds::sub::SampleInfo *>& info_seq,
            const rti::recording::storage::SelectorState& selector);

    /*
     * The return loan operation should free any resources allocated by the
     * read() operation.
     */
    virtual void return_loan(
            std::vector<dds::core::xtypes::DynamicData *>& sample_seq,
            std::vector<dds::sub::SampleInfo *>& info_seq);

    /*
     * This method should flag Replay/Converter that all data related to the
     * data stream has been read and that we're ready for termination.
     */
    virtual bool finished();

    virtual void reset();

private:

    std::ifstream *data_file_;
    int64_t current_timestamp_;
    int current_valid_data_;
    DDS_Long current_data_id_;
    std::string current_data_msg_;
    dds::core::xtypes::StructType type_;
    /*
     * Read one single sample from the data file. This method deserializes the
     * textual format of the sample into a dynamic data object that is going to
     * be returned to Replay/Converter for processing.
     */
    bool read_sample();
};

/*
 * [TODO]
 */
class KyotoCabinetStreamInfoReader
        : public rti::recording::storage::StorageStreamInfoReader {
public:
    KyotoCabinetStreamInfoReader(std::ifstream *info_file);
    virtual ~KyotoCabinetStreamInfoReader();

    /*
     * Implementation of the read operation. It should interpret the selector
     * state object that expresses the specific needs of Replay/Converter about
     * the data to be provided (data not read before vs data of any kind, lower
     * and upper time limits, etc).
     */
    virtual void read(
            std::vector<rti::routing::StreamInfo *>& sample_seq,
            const rti::recording::storage::SelectorState& selector);

    /*
     * The return loan operation should free any resources allocated by the
     * read() operation.
     */
    virtual void return_loan(
            std::vector<rti::routing::StreamInfo *>& sample_seq);

    /*
     * An int64-represented time-stamp (in nanoseconds) representing the
     * starting point in time where recorded data exists, or when the service
     * started executing.
     */
    virtual int64_t service_start_time();

    /*
     * An int64-represented time-stamp (in nanoseconds) representing the
     * final point in time where recorded data exists, or when the service
     * finished executing.
     */
    virtual int64_t service_stop_time();

    virtual bool finished();

    virtual void reset();

private:

    std::ifstream *info_file_;
    bool stream_info_taken_;
    rti::routing::StreamInfo example_stream_info_;
    dds::core::xtypes::StructType example_type_;
};

} // namespace kyoto_cabinet
