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

#include "rti/recording/storage/StorageWriter.hpp"
#include "rti/recording/storage/StorageStreamWriter.hpp"
#include "rti/recording/storage/StorageDiscoveryStreamWriter.hpp"

#include <kchashdb.h>

#include <fstream>

namespace kyoto_cabinet {

/*
 * Convenience macro to forward-declare the C-style function that will be
 * called by RTI Recording Service to create your class.
 */
RTI_RECORDING_STORAGE_WRITER_CREATE_DECL(KyotoCabinetWriter);

/*
 * [TODO]
 */
class KyotoCabinetWriter : public rti::recording::storage::StorageWriter {
public:
    KyotoCabinetWriter(const rti::routing::PropertySet& properties);
    virtual ~KyotoCabinetWriter();

    /*
     * Recording Service will call this method to create a Stream Writer object
     * associated with a user-data topic that has been discovered.
     * The property set passed as a parameter contains information about the
     * stream not provided by the stream info object. For example, Recording
     * Service will add the DDS domain ID as a property to this set.
     */
    rti::recording::storage::StorageStreamWriter * create_stream_writer(
            const rti::routing::StreamInfo& stream_info,
            const rti::routing::PropertySet& properties);

    /*
     * Recording Service will call this method to obtain a stream writer for the
     * DDS publication built-in discovery topic.
     * Note that we're not defining a participant or subscription creation
     * method. That is telling Recording Service that we're not going to store
     * samples for those two built-in topics.
     */
    rti::recording::storage::PublicationStorageWriter * create_publication_writer();

    /*
     * Recording Service will call this method to delete a previously created
     * Stream Writer (no matter if it was created with the create_stream_writer()
     * or create_discovery_stream_writer() method).
     */
    void delete_stream_writer(
            rti::recording::storage::StorageStreamWriter *writer);

private:

    std::string data_filename_prefix_;

    std::string pub_file_name_;
};

/**
 * [TODO]
 */
class KyotoCabinetStreamWriter :
        public rti::recording::storage::DynamicDataStorageStreamWriter {
public:
    KyotoCabinetStreamWriter(
            const std::string& data_filename_prefix,
            const std::string& stream_name);

    virtual ~KyotoCabinetStreamWriter();

    /*
     * [TODO]
     */
    void store(
            const std::vector<dds::core::xtypes::DynamicData *>& sample_seq,
            const std::vector<dds::sub::SampleInfo *>& info_seq);

private:

    kyotocabinet::HashDB data_file_;

    std::string stream_name_;

    std::vector<char> cdr_buffer_;
};

/*
 * [TODO]
 */
class PubDiscoveryKyotoCabinetWriter :
        public rti::recording::storage::PublicationStorageWriter {
public:
    PubDiscoveryKyotoCabinetWriter(const std::string& pub_filename);
    ~PubDiscoveryKyotoCabinetWriter();

    void store(
            const std::vector<dds::topic::PublicationBuiltinTopicData *>& sample_seq,
            const std::vector<dds::sub::SampleInfo *>& info_seq);

private:

    std::string pub_filename_;
};

} // namespace cpp_example
