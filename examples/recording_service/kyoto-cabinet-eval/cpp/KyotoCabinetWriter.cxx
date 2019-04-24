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

#include <memory>

#include <dds_c/dds_c_typeobject.h>

#include "KyotoCabinetWriter.hpp"
#include "ReducedDCPSPublication.hpp"

#define NANOSECS_PER_SEC 1000000000ll

#define DATA_FILENAME_PREFIX_PROPERTY "example.kyoto_cabinet.data_filename_prefix"
#define PUB_FILENAME_PROPERTY "example.kyoto_cabinet.pub_filename"

namespace kyoto_cabinet {

/*
 * Convenience macro to define the C-style function that will be called by RTI
 * Recording Service to create your class.
 */
RTI_RECORDING_STORAGE_WRITER_CREATE_DEF(KyotoCabinetWriter);

/*
 * In the xml configuration, under the property tag for the storage plugin, a
 * collection of name/value pairs can be passed. In this case, this example
 * chooses to define a property to name the filename to use.
 */
KyotoCabinetWriter::KyotoCabinetWriter(
        const rti::routing::PropertySet& properties) :
    StorageWriter(properties)
{
    rti::routing::PropertySet::const_iterator found =
            properties.find(DATA_FILENAME_PREFIX_PROPERTY);
    if (found == properties.end()) {
        throw std::runtime_error(
                "Failed to get data file name prefix in properties");
    }
    data_filename_prefix_ = found->second;
    found = properties.find(PUB_FILENAME_PROPERTY);
    if (found == properties.end()) {
        throw std::runtime_error(
                "Failed to get pub file name in properties");
    }
    pub_file_name_ = found->second;

    /* Obtain current time */
//    int64_t current_time = (int64_t) time(NULL);
//    if (current_time == -1) {
//        throw std::runtime_error("Failed to obtain the current time");
//    }
//    /* Time was returned in seconds. Transform to nanoseconds */
//    current_time *= NANOSECS_PER_SEC;
//    info_file_ << "Start timestamp: " << current_time << std::endl;
//    if (info_file_.fail()) {
//        throw std::runtime_error("Failed to write start timestamp");
//    }
}

KyotoCabinetWriter::~KyotoCabinetWriter()
{
//    if (info_file_.good()) {
//        /* Obtain current time */
//        int64_t current_time = (int64_t) time(NULL);
//        if (current_time == -1) {
//            // can't throw in a destructor
//            std::cerr << "Failed to obtain the current time";
//        }
//        /* Time was returned in seconds. Transform to nanoseconds */
//        current_time *= NANOSECS_PER_SEC;
//        info_file_ << "End timestamp: " << current_time << std::endl;
//    }
}

rti::recording::storage::StorageStreamWriter *
KyotoCabinetWriter::create_stream_writer(
        const rti::routing::StreamInfo& stream_info,
        const rti::routing::PropertySet&)
{
    return new KyotoCabinetStreamWriter(
            data_filename_prefix_,
            stream_info.stream_name());
}

rti::recording::storage::PublicationStorageWriter *
KyotoCabinetWriter::create_publication_writer()
{
    return new PubDiscoveryKyotoCabinetWriter(pub_file_name_);
}

void KyotoCabinetWriter::delete_stream_writer(
        rti::recording::storage::StorageStreamWriter *writer)
{
    delete writer;
}

KyotoCabinetStreamWriter::KyotoCabinetStreamWriter(
        const std::string& data_filename_prefix,
        const std::string& stream_name) :
    stream_name_(stream_name),
    cdr_buffer_(2048)
{
    std::string db_filename;
    db_filename += data_filename_prefix;
    db_filename += "-";
    db_filename += stream_name;
    db_filename += ".dat";
    if (!data_file_.open(db_filename)) {
        std::stringstream log_msg;
        log_msg << "Failed to open Kyoto Cabinet HashDB file: " << db_filename;
        throw std::runtime_error(log_msg.str());
    }
}

KyotoCabinetStreamWriter::~KyotoCabinetStreamWriter()
{
    data_file_.close();
}

/*
 * This function is called by Recorder whenever there are samples available for
 * one of the streams previously discovered and accepted (see the
 * FileStorageWriter_create_stream_writer() function below). Recorder provides
 * the samples and their associated information objects in Routing Service
 * format, this is, untyped format.
 * In our case we know that, except for the built-in DDS discovery topics which
 * are received in their own format - and that we're not really storing -, that
 * the format of the data we're receiving is DDS Dynamic Data. This will always
 * be the format received for types recorded from DDS.
 * The function traverses the collection of samples and stores the data.
 */
void KyotoCabinetStreamWriter::store(
        const std::vector<dds::core::xtypes::DynamicData *>& sample_seq,
        const std::vector<dds::sub::SampleInfo *>& info_seq)
{
    using namespace dds::core::xtypes;
    using namespace rti::core::xtypes;
    using namespace dds::sub;

    std::vector<char> buffer(2048);

    data_file_.begin_transaction();

    const int32_t count = sample_seq.size();
    for (int32_t i = 0; i < count; ++i) {
        const SampleInfo& sample_info = *(info_seq[i]);
        // we first first print the sample's metadata
        int64_t timestamp =
                (int64_t) sample_info->reception_timestamp().sec()
                * NANOSECS_PER_SEC;
        timestamp += sample_info->reception_timestamp().nanosec();
        const dds::core::xtypes::DynamicData& sample = *(sample_seq[i]);
        rti::core::xtypes::to_cdr_buffer(buffer, sample);
        if (!data_file_.add(
                reinterpret_cast<char *>(&timestamp),
                sizeof(int64_t),
                buffer.data(),
                buffer.size())) {
            std::stringstream log_msg;
            log_msg << "Failed to add value to file:" << std::endl
                    << "    file path : " << data_file_.path()  << std::endl
                    << "    value     : " << timestamp << std::endl ;
            std::cerr << log_msg.str();
        }
    }
    data_file_.end_transaction();
}

PubDiscoveryKyotoCabinetWriter::PubDiscoveryKyotoCabinetWriter(
        const std::string& pub_filename) :
    pub_filename_(pub_filename)
{
    if (!pub_file_.open(pub_filename_)) {
        std::stringstream log_msg;
        log_msg << "Failed to open Kyoto Cabinet HashDB file: "
                << pub_filename_;
        throw std::runtime_error(log_msg.str());
    }
}

PubDiscoveryKyotoCabinetWriter::~PubDiscoveryKyotoCabinetWriter()
{
    pub_file_.close();
}

void PubDiscoveryKyotoCabinetWriter::store(
        const std::vector<dds::topic::PublicationBuiltinTopicData *>& sample_seq,
        const std::vector<dds::sub::SampleInfo *>& info_seq)
{
    using namespace dds::sub;

    ReducedDCPSPublication reduced_sample;
    std::vector<char> buffer(2048);

    pub_file_.begin_transaction();

    const int32_t count = sample_seq.size();
    for (int32_t i = 0; i < count; ++i) {
        const SampleInfo& sample_info = *(info_seq[i]);
        // we first first print the sample's metadata
        int64_t timestamp =
                (int64_t) sample_info->reception_timestamp().sec()
                * NANOSECS_PER_SEC;
        timestamp += sample_info->reception_timestamp().nanosec();
        dds::topic::PublicationBuiltinTopicData& sample = *(sample_seq[i]);
        reduced_sample.valid_data(sample_info.valid());
        if (sample_info.valid()) {
            reduced_sample.topic_name(sample.topic_name());
            reduced_sample.type_name(sample.type_name());
            const dds::core::optional<dds::core::xtypes::DynamicType> type =
                    sample->type();
            if (type.is_set()) {
                const dds::core::xtypes::DynamicType& dynamic_type = type.get();
                const DDS_TypeCode& native_type = dynamic_type.native();
                DDS_TypeObject *type_object =
                        DDS_TypeObject_create_from_typecode(&native_type);
                if (type_object == NULL) {
                    std::stringstream log_msg;
                    log_msg << "Failed to create type-object from type-code"
                            << std::endl;
                    log_msg << "    Topic name : " << sample.topic_name()
                            << std::endl;
                    log_msg << "    Type name  : " << sample.type_name()
                            << std::endl;
                    throw std::runtime_error(log_msg.str());
                }
                std::shared_ptr<DDS_TypeObject> type_object_ptr(
                        type_object,
                        DDS_TypeObject_delete);
                uint32_t type_object_buffer_len =
                        DDS_TypeObject_get_serialized_size(type_object);
                std::vector<uint8_t> typeobject_buffer(type_object_buffer_len);
                if (DDS_TypeObject_serialize(
                        type_object,
                        (char *) &(typeobject_buffer[0]),
                        &type_object_buffer_len) != DDS_RETCODE_OK) {
                    std::stringstream log_msg;
                    log_msg << "Failed to serialize type-object"
                            << std::endl;
                    log_msg << "    Topic name : " << sample.topic_name()
                            << std::endl;
                    log_msg << "    Type name  : " << sample.type_name()
                            << std::endl;
                    throw std::runtime_error(log_msg.str());
                }
                reduced_sample.type(typeobject_buffer);
            }
        }
        dds::topic::topic_type_support<ReducedDCPSPublication>::to_cdr_buffer(
                buffer,
                reduced_sample);
        if (!pub_file_.add(
                reinterpret_cast<char *>(&timestamp),
                sizeof(int64_t),
                buffer.data(),
                buffer.size())) {
            std::stringstream log_msg;
            log_msg << "Failed to add value to file:" << std::endl
                    << "    file path : " << pub_file_.path()  << std::endl
                    << "    value     : " << timestamp << std::endl ;
            std::cerr << log_msg.str();
        }
    }
    pub_file_.end_transaction();
}

} // namespace kyoto_cabinet

