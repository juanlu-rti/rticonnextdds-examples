/*
 * (c) 2020 Copyright, Real-Time Innovations, Inc.  All rights reserved.
 *
 * RTI grants Licensee a license to use, modify, compile, and create derivative
 * works of the Software.  Licensee has the right to distribute object form
 * only for use with RTI products.  The Software is provided "as is", with no
 * warranty of any type, including any warranty for fitness for any purpose.
 * RTI is under no obligation to maintain or support the Software.  RTI shall
 * not be liable for any incidental or consequential damages arising out of the
 * use or inability to use the software.
 */

#ifndef RTI_RECORDING_EXAMPLES_UTILS_H_
#define RTI_RECORDING_EXAMPLES_UTILS_H_


#include <dds/dds.hpp>

#include <rti/core/constants.hpp>

#include <leveldb/comparator.h>

#include "LevelDb_RecorderTypes.hpp"


namespace rti { namespace recording { namespace examples {

/**
 * @brief Convert a Time object into an int-64 based timestamp representing
 *        nanoseconds.
 */
inline int64_t to_nanosec_timestamp(const dds::core::Time &time)
{
    int64_t timestamp = time.sec();
    timestamp *= (int64_t) rti::core::nanosec_per_sec;
    timestamp += (int64_t) time.nanosec();
    return timestamp;
}

/**
 * @brief Convert an int-64-based timestamp, representing nanoseconds, into a
 *        Time object.
 */
inline dds::core::Time timestamp_to_time(int64_t timestamp)
{
    dds::core::Time time_out;
    time_out.sec(timestamp / (int64_t) rti::core::nanosec_per_sec);
    time_out.nanosec(timestamp % (int64_t) rti::core::nanosec_per_sec);
    return time_out;
}

/**
 * @brief Convert a Duration object into an int-64-based timestamp representing
 *        nanoseconds.
 */
inline int64_t to_nanosec_timestamp(const dds::core::Duration &time_duration)
{
    int64_t timestamp = time_duration.sec();
    timestamp *= (int64_t) rti::core::nanosec_per_sec;
    timestamp += (int64_t) time_duration.nanosec();
    return timestamp;
}

/**
 * @brief Convert a LevelDB slice object into a user-defined type. The type
 *        should have been generated by RTI DDS Code Generator in modern C++
 *        language.
 *        In the case of this example, the two types that we will use here are
 *        'UserDataKey' and 'UserDataValue'. See the IDL file,
 *        LevelDb_RecorderTypes.idl, for more reference.
 */
template <typename T>
inline T slice_to_user_type(leveldb::Slice slice, std::vector<char> &buffer)
{
    T value_out;
    const char *raw_data = slice.data();
    buffer.resize(slice.size());
    std::copy(raw_data, raw_data + slice.size(), buffer.begin());
    dds::topic::topic_type_support<T>::from_cdr_buffer(value_out, buffer);
    return value_out;
}

inline const std::string working_dir_property_name()
{
    return "rti.recording.examples.leveldb.working_dir";
}

inline const std::string enable_auto_dir_property_name()
{
    return "rti.recording.examples.leveldb.enable_auto_dir";
}

/**
 * @brief The key used for storing the time the Recorder service was started.
 *        This value is stored in the metadata.dat database file.
 */
inline const std::string start_time_key_name()
{
    return "rti.recording.examples.leveldb.start_time";
}

/**
 * @brief The key used for storing the time the Recorder service was stopped.
 *        This value is stored in the metadata.dat database file.
 */
inline const std::string end_time_key_name()
{
    return "rti.recording.examples.leveldb.end_time";
}

/**
 * The default Comparator provided by LevelDB uses lexicographic ordering. This
 * doesn't apply to our keys, which are byte streams. Furthermore, for ordering
 * purposes, we just care about the reception timestamp.
 * This Comparator implementation compares two UserDataKey objects based on
 * their reception timestamp and the resulting ordering in LevelDB will follow
 * this pattern.
 */
class UserDataKeyComparator : public leveldb::Comparator {
public:
    UserDataKeyComparator();

    ~UserDataKeyComparator();

    /*
     * Three-way comparison function:
     *     - if a < b: negative result
     *     - if a > b: positive result
     *     - else: zero result
     */
    virtual int Compare(const leveldb::Slice &a, const leveldb::Slice &b) const;

    virtual const char *Name() const;

    virtual void FindShortestSeparator(
            std::string *start,
            const leveldb::Slice &limit) const;

    virtual void FindShortSuccessor(std::string *key) const;
};

/**
 * Utility class to delete a type-object instance comfortably from within a
 * smart pointer (can be easily used as a smart pointer deleter).
 */
struct TypeObjectDeleter {
    TypeObjectDeleter(DDS_TypeObjectFactory *type_factory)
            : type_factory_(type_factory)
    {
    }

    void operator()(DDS_TypeObject *type)
    {
        DDS_TypeObjectFactory_delete_typeobject(type_factory_, type);
    }

    DDS_TypeObjectFactory *type_factory_;
};


}}}  // namespace rti::recording::examples

#endif
