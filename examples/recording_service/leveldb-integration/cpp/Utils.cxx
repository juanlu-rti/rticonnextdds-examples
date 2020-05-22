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

#include <vector>

#include <leveldb/comparator.h>
#include <leveldb/slice.h>

#include "Utils.hpp"

namespace rti { namespace recording { namespace examples {

UserDataKeyComparator::UserDataKeyComparator() = default;

UserDataKeyComparator::~UserDataKeyComparator() = default;


int UserDataKeyComparator::Compare(
        const leveldb::Slice &a,
        const leveldb::Slice &b) const
{
    return memcmp(a.data(), b.data(), a.size());
}

const char *UserDataKeyComparator::Name() const
{
    return "rti.recording.examples.UserDataKeyComparator.1.0";
}

void UserDataKeyComparator::FindShortestSeparator(
        std::string *start,
        const leveldb::Slice &limit) const
{
}

void UserDataKeyComparator::FindShortSuccessor(std::string *key) const
{
}

}}}  // namespace rti::recording::examples
