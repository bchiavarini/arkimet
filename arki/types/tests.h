/**
 * Copyright (C) 2007--2013  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */
#ifndef ARKI_TYPES_TESTUTILS_H
#define ARKI_TYPES_TESTUTILS_H

#include <arki/tests/tests.h>
#include <arki/types/time.h>
#include <vector>
#include <string>
#include <cstdint>

namespace arki {

namespace types {
class Type;
}

namespace tests {

/// Prepackaged set of tests for generic arki types
struct TestGenericType
{
    // Item code
    std::string tag;
    // Stringified sample to test
    std::string sample;
    // Alternate items that should all test equals to sample
    std::vector<std::string> alternates;
    // Optional SQL exactquery to test
    std::string exact_query;
    // List of items that test higher than sample
    std::vector<std::string> lower;
    // List of items that test lower than sample
    std::vector<std::string> higher;

    TestGenericType(const std::string& tag, const std::string& sample);

    void check() const;

    /// Decode item from encoded, running single-item checks and passing ownership back the caller
    void check_item(const std::string& encoded, std::unique_ptr<types::Type>& item) const;
};


class ActualType : public arki::utils::tests::Actual<const arki::types::Type*>
{
public:
    ActualType(const types::Type* actual) : arki::utils::tests::Actual<const types::Type*>(actual) {}

    void operator==(const types::Type* expected) const;
    void operator!=(const types::Type* expected) const;

    void operator==(const types::Type& expected) const { operator==(&expected); }
    void operator!=(const types::Type& expected) const { operator!=(&expected); }
    template<typename E> void operator==(const std::unique_ptr<E>& expected) const { operator==(expected.get()); }
    template<typename E> void operator!=(const std::unique_ptr<E>& expected) const { operator!=(expected.get()); }
    void operator==(const std::string& expected) const;
    void operator!=(const std::string& expected) const;
    /*
    template<typename E> TestIsLt<A, E> operator<(const E& expected) const { return TestIsLt<A, E>(actual, expected); }
    template<typename E> TestIsLte<A, E> operator<=(const E& expected) const { return TestIsLte<A, E>(actual, expected); }
    template<typename E> TestIsGt<A, E> operator>(const E& expected) const { return TestIsGt<A, E>(actual, expected); }
    template<typename E> TestIsGte<A, E> operator>=(const E& expected) const { return TestIsGte<A, E>(actual, expected); }
    */

    /**
     * Check that a metadata field can be serialized and deserialized in all
     * sorts of ways
     */
    void serializes() const;

    /**
     * Check comparison operators
     */
    void compares(const types::Type& higher) const;

    /// Check all components of a source::Blob item
    void is_source_blob(
        const std::string& format, const std::string& basedir, const std::string& fname,
        uint64_t ofs, uint64_t size);

    /// Check all components of a source::URL item
    void is_source_url(const std::string& format, const std::string& url);

    /// Check all components of a source::Inline item
    void is_source_inline(const std::string& format, uint64_t size);

    /// Check all components of a Time item
    void is_time(int ye, int mo, int da, int ho, int mi, int se);

    /// Check all components of a reftime::Position item
    void is_reftime_position(const int (&time)[6]);

    /// Check all components of a reftime::Position item
    void is_reftime_position(const types::Time&);

    /// Check all components of a reftime::Position item
    void is_reftime_period(const int (&begin)[6], const int (&end)[6]);

    /// Check all components of a reftime::Position item
    void is_reftime_period(const types::Time&, const types::Time&);
};

inline arki::tests::ActualType actual_type(const arki::types::Type& actual) { return arki::tests::ActualType(&actual); }
inline arki::tests::ActualType actual_type(const arki::types::Type* actual) { return arki::tests::ActualType(actual); }
inline arki::tests::ActualType actual(const arki::types::Type& actual) { return arki::tests::ActualType(&actual); }
template<typename T>
inline arki::tests::ActualType actual(const std::unique_ptr<T>& actual) { return arki::tests::ActualType(actual.get()); }

}
}
#endif
