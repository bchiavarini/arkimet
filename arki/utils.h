#ifndef ARKI_UTILS_H
#define ARKI_UTILS_H

/*
 * utils - General utility functions
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <sstream>
#include <string>
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>
#include <stdint.h>

namespace arki {
namespace utils {

// static_assert implementation (taken from boost)
// It relies on sizeof(incomplete_type) generating an error message containing
// the name of the incomplete type.
template <bool x> struct STATIC_ASSERTION_FAILURE;
template <> struct STATIC_ASSERTION_FAILURE<true> { enum { value = 1 }; };
template<int x> struct static_assert_test{};
#define ARKI_STATIC_ASSERT( B ) \
	typedef ::arki::utils::static_assert_test<\
		sizeof(::arki::utils::STATIC_ASSERTION_FAILURE<(bool)(B)>)> arki_utils_static_assert_typedef_ ## __LINE__

/// Create an empty file, succeeding if it already exists
void createFlagfile(const std::string& pathname);

/// Create an empty file, failing if it already exists
void createNewFlagfile(const std::string& pathname);

template<typename A, typename B>
int compareMaps(const A& m1, const B& m2)
{
        typename A::const_iterator a = m1.begin();
        typename B::const_iterator b = m2.begin();
        for ( ; a != m1.end() && b != m2.end(); ++a, ++b)
        {
                if (a->first < b->first)
                        return -1;
                if (a->first > b->first)
                        return 1;
                if (int res = a->second->compare(*b->second)) return res;
        }
        if (a == m1.end() && b == m2.end())
                return 0;
        if (a == m1.end())
                return -1;
        return 1;
}

// Save the state of a stream, the RAII way
class SaveIOState
{
		std::ios_base& s;
		std::ios_base::fmtflags f;
public:
		SaveIOState(std::ios_base& s) : s(s), f(s.flags())
		{}
		~SaveIOState()
		{
				s.flags(f);
		}
};

// Dump the string, in hex, to stderr, prefixed with name
void hexdump(const char* name, const std::string& str);

// Dump the string, in hex, to stderr, prefixed with name
void hexdump(const char* name, const unsigned char* str, int len);

/**
 * RAII-style class changing into a newly created temporary directory during
 * the lifetime of the object.
 *
 * The temporary directory is created at constructor time and deleted at
 * destructor time.
 */
struct MoveToTempDir
{
    std::string old_dir;
    std::string tmp_dir;

    MoveToTempDir(const std::string& pattern = "/tmp/tmpdir.XXXXXX");
    ~MoveToTempDir();
};

/**
 * Get format from file extension
 *
 * @returns the empty string if no file extension was found
 */
std::string get_format(const std::string& fname);

/**
 * Get format from file extension, throwing an exception if no extension was
 * found
 */
std::string require_format(const std::string& fname);

}
}

// vim:set ts=4 sw=4:
#endif
