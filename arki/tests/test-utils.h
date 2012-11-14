/**
 * Copyright (C) 2007--2012  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#ifndef ARKI_TEST_UTILS_H
#define ARKI_TEST_UTILS_H

#include <wibble/tests.h>

namespace arki {
namespace tests {

#define ALWAYS_THROWS __attribute__ ((noreturn))

class Location
{
    const Location* parent;
    const std::string& file;
    int line;

    Location(const Location& parent, const std::string& file, int line);

    void fail_test(const std::string& msg) ALWAYS_THROWS;
    void backtrace(std::ostream& out) const;
};

#define LOC wibble::tests::Location(__FILE__, __LINE__, "")
#define ILOC wibble::tests::Location(loc, __FILE__, __LINE__, "")
#define LOCPRM const wibble::tests::Location& loc

#define ensure_contains(x, y) arki::tests::impl_ensure_contains(wibble::tests::Location(__FILE__, __LINE__, #x " == " #y), (x), (y))
#define inner_ensure_contains(x, y) arki::tests::impl_ensure_contains(wibble::tests::Location(loc, __FILE__, __LINE__, #x " == " #y), (x), (y))
void impl_ensure_contains(const wibble::tests::Location& loc, const std::string& haystack, const std::string& needle);

#define ensure_not_contains(x, y) arki::tests::impl_ensure_not_contains(wibble::tests::Location(__FILE__, __LINE__, #x " == " #y), (x), (y))
#define inner_ensure_not_contains(x, y) arki::tests::impl_ensure_not_contains(wibble::tests::Location(loc, __FILE__, __LINE__, #x " == " #y), (x), (y))
void impl_ensure_not_contains(const wibble::tests::Location& loc, const std::string& haystack, const std::string& needle);

#define ensure_md_equals(md, type, strval) \
    ensure_equals((md).get<type>(), type::decodeString(strval))

void ensure_file_exists(LOCPRM, const std::string& fname);
void ensure_not_file_exists(LOCPRM, const std::string& fname);

}
}

#endif
