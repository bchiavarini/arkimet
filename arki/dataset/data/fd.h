#ifndef ARKI_DATASET_DATA_FD_H
#define ARKI_DATASET_DATA_FD_H

/*
 * data - Base class for unix fd based read/write functions
 *
 * Copyright (C) 2012--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/libconfig.h>
#include <arki/dataset/data.h>
#include <string>

namespace wibble {
namespace sys {
class Buffer;
}
}

namespace arki {
namespace dataset {
namespace data {
namespace fd {

class Writer : public data::Writer
{
protected:
    int fd;

public:
    Writer(const std::string& relname, const std::string& absname, bool truncate=false);
    ~Writer();

    void lock();
    void unlock();
    off_t wrpos();
    void write(const wibble::sys::Buffer& buf);
    void truncate(off_t pos);
};

struct Maint : public data::Maint
{
    size_t remove(const std::string& absname);
    void truncate(const std::string& absname, size_t offset);
    FileState check(const std::string& absname, const metadata::Collection& mds, unsigned max_gap=0, bool quick=true);

    static Pending repack(
            const std::string& rootdir,
            const std::string& relname,
            metadata::Collection& mds,
            data::Writer* make_repack_writer(const std::string&, const std::string&));
};

}
}
}
}

#endif
