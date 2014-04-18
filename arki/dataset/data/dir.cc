/*
 * data/dir - Directory based data collection
 *
 * Copyright (C) 2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include "dir.h"
#include "arki/metadata.h"
#include "arki/utils/fd.h"
#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/sys/buffer.h>
#include <wibble/sys/fs.h>
#include <cerrno>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <fcntl.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace dataset {
namespace data {
namespace dir {

namespace {

struct FdLock
{
    const std::string& pathname;
    int fd;
    struct flock lock;

    FdLock(const std::string& pathname, int fd) : pathname(pathname), fd(fd)
    {
        lock.l_type = F_WRLCK;
        lock.l_whence = SEEK_SET;
        lock.l_start = 0;
        lock.l_len = 0;
        // Use SETLKW, so that if it is already locked, we just wait
        if (fcntl(fd, F_SETLKW, &lock) == -1)
            throw wibble::exception::File(pathname, "locking the file for writing");
    }

    ~FdLock()
    {
        lock.l_type = F_UNLCK;
        fcntl(fd, F_SETLK, &lock);
    }
};

struct SequenceFile
{
    std::string pathname;
    int fd;

    SequenceFile(const std::string& pathname)
        : pathname(pathname), fd(-1)
    {
        fd = open(pathname.c_str(), O_RDWR | O_CREAT | O_CLOEXEC | O_NOATIME | O_NOFOLLOW, 0666);
        if (fd == -1)
            throw wibble::exception::File(pathname, "cannot open sequence file");
    }

    ~SequenceFile()
    {
        if (fd != -1)
            close(fd);
    }

    size_t next()
    {
        FdLock lock(pathname, fd);
        uint64_t cur;

        // Read the value in the sequence file
        ssize_t count = pread(fd, &cur, sizeof(cur), 0);
        if (count < 0)
            throw wibble::exception::File(pathname, "cannot read sequence file");
        if ((size_t)count < sizeof(cur))
            cur = 0;

        // Increment
        ++cur;

        // Write it out
        count = pwrite(fd, &cur, sizeof(cur), 0);
        if (count < 0)
            throw wibble::exception::File(pathname, "cannot write sequence file");

        return (size_t)cur;
    }
};

/// Write the buffer to a file. If the file already exists, does nothing and returns false
bool add_file(const std::string& absname, const wibble::sys::Buffer& buf)
{
    int fd = open(absname.c_str(), O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL, 0666);
    if (fd == -1)
    {
        if (errno == EEXIST) return false;
        throw wibble::exception::File(absname, "cannot create file");
    }

    utils::fd::HandleWatch hw(absname, fd);

    ssize_t count = pwrite(fd, buf.data(), buf.size(), 0);
    if (count < 0)
        throw wibble::exception::File(absname, "cannot write file");

    if (fdatasync(fd) < 0)
        throw wibble::exception::File(absname, "flushing write");

    return true;
}

struct Append : public Transaction
{
    bool fired;
    Metadata& md;
    string absname;
    size_t pos;
    size_t size;

    Append(Metadata& md, std::string& absname, size_t pos, size_t size)
        : fired(false), md(md), absname(absname), pos(pos), size(size)
    {
    }

    virtual ~Append()
    {
        if (!fired) rollback();
    }

    virtual void commit()
    {
        if (fired) return;

        // Set the source information that we are writing in the metadata
        md.source = types::source::Blob::create(md.source->format, "", absname, pos, size);

        fired = true;
    }

    virtual void rollback()
    {
        if (fired) return;

        // If we had a problem, remove the file that we have created. The
        // sequence will have skipped one number, but we do not need to
        // guarantee consecutive numbers, so it is just a cosmetic issue. This
        // case should be rare enough that even the cosmetic issue should
        // rarely be noticeable.
        string target_name = str::joinpath(absname, str::fmtf("%06zd.%s", pos, md.source->format.c_str()));
        unlink(target_name.c_str());

        fired = true;
    }
};

}

Writer::Writer(const std::string& relname, const std::string& absname, bool truncate)
    : data::Writer(relname, absname), seqfile(str::joinpath(absname, ".sequence"))
{
    // Ensure that the directory 'absname' exists
    wibble::sys::fs::mkpath(absname);
}

Writer::~Writer()
{
}

void Writer::append(Metadata& md)
{
    SequenceFile sf(seqfile);
    while (true)
    {
        size_t pos = sf.next();
        string fname = str::joinpath(absname, str::fmtf("%zd.%s", pos, md.source->format.c_str()));
        if (add_file(fname, md.getData()))
            break;
    }
}

off_t Writer::append(const wibble::sys::Buffer& buf)
{
    throw wibble::exception::Consistency("dir::Writer::append not implemented");
}

Pending Writer::append(Metadata& md, off_t* ofs)
{
    SequenceFile sf(seqfile);
    sys::Buffer buf = md.getData();
    string target_name;
    size_t pos = 0;
    size_t size = buf.size();
    while (true)
    {
        pos = sf.next();
        target_name = str::joinpath(absname, str::fmtf("%06zd.%s", pos, md.source->format.c_str()));
        if (add_file(target_name, buf))
            break;
    }

    *ofs = pos;

    return new Append(md, absname, pos, size);
}

FileState Writer::check(const std::string& absname, const metadata::Collection& mds, bool quick)
{
    throw wibble::exception::Consistency("dir::Writer::check not implemented");
}

OstreamWriter::OstreamWriter()
{
    throw wibble::exception::Consistency("dir::OstreamWriter not implemented");
}

OstreamWriter::~OstreamWriter()
{
}

size_t OstreamWriter::stream(Metadata& md, std::ostream& out) const
{
    throw wibble::exception::Consistency("dir::OstreamWriter::stream not implemented");
}

size_t OstreamWriter::stream(Metadata& md, int out) const
{
    throw wibble::exception::Consistency("dir::OstreamWriter::stream not implemented");
}

}
}
}
}
