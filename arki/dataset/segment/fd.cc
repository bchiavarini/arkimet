#include "fd.h"
#include "arki/exceptions.h"
#include "arki/metadata.h"
#include "arki/metadata/collection.h"
#include "arki/types/source/blob.h"
#include "arki/scan/any.h"
#include "arki/utils/compress.h"
#include "arki/utils/files.h"
#include "arki/utils/string.h"
#include "arki/utils/sys.h"
#include "arki/utils/lock.h"
#include "arki/utils.h"
#include "arki/dataset/reporter.h"
#include "arki/nag.h"
#include <algorithm>
#include <system_error>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>

using namespace std;
using namespace arki::types;
using namespace arki::utils;

namespace arki {
namespace dataset {
namespace segment {
namespace fd {

namespace {

struct Append : public Transaction
{
    Segment& w;
    Metadata& md;
    bool fired = false;
    const std::vector<uint8_t>& buf;
    off_t pos;

    Append(Segment& w, Metadata& md)
        : w(w), md(md), buf(md.getData()), pos(w.append_lock())
    {
    }

    virtual ~Append()
    {
        if (!fired) rollback();
    }

    void commit() override
    {
        if (fired) return;

        // Append the data
        w.write(pos, buf);
        w.append_unlock(pos);

        // Set the source information that we are writing in the metadata
        md.set_source(Source::createBlob(md.source().format, "", w.absname, pos, buf.size()));

        fired = true;
    }

    void rollback() override
    {
        if (fired) return;

        // If we had a problem, attempt to truncate the file to the original size
        w.fdtruncate(pos);
        w.append_unlock(pos);
        fired = true;
    }
};

}


Segment::Segment(const std::string& relname, const std::string& absname)
    : dataset::Segment(relname, absname), fd(absname)
{
}

Segment::~Segment()
{
    close();
}

bool Segment::can_store(const std::string& format)
{
    return format == "grib" || format == "grib1" || format == "grib2"
        || format == "bufr" || format == "vm2";
}

Pending Segment::append(Metadata& md, off_t* ofs)
{
    open();

    Append* res = new Append(*this, md);
    *ofs = res->pos;
    return res;
}

off_t Segment::append(Metadata& md)
{
    open();

    // Get the data blob to append
    const std::vector<uint8_t>& buf = md.getData();

    // Lock and get the write position in the data file
    off_t pos = append_lock();

    try {
        // Append the data
        write(pos, buf);
    } catch (...) {
        // If we had a problem, attempt to truncate the file to the original size
        fdtruncate(pos);
        append_unlock(pos);
        throw;
    }

    append_unlock(pos);

    // Set the source information that we are writing in the metadata
    // md.set_source(Source::createBlob(md.source().format, "", absname, pos, buf.size()));
    return pos;
}

off_t Segment::append(const std::vector<uint8_t>& buf)
{
    open();

    off_t pos = append_lock();
    try {
        write(pos, buf);
    } catch (...) {
        fdtruncate(pos);
        append_unlock(pos);
        throw;
    }
    append_unlock(pos);
    return pos;
}

void Segment::open()
{
    if (fd != -1) return;

    // Open the data file
    fd.open(O_WRONLY | O_CREAT, 0666);
}

void Segment::truncate_and_open()
{
    if (fd != -1) return;

    // Open the data file
    fd.open(O_WRONLY | O_CREAT | O_TRUNC, 0666);
}

void Segment::close()
{
    fd.close();
}

off_t Segment::append_lock()
{
    Lock lock;
    lock.l_type = F_WRLCK;
    lock.l_whence = SEEK_END;
    lock.l_start = 0;
    lock.l_len = 0;
    lock.ofd_setlkw(fd);

    // Now that we are locked, get the write position in the data file
    return fd.lseek(0, SEEK_END);
}

void Segment::append_unlock(off_t wrpos)
{
    Lock lock;
    lock.l_type = F_UNLCK;
    lock.l_whence = SEEK_SET;
    lock.l_start = wrpos;
    lock.l_len = 0;
    lock.ofd_setlk(fd);
}

void Segment::fdtruncate(off_t pos)
{
    open();

    if (ftruncate(fd, pos) == -1)
        nag::warning("truncating %s to previous size %zd (rollback of append operation): %m", absname.c_str(), pos);
}

void Segment::write(off_t wrpos, const std::vector<uint8_t>& buf)
{
    // Prevent caching (ignore function result)
    //(void)posix_fadvise(df.fd, pos, buf.size(), POSIX_FADV_DONTNEED);

    // Append the data
    fd.lseek(wrpos);
    fd.write_all_or_throw(buf.data(), buf.size());

    if (fdatasync(fd) < 0)
    {
        stringstream ss;
        ss << "cannot flush write to " << absname;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

State Segment::check_fd(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, unsigned max_gap, bool quick)
{
    close();

    // Check the data if requested
    if (!quick)
    {
        const scan::Validator* validator = &scan::Validator::by_filename(absname.c_str());

        for (const auto& i: mds)
        {
            try {
                validate(*i, *validator);
            } catch (std::exception& e) {
                stringstream out;
                out << "validation failed at " << i->source() << ": " << e.what();
                reporter.segment_info(ds, relname, out.str());
                return SEGMENT_UNALIGNED;
            }
        }
    }

    // Create the list of data (offset, size) sorted by offset
    vector< pair<off_t, size_t> > spans;
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        spans.push_back(make_pair(source.offset, source.size));
    }
    std::sort(spans.begin(), spans.end());

    // Check for overlaps
    off_t end_of_last_data_checked = 0;
    for (vector< pair<off_t, size_t> >::const_iterator i = spans.begin(); i != spans.end(); ++i)
    {
        // If an item begins after the end of another, they overlap and the file needs rescanning
        if (i->first < end_of_last_data_checked)
            return SEGMENT_UNALIGNED;

        end_of_last_data_checked = i->first + i->second;
    }

    // Check for truncation
    off_t file_size = utils::compress::filesize(absname);
    if (file_size < end_of_last_data_checked)
    {
        stringstream ss;
        ss << "file looks truncated: its size is " << file_size << " but data is known to exist until " << end_of_last_data_checked << " bytes";
        reporter.segment_info(ds, relname, ss.str());
        return SEGMENT_UNALIGNED;
    }

    // Check if file_size matches the expected file size
    bool has_hole = false;
    if (file_size > end_of_last_data_checked + max_gap)
        has_hole = true;

    // Check for holes or elements out of order
    end_of_last_data_checked = 0;
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        const source::Blob& source = (*i)->sourceBlob();
        if (source.offset < (size_t)end_of_last_data_checked || source.offset > (size_t)end_of_last_data_checked + max_gap)
            has_hole = true;

        end_of_last_data_checked = max(end_of_last_data_checked, (off_t)(source.offset + source.size));
    }

    // Take note of files with holes
    if (has_hole)
    {
        nag::verbose("%s: contains deleted data or data to be reordered", absname.c_str());
        return SEGMENT_DIRTY;
    } else {
        return SEGMENT_OK;
    }
}

void Segment::validate(Metadata& md, const scan::Validator& v)
{
    if (const types::source::Blob* blob = md.has_source_blob()) {
        if (blob->filename != relname)
            throw std::runtime_error("metadata to validate does not appear to be from this segment");

        if (sys::exists(absname))
        {
            sys::File fd(absname, O_RDONLY);
            v.validate_file(fd, blob->offset, blob->size);
            return;
        }
        // If the file does not exist, it may be a compressed segment.
        // TODO: Until handling of compressed segments is incorporated here, we
        // just let Metadata::getData see if it can load the data somehow.
    }
    const auto& buf = md.getData();
    v.validate_buf(buf.data(), buf.size());
}

size_t Segment::remove()
{
    close();

    size_t size = sys::size(absname);
    sys::unlink(absname.c_str());
    return size;
}

void Segment::truncate(size_t offset)
{
    close();

    if (!sys::exists(absname))
        utils::createFlagfile(absname);

    utils::files::PreserveFileTimes pft(absname);
    if (::truncate(absname.c_str(), offset) < 0)
    {
        stringstream ss;
        ss << "cannot truncate " << absname << " at " << offset;
        throw std::system_error(errno, std::system_category(), ss.str());
    }
}

void Segment::test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx)
{
    close();
    File fd(this->fd.name(), O_RDWR);
    sys::PreserveFileTimes pt(fd);
    off_t start_ofs = mds[data_idx].sourceBlob().offset;
    off_t end = fd.lseek(0, SEEK_END);
    std::vector<uint8_t> buf(end - start_ofs);
    fd.lseek(start_ofs);
    fd.read_all_or_throw(buf.data(), buf.size());
    fd.lseek(start_ofs - overlap_size);
    fd.write_all_or_throw(buf.data(), buf.size());
    fd.ftruncate(end - overlap_size);

    for (unsigned i = data_idx; i < mds.size(); ++i)
    {
        unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
        source->offset -= overlap_size;
        mds[i].set_source(move(source));
    }
}

void Segment::test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx)
{
    close();
    File fd(this->fd.name(), O_RDWR);
    sys::PreserveFileTimes pt(fd);
    off_t end = fd.lseek(0, SEEK_END);
    if (data_idx >= mds.size())
    {
        fd.ftruncate(end + hole_size);
    } else {
        off_t start_ofs = mds[data_idx].sourceBlob().offset;
        std::vector<uint8_t> buf(end - start_ofs);
        fd.lseek(start_ofs);
        fd.read_all_or_throw(buf.data(), buf.size());
        fd.lseek(start_ofs + hole_size);
        fd.write_all_or_throw(buf.data(), buf.size());

        for (unsigned i = data_idx; i < mds.size(); ++i)
        {
            unique_ptr<source::Blob> source(mds[i].sourceBlob().clone());
            source->offset += hole_size;
            mds[i].set_source(move(source));
        }
    }
}

void Segment::test_corrupt(const metadata::Collection& mds, unsigned data_idx)
{
    close();
    const auto& s = mds[data_idx].sourceBlob();
    File fd(this->fd.name(), O_RDWR);
    sys::PreserveFileTimes pt(fd);
    fd.lseek(s.offset);
    fd.write_all_or_throw("\0", 1);
}

Pending Segment::repack(
        const std::string& rootdir,
        const std::string& relname,
        metadata::Collection& mds,
        Segment* make_repack_segment(const std::string&, const std::string&),
        bool skip_validation,
        unsigned test_flags)
{
    struct Rename : public Transaction
    {
        sys::File src;
        std::string tmpabsname;
        std::string absname;
        bool fired = false;

        Rename(const std::string& tmpabsname, const std::string& absname)
            : src(absname, O_RDWR), tmpabsname(tmpabsname), absname(absname)
        {
            repack_lock();
        }

        virtual ~Rename()
        {
            if (!fired) rollback();
        }

        /**
         * Lock the whole segment for repacking
         */
        void repack_lock()
        {
            Lock lock;
            lock.l_type = F_WRLCK;
            lock.l_whence = SEEK_SET;
            lock.l_start = 0;
            lock.l_len = 0;
            lock.ofd_setlkw(src);
        }

        /**
         * Unlock the segment after repacking
         */
        void repack_unlock()
        {
            Lock lock;
            lock.l_type = F_UNLCK;
            lock.l_whence = SEEK_SET;
            lock.l_start = 0;
            lock.l_len = 0;
            lock.ofd_setlk(src);
        }

        virtual void commit()
        {
            if (fired) return;
            // Rename the data file to its final name
            try {
                if (rename(tmpabsname.c_str(), absname.c_str()) < 0)
                    throw_system_error("cannot rename " + tmpabsname + " to " + absname);
            } catch (...) {
                repack_unlock();
                throw;
            }
            repack_unlock();
            fired = true;
        }

        virtual void rollback()
        {
            if (fired) return;
            unlink(tmpabsname.c_str());
            repack_unlock();
            fired = true;
        }
    };

    string absname = str::joinpath(rootdir, relname);
    string tmprelname = relname + ".repack";
    string tmpabsname = absname + ".repack";

    // Make sure mds are not holding a read lock on the file to repack
    for (auto& md: mds) md->sourceBlob().unlock();

    // Get a validator for this file
    const scan::Validator& validator = scan::Validator::by_filename(absname);

    // Reacquire the lock here for writing
    Rename* rename;
    Pending p(rename = new Rename(tmpabsname, absname));

    // Create a writer for the temp file
    unique_ptr<Segment> writer(make_repack_segment(tmprelname, tmpabsname));

    if (test_flags & TEST_MISCHIEF_MOVE_DATA)
        writer->test_add_padding(1);

    // Fill the temp file with all the data in the right order
    for (metadata::Collection::const_iterator i = mds.begin(); i != mds.end(); ++i)
    {
        // Read the data
        auto buf = (*i)->sourceBlob().read_data(rename->src, false);
        // Validate it
        if (!skip_validation)
            validator.validate_buf(buf.data(), buf.size());
        // Append it to the new file
        off_t w_off = writer->append(buf);
        // Update the source information in the metadata
        (*i)->set_source(Source::createBlobUnlocked((*i)->source().format, rootdir, relname, w_off, buf.size()));
        // Drop the cached data, to prevent ending up with the whole segment
        // sitting in memory
        (*i)->drop_cached_data();
    }

    return p;
}

}
}
}
}
