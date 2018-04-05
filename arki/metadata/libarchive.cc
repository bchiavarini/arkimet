#include "libarchive.h"
#include "arki/metadata.h"
#include "arki/binary.h"
#include "arki/exceptions.h"
#include "arki/utils/sys.h"
#include "arki/types/source/blob.h"
#include "arki/types/reftime.h"
#include <archive.h>
#include <archive_entry.h>
#include <cstring>

namespace arki {
namespace metadata {

#ifdef HAVE_LIBARCHIVE
struct archive_runtime_error: public std::runtime_error
{
    archive_runtime_error(struct ::archive* a, const std::string& msg)
        : std::runtime_error(msg + ": " + archive_error_string(a))
    {
    }
};
#endif

LibarchiveOutput::LibarchiveOutput(const std::string& format, core::NamedFileDescriptor& out)
    : format(format), out(out)
{
#ifdef HAVE_LIBARCHIVE
    a = archive_write_new();
    if (a == nullptr)
        throw_system_error("archive_write_new failed");
    entry = archive_entry_new();
    if (entry == nullptr)
        throw_system_error("archive_entry_new failed");

    if (format == "tar")
    {
        if (archive_write_set_format_gnutar(a) != ARCHIVE_OK)
            throw archive_runtime_error(a, "cannot set tar archive format");
    }
    else if (format == "tar.gz")
    {
        if (archive_write_set_format_gnutar(a) != ARCHIVE_OK)
            throw archive_runtime_error(a, "cannot set tar archive format");
        if (archive_write_add_filter_gzip(a) != ARCHIVE_OK)
            throw archive_runtime_error(a, "cannot add gzip compression");
    }
    else if (format == "tar.xz")
    {
        if (archive_write_set_format_gnutar(a) != ARCHIVE_OK)
            throw archive_runtime_error(a, "cannot set tar archive format");
        if (archive_write_add_filter_lzma(a) != ARCHIVE_OK)
            throw archive_runtime_error(a, "cannot add lzma compression");
    }
    else if (format == "zip")
    {
        if (archive_write_set_format_zip(a) != ARCHIVE_OK)
            throw archive_runtime_error(a, "cannot set zip archive format");
    }

    if (archive_write_open_fd(a, out) != ARCHIVE_OK)
        throw archive_runtime_error(a, "archive_write_open_fd failed");
#endif
}

#ifdef HAVE_LIBARCHIVE
LibarchiveOutput::~LibarchiveOutput()
{
    archive_entry_free(entry);
    archive_write_free(a);
}
#endif

void LibarchiveOutput::append(const Metadata& md)
{
#ifndef HAVE_LIBARCHIVE
    throw std::runtime_error("libarchive not supported in this build");
#else
    snprintf(filename_buf, 255, "data/%06zd.%s", mds.size() + 1, md.source().format.c_str());
    std::unique_ptr<Metadata> stored_md = Metadata::create_copy(md);
    const std::vector<uint8_t>& stored_data = stored_md->getData();
    std::unique_ptr<types::Source> stored_source = types::Source::createBlobUnlocked(md.source().format, "", filename_buf, 0, stored_data.size());


    archive_entry_clear(entry);
    archive_entry_set_pathname(entry, filename_buf);
    archive_entry_set_size(entry, stored_data.size());
    archive_entry_set_filetype(entry, AE_IFREG);
    archive_entry_set_perm(entry, 0644);
    if (const auto* reftime = stored_md->get<types::reftime::Position>())
        archive_entry_set_mtime(entry, reftime->time.to_unix(), 0);

    if (archive_write_header(a, entry) != ARCHIVE_OK)
        throw archive_runtime_error(a, "cannot write entry header");

    size_t ofs = 0;
    while (ofs < stored_data.size())
    {
        la_ssize_t written = archive_write_data(a, stored_data.data() + ofs, stored_data.size() - ofs);
        if (written < 0)
            throw archive_runtime_error(a, "cannot write entry data");
        /*
         * archive_write_data(3) says:
         * In libarchive 3.x, this function sometimes returns zero on success
         * instead of returning the number of bytes written.  Specifically,
         * this occurs when writing to an archive_write_disk handle.  Clients
         * should treat any value less than zero as an error and consider any
         * non-negative value as success.
         *
         * Assuming that 0 means "all was written" and breaking out of the
         * loop, although we are not using archive_write_disk, so it should
         * never happen.
         */
        if (written == 0)
            break;
        ofs += written;
    }

    stored_md->drop_cached_data();
    mds.acquire(std::move(stored_md));
#endif
}

void LibarchiveOutput::flush()
{
#ifdef HAVE_LIBARCHIVE
    std::vector<uint8_t> buf;
    BinaryEncoder enc(buf);
    for (const auto& md: mds)
        md->encodeBinary(enc);

    archive_entry_clear(entry);
    archive_entry_set_pathname(entry, "data/metadata.md");
    archive_entry_set_size(entry, buf.size());
    archive_entry_set_filetype(entry, AE_IFREG);
    archive_entry_set_perm(entry, 0644);
    archive_entry_set_mtime(entry, time(nullptr), 0);
    if (archive_write_header(a, entry) != ARCHIVE_OK)
        throw archive_runtime_error(a, "cannot write entry header");

    size_t ofs = 0;
    while (ofs < buf.size())
    {
        la_ssize_t written = archive_write_data(a, buf.data() + ofs, buf.size() - ofs);
        if (written < 0)
            throw archive_runtime_error(a, "cannot write entry data");
        /*
         * archive_write_data(3) says:
         * In libarchive 3.x, this function sometimes returns zero on success
         * instead of returning the number of bytes written.  Specifically,
         * this occurs when writing to an archive_write_disk handle.  Clients
         * should treat any value less than zero as an error and consider any
         * non-negative value as success.
         *
         * Assuming that 0 means "all was written" and breaking out of the
         * loop, although we are not using archive_write_disk, so it should
         * never happen.
         */
        if (written == 0)
            break;
        ofs += written;
    }

    if (archive_write_close(a) != ARCHIVE_OK)
        throw archive_runtime_error(a, "cannot close archive");
#endif
}

}
}
