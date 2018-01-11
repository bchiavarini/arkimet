#ifndef ARKI_DATASET_SEGMENT_H
#define ARKI_DATASET_SEGMENT_H

/// Dataset segment read/write functions

#include <arki/libconfig.h>
#include <arki/defs.h>
#include <arki/file.h>
#include <arki/transaction.h>
#include <arki/nag.h>
#include <string>
#include <vector>
#include <iosfwd>
#include <sys/types.h>
#include <memory>

namespace arki {
class Metadata;
class ConfigFile;

namespace metadata {
class Collection;
}

namespace types {
namespace source {
class Blob;
}
}

namespace scan {
class Validator;
}

namespace dataset {
class Reporter;
class Segment;

static const unsigned SEGMENT_OK          = 0;
static const unsigned SEGMENT_DIRTY       = 1 << 0; /// Segment contains data deleted or out of order
static const unsigned SEGMENT_UNALIGNED   = 1 << 1; /// Segment contents are inconsistent with the index
static const unsigned SEGMENT_MISSING     = 1 << 2; /// Segment is known to the index, but does not exist on disk
static const unsigned SEGMENT_DELETED     = 1 << 3; /// Segment contents have been entirely deleted
static const unsigned SEGMENT_CORRUPTED   = 1 << 4; /// File is broken in a way that needs manual intervention
static const unsigned SEGMENT_ARCHIVE_AGE = 1 << 5; /// File is old enough to be archived
static const unsigned SEGMENT_DELETE_AGE  = 1 << 6; /// File is old enough to be deleted

static const unsigned TEST_MISCHIEF_MOVE_DATA = 1; /// During repack, move all data to a different location than it was before

namespace segment {
class Writer;


/**
 * State of a file in a dataset, as one or more of the FILE_* flags
 */
struct State
{
    unsigned value;

    State() : value(SEGMENT_OK) {}
    State(unsigned value) : value(value) {}

    bool is_ok() const { return value == SEGMENT_OK; }

    bool has(unsigned state) const
    {
        return value & state;
    }

    State& operator+=(const State& fs)
    {
        value |= fs.value;
        return *this;
    }

    State& operator-=(const State& fs)
    {
        value &= ~fs.value;
        return *this;
    }

    State operator+(const State& fs) const
    {
        return State(value | fs.value);

    }

    State operator-(const State& fs) const
    {
        return State(value & ~fs.value);

    }

    bool operator==(const State& fs) const
    {
        return value == fs.value;
    }

    /// Return a text description of this file state
    std::string to_string() const;
};

/// Print to ostream
std::ostream& operator<<(std::ostream&, const State&);

/**
 * Visitor interface for scanning information about the segments in the database
 */
typedef std::function<void(const std::string&, segment::State)> state_func;

/**
 * Visitor interface for scanning information about the contents of segments in the database
 */
typedef std::function<void(const std::string&, segment::State, const metadata::Collection&)> contents_func;


namespace impl {

template<typename T, unsigned max_size=3>
class Cache
{
protected:
    std::shared_ptr<T> items[max_size];

    /// Move an existing element to the front of the list
    void move_to_front(unsigned pos)
    {
        // Save a pointer to the item
        std::shared_ptr<T> item = items[pos];
        // Shift all previous items forward
        for (unsigned i = pos; i > 0; --i)
            items[i] = items[i - 1];
        // Set the first element to item
        items[0] = item;
    }

public:
    ~Cache()
    {
    }

    void clear()
    {
        for (unsigned i = 0; i < max_size; ++i)
            items[i].reset();
    }

    /**
     * Get an element from the cache given its file name
     *
     * @returns 0 if not found, else a pointer to the element, whose memory is
     *          still managed by the cache
     */
    std::shared_ptr<T> get(const std::string& relname)
    {
        for (unsigned i = 0; i < max_size && items[i]; ++i)
            if (items[i]->relname == relname)
            {
                // Move to front if it wasn't already
                if (i > 0)
                    move_to_front(i);
                return items[0];
            }
        // Not found
        return std::shared_ptr<T>();
    }

    /**
     * Add an element to the cache, taking ownership of its memory management
     *
     * @returns the element itself, just for the convenience of being able to
     * type "return cache.add(Value::create());"
     */
    std::shared_ptr<T> add(std::shared_ptr<T> val)
    {
        // Shift all other elements forward
        for (unsigned i = max_size - 1; i > 0; --i)
            items[i] = items[i - 1];
        // Set the first element to val
        return items[0] = val;
    }

    void foreach_cached(std::function<void(T&)> func)
    {
        for (unsigned i = 0; i < max_size && items[i]; ++i)
            func(*items[i]);
    }
};

}

}

/**
 * Interface for managing a dataset segment.
 *
 * A dataset segment is a group of data elements stored on disk. It can be a
 * file with all data elements appended one after the other, for formats like
 * BUFR or GRIB that support it; it can be a text file with one data item per
 * line, for line based formats like VM2, or it can be a tar file or a
 * directory with each data item in a different file, for formats like HDF5
 * that cannot be trivially concatenated in the same file.
 */
class Segment
{
public:
    std::string root;
    std::string relname;
    std::string absname;

    // TODO: document and move to the right subclass if it's not needed all the time
    struct Payload
    {
        virtual ~Payload() {}
    };
    Payload* payload = nullptr;

    Segment(const std::string& root, const std::string& relname, const std::string& absname);
    virtual ~Segment();
};


namespace segment {

struct Writer : public Segment
{
    using Segment::Segment;

    /**
     * Append the data, in a transaction, updating md's source information.
     *
     * At the beginning of the transaction, the file is locked and the write
     * offset is read. Committing the transaction actually writes the data to
     * the file.
     *
     * Returns a reference to the blob source that will be set into \a md on
     * commit
     */
    virtual Pending append(Metadata& md, const types::source::Blob** new_source=0) = 0;
};


class Checker : public Segment
{
protected:
    virtual void validate(Metadata& md, const scan::Validator& v) = 0;

public:
    using Segment::Segment;

    virtual void lock() = 0;

    virtual segment::State check(dataset::Reporter& reporter, const std::string& ds, const metadata::Collection& mds, bool quick=true) = 0;
    virtual size_t remove() = 0;

    /**
     * Check if the segment exists on disk
     */
    virtual bool exists_on_disk() = 0;

    /**
     * Rewrite this segment so that the data are in the same order as in `mds`.
     *
     * `rootdir` is the directory to use as root for the Blob sources in `mds`.
     */
    virtual Pending repack(const std::string& rootdir, metadata::Collection& mds, unsigned test_flags=0) = 0;

    /**
     * Truncate the segment at the given offset
     */
    virtual void test_truncate(size_t offset) = 0;

    /**
     * Truncate the data at position `data_idx`
     */
    virtual void test_truncate(const metadata::Collection& mds, unsigned data_idx);

    /**
     * Move all the data in the segment starting from the one in position
     * `data_idx` forwards by `hole_size`.
     *
     * `mds` represents the state of the segment before the move, and is
     * updated to reflect the new state of the segment.
     */
    virtual void test_make_hole(metadata::Collection& mds, unsigned hole_size, unsigned data_idx) = 0;

    /**
     * Move all the data in the segment starting from the one in position
     * `data_idx` backwards by `overlap_size.
     *
     * `mds` represents the state of the segment before the move, and is
     * updated to reflect the new state of the segment.
     */
    virtual void test_make_overlap(metadata::Collection& mds, unsigned overlap_size, unsigned data_idx) = 0;

    /**
     * Corrupt the data at position `data_idx`, by replacing its first byte
     * with the value 0.
     */
    virtual void test_corrupt(const metadata::Collection& mds, unsigned data_idx) = 0;
};


/// Manage instantiating the right readers/writers for a dataset
class Manager
{
protected:
    impl::Cache<Writer> writers;
    std::string root;

    virtual std::shared_ptr<Writer> create_writer_for_format(const std::string& format, const std::string& relname, const std::string& absname) = 0;
    virtual std::shared_ptr<Checker> create_checker_for_format(const std::string& format, const std::string& relname, const std::string& absname) = 0;

public:
    Manager(const std::string& root);
    virtual ~Manager();

    /**
     * Empty the cache of segments, flushing and closing all files currently
     * open
     */
    void flush_writers();

    /// Run a function on each cached writer
    void foreach_cached_writer(std::function<void(Writer&)>);

    std::shared_ptr<Writer> get_writer(const std::string& relname);
    std::shared_ptr<Writer> get_writer(const std::string& format, const std::string& relname);

    std::shared_ptr<Checker> get_checker(const std::string& relname);
    std::shared_ptr<Checker> get_checker(const std::string& format, const std::string& relname);

    /**
     * Repack the file relname, so that it contains only the data in mds, in
     * the same order as in mds.
     *
     * The source metadata in mds will be updated to point to the new file.
     */
    virtual Pending repack(const std::string& relname, metadata::Collection& mds, unsigned test_flags=0) = 0;

    /**
     * Check the given file against its expected set of contents.
     *
     * @returns the State with the state of the file
     */
    virtual State check(dataset::Reporter& reporter, const std::string& ds, const std::string& relname, const metadata::Collection& mds, bool quick=true) = 0;

    /**
     * Truncate a file at the given offset
     *
     * This function is useful for implementing unit tests.
     */
    virtual void test_truncate(const std::string& relname, size_t offset) = 0;

    /**
     * Given the relative path of a segment, return true if it exists on disk
     */
    virtual bool exists(const std::string& relpath) const = 0;

    /**
     * Scan a dataset for data files, returning a set of pathnames relative to
     * root.
     */
    virtual void scan_dir(std::function<void(const std::string& relname)> dest) = 0;

    /// Create a Manager
    static std::unique_ptr<Manager> get(const std::string& root, bool force_dir=false, bool mock_data=false);
};

}

}
}
#endif
