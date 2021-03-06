#ifndef ARKI_DATASET_SEGMENT_H
#define ARKI_DATASET_SEGMENT_H

/// Dataset segment read/write functions

#include <arki/libconfig.h>
#include <arki/defs.h>
#include <arki/core/fwd.h>
#include <arki/segment.h>
#include <string>
#include <iosfwd>
#include <memory>

namespace arki {
class ConfigFile;

namespace scan {
class Validator;
}

namespace dataset {
class Reporter;

/// Manage instantiating the right readers/writers for a dataset
class SegmentManager
{
protected:
    std::string root;

    virtual std::shared_ptr<segment::Writer> create_writer_for_format(const std::string& format, const std::string& relpath, const std::string& abspath) = 0;
    virtual std::shared_ptr<segment::Checker> create_checker_for_format(const std::string& format, const std::string& relpath, const std::string& abspath) = 0;

public:
    SegmentManager(const std::string& root);
    virtual ~SegmentManager();

    std::shared_ptr<segment::Reader> get_reader(const std::string& format, const std::string& relpath, std::shared_ptr<core::Lock> lock);
    std::shared_ptr<segment::Writer> get_writer(const std::string& format, const std::string& relpath);
    std::shared_ptr<segment::Checker> get_checker(const std::string& format, const std::string& relpath);

    /**
     * Scan a dataset for data files, returning a set of pathnames relative to
     * root.
     */
    virtual void scan_dir(std::function<void(const std::string& relpath)> dest) = 0;

    /// Create a Manager
    static std::unique_ptr<SegmentManager> get(const std::string& root, bool force_dir=false, bool mock_data=false);
};

}
}
#endif
