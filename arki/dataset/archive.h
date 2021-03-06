#ifndef ARKI_DATASET_ARCHIVE_H
#define ARKI_DATASET_ARCHIVE_H

/// Handle archived data

#include <arki/dataset.h>
#include <arki/summary.h>
#include <arki/core/fwd.h>
#include <arki/metadata/fwd.h>
#include <arki/matcher/fwd.h>
#include <arki/dataset/fwd.h>
#include <string>
#include <map>
#include <iosfwd>

namespace arki {
class Summary;

namespace dataset {

namespace segmented {
class Checker;
class CheckerSegment;
}

namespace index {
class Manifest;
}

namespace simple {
class Reader;
}

namespace archive {

class ArchivesReaderRoot;
class ArchivesCheckerRoot;

bool is_archive(const std::string& dir);

}

struct ArchivesConfig : public dataset::Config
{
    std::string root;

    ArchivesConfig(const std::string& root);

    static std::shared_ptr<const ArchivesConfig> create(const std::string& root);
};

/**
 * Set of archives.
 *
 * Every archive is a subdirectory. Subdirectories are always considered in
 * alphabetical order, except for a subdirectory named "last". The subdirectory
 * named "last" if it exists it always appears last.
 *
 * The idea is to have one archive called "last" where data is automatically
 * archived from the live dataset, and other archives maintained manually by
 * moving files from "last" into them.
 *
 * ondisk2 dataset maintenance will therefore only write files older than
 * "archive age" to the "last" archive.
 *
 * When doing archive maintenance, all archives are checked. If a file is
 * manually moved from an archive to another, it will be properly deindexed and
 * reindexed during the archive maintenance.
 *
 * When querying, all archives are queried, following the archive order:
 * alphabetical order except the archive named "last" is queried last.
 */
class ArchivesReader : public Reader
{
protected:
    std::shared_ptr<const ArchivesConfig> m_config;
    archive::ArchivesReaderRoot* archives = nullptr;

    void summary_for_all(Summary& out);

public:
    ArchivesReader(std::shared_ptr<const ArchivesConfig> config);
    virtual ~ArchivesReader();

    std::string type() const override;
    const ArchivesConfig& config() const override { return *m_config; }

    void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end) const;
    bool query_data(const dataset::DataQuery& q, metadata_dest_func) override;
    void query_bytes(const dataset::ByteQuery& q, core::NamedFileDescriptor& out) override;
    void query_bytes(const dataset::ByteQuery& q, core::AbstractOutputFile& out) override;
    void query_summary(const Matcher& matcher, Summary& summary) override;

    /// Return the number of archives found, used for testing
    unsigned test_count_archives() const;
};

class ArchivesChecker : public Checker
{
protected:
    std::shared_ptr<const ArchivesConfig> m_config;
    archive::ArchivesCheckerRoot* archives = nullptr;

public:
    /// Create an archive for the dataset at the given root dir.
    ArchivesChecker(std::shared_ptr<const ArchivesConfig> config);
    virtual ~ArchivesChecker();

    std::string type() const override;
    const ArchivesConfig& config() const override { return *m_config; }

    void index_segment(const std::string& relpath, metadata::Collection&& mds);
    void release_segment(const std::string& relpath, const std::string& new_root, const std::string& new_relpath, const std::string& new_abspath);
    void segments_recursive(CheckerConfig& opts, std::function<void(segmented::Checker&, segmented::CheckerSegment&)> dest);

    void remove_old(CheckerConfig& opts) override;
    void remove_all(CheckerConfig& opts) override;
    void repack(CheckerConfig& opts, unsigned test_flags=0) override;
    void check(CheckerConfig& opts) override;
    void check_issue51(CheckerConfig& opts) override;
    void tar(CheckerConfig& config) override;
    void zip(CheckerConfig& config) override;
    void compress(CheckerConfig& config, unsigned groupsize) override;
    void state(CheckerConfig& config) override;

    /// Return the number of archives found, used for testing
    unsigned test_count_archives() const;
};

}
}

#endif
