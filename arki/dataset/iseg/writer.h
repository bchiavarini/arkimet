#ifndef ARKI_DATASET_ISEG_WRITER_H
#define ARKI_DATASET_ISEG_WRITER_H

/// dataset/iseg/writer - Writer for datasets with one index per segment

#include <arki/dataset/iseg.h>
#include <arki/configfile.h>

#include <string>
#include <iosfwd>

namespace arki {
class Matcher;

namespace dataset {
namespace iseg {
class Reader;

class Writer : public segmented::Writer
{
protected:
    std::shared_ptr<const iseg::Config> m_config;

    /// Return a (shared) instance of the Datafile for the given relative pathname
    Segment* file(const Metadata& md, const std::string& format);

public:
    Writer(std::shared_ptr<const iseg::Config> config);
    virtual ~Writer();

    const iseg::Config& config() const override { return *m_config; }

    std::string type() const override;

    AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) override;
    void remove(Metadata& md);
    void flush() override;

    //virtual Pending test_writelock();

    static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};


class Checker : public segmented::Checker
{
protected:
    std::shared_ptr<const iseg::Config> m_config;

    /// Return a (shared) instance of the Segment for the given relative pathname
    Segment* file(const Metadata& md, const std::string& format);

public:
    Checker(std::shared_ptr<const iseg::Config> config);
    virtual ~Checker();

    const iseg::Config& config() const override { return *m_config; }

    std::string type() const override;

    void removeAll(dataset::Reporter& reporter, bool writable=false) override;
    segmented::State scan(dataset::Reporter& reporter, bool quick=true) override;
    void repack(dataset::Reporter& reporter, bool writable=false) override;
    void check(dataset::Reporter& reporter, bool fix, bool quick) override;

    void indexSegment(const std::string& relpath, metadata::Collection&& contents) override;
    void rescanSegment(const std::string& relpath) override;
    size_t repackSegment(const std::string& relpath) override;
    void releaseSegment(const std::string& relpath, const std::string& destpath) override;
    size_t removeSegment(const std::string& relpath, bool withData=false) override;
    size_t vacuum() override;
};

}
}
}
#endif
