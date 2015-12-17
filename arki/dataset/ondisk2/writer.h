#ifndef ARKI_DATASET_ONDISK2_WRITER_H
#define ARKI_DATASET_ONDISK2_WRITER_H

/// dataset/ondisk2/writer - Local on disk dataset writer

#include <arki/dataset/local.h>
#include <arki/configfile.h>
#include <arki/dataset/index/contents.h>
#include <string>
#include <memory>

namespace arki {
class Metadata;
class Matcher;
class Summary;

namespace types {
class AssignedDataset;
}

namespace dataset {
namespace maintenance {
class MaintFileVisitor;
}

namespace ondisk2 {
class Archive;
class Archives;

namespace writer {
class RealRepacker;
class RealFixer;
}

class Writer : public SegmentedWriter
{
protected:
	ConfigFile m_cfg;
    index::WContents m_idx;

    AcquireResult acquire_replace_never(Metadata& md);
    AcquireResult acquire_replace_always(Metadata& md);
    AcquireResult acquire_replace_higher_usn(Metadata& md);

public:
	// Initialise the dataset with the information from the configurationa in 'cfg'
	Writer(const ConfigFile& cfg);

	virtual ~Writer();

    /**
     * Acquire the given metadata item (and related data) in this dataset.
     *
     * After acquiring the data successfully, the data can be retrieved from
     * the dataset.  Also, information such as the dataset name and the id of
     * the data in the dataset are added to the Metadata object.
     *
     * @return true if the data is successfully stored in the dataset, else
     * false.  If false is returned, a note is added to the dataset explaining
     * the reason of the failure.
     */
    virtual AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT);

    virtual void remove(Metadata& md);

    virtual void flush();
    virtual Pending test_writelock();

	virtual void maintenance(maintenance::MaintFileVisitor& v, bool quick=true);
	virtual void sanityChecks(std::ostream& log, bool writable=false);
	virtual void removeAll(std::ostream& log, bool writable);

	virtual void rescanFile(const std::string& relpath);
	virtual size_t repackFile(const std::string& relpath);
	virtual size_t removeFile(const std::string& relpath, bool withData=false);
	virtual void archiveFile(const std::string& relpath);
	virtual size_t vacuum();

	/**
	 * Iterate through the contents of the dataset, in depth-first order.
	 */
	//void depthFirstVisit(Visitor& v);

	static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);

	friend class writer::RealRepacker;
	friend class writer::RealFixer;
};

/**
 * Temporarily override the current date used to check data age.
 *
 * This is used to be able to write unit tests that run the same independently
 * of when they are run.
 */
struct TestOverrideCurrentDateForMaintenance
{
    time_t old_ts;

    TestOverrideCurrentDateForMaintenance(time_t ts);
    ~TestOverrideCurrentDateForMaintenance();
};

}
}
}

// vim:set ts=4 sw=4:
#endif
