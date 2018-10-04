#ifndef ARKI_DATASET_TESTS_H
#define ARKI_DATASET_TESTS_H

#include <arki/metadata/tests.h>
#include <arki/configfile.h>
#include <arki/types/fwd.h>
#include <arki/metadata.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/dataset.h>
#include <arki/dataset/segment.h>
#include <arki/dataset/segmented.h>
#include <arki/libconfig.h>
#include <vector>
#include <string>

namespace arki {
struct Metadata;
struct Dispatcher;

namespace dataset {
struct Reader;
struct Writer;
struct Checker;
struct LocalConfig;
struct LocalReader;
struct LocalWriter;
struct LocalChecker;
struct ArchivesReader;
struct ArchivesChecker;

namespace segmented {
struct Reader;
struct Writer;
struct Checker;
}

namespace ondisk2 {
struct Config;
struct Reader;
struct Writer;
struct Checker;
}

namespace simple {
struct Reader;
struct Writer;
struct Checker;
}

namespace iseg {
struct Reader;
struct Writer;
struct Checker;
}
}

namespace tests {

// Return the number of days passed from the given date until today
int days_since(int year, int month, int day);

// Return the file name of the Manifest index
std::string manifest_idx_fname();


/**
 * State of all segments in the dataset
 */
struct State : public std::map<std::string, dataset::segmented::SegmentState>
{
    using std::map<std::string, dataset::segmented::SegmentState>::map;

    bool has(const std::string& relpath) const;

    const dataset::segmented::SegmentState& get(const std::string& seg) const;

    /// Count how many segments have this state
    unsigned count(segment::State state) const;

    void dump(FILE* out) const;
};


/**
 * Test fixture to test a dataset.
 *
 * It is initialized with the dataset configuration and takes care of
 * instantiating readers, writers and checkers, and to provide common functions
 * to test them.
 */
struct DatasetTest : public Fixture
{
protected:
    /**
     * Parsed dataset configuration, cleared each time by test_teardown() and
     * generated by config()
     */
    std::shared_ptr<const dataset::Config> m_config;

public:
    enum Counted {
        COUNTED_OK,
        COUNTED_ARCHIVE_AGE,
        COUNTED_DELETE_AGE,
        COUNTED_DIRTY,
        COUNTED_DELETED,
        COUNTED_UNALIGNED,
        COUNTED_MISSING,
        COUNTED_CORRUPTED,
        COUNTED_MAX,
    };

    /*
     * Default dataset configuration, regenerated each time by test_setup by
     * concatenating cfg_default and cfg_instance.
     *
     * The 'path' value of the configuration will always be set to the absolute
     * path of the root of the dataset (ds_root)
     *
     * The 'name' value of the configuration will always be set to ds_name.
     */
    ConfigFile cfg;
    // Extra configuration for this instance of this fixture
    std::string cfg_instance;
    // Dataset name (always "testds")
    std::string ds_name;
    // Dataset root directory
    std::string ds_root;
    dataset::SegmentManager* segment_manager = nullptr;
    std::vector<Metadata> import_results;

    /**
     * @param cfg_tail
     *   Snippet of configuration that will be parsed by test_setup
     */
    DatasetTest(const std::string& cfg_instance=std::string());
    ~DatasetTest();

    /**
     * Build cfg based on cfg_default and cfg_instance, and remove the dataset
     * directory if it exists.
     */
    void test_setup(const std::string& cfg_default=std::string());
    void test_teardown();
    /// Clear cached dataset::* instantiations
    void test_reread_config();
    /**
     * Teardown and setup test again, to run two different configuration in the
     * same test case
     */
    void reset_test(const std::string& cfg_default=std::string());

    const dataset::Config& config();
    std::shared_ptr<const dataset::Config> dataset_config();
    std::shared_ptr<const dataset::LocalConfig> local_config();
    std::shared_ptr<const dataset::ondisk2::Config> ondisk2_config();

    dataset::SegmentManager& segments();

    // Return the file name of the index of the current dataset
    std::string idxfname(const ConfigFile* wcfg = 0) const;

    /**
     * Return the segment pathname in the current dataset where md is expected
     * to have been imported
     */
    std::string destfile(const Metadata& md) const;

    /**
     * Return the segment pathname in the current dataset where md is expected
     * to have been archived
     */
    std::string archive_destfile(const Metadata& md) const;

    /**
     * Return all the distinct segment pathnames in the current dataset after f
     * has been imported
     */
    std::set<std::string> destfiles(const metadata::Collection& mds) const;

    /**
     * Return the number of distinct dataset segments created by importing f in
     * the test dataset
     */
    unsigned count_dataset_files(const metadata::Collection& f) const;

    /// Scan the dataset and return its state
    State scan_state(bool quick=true);

    /// Scan the dataset and return its state
    State scan_state(const Matcher& matcher, bool quick=true);

    std::unique_ptr<dataset::segmented::Reader> makeSegmentedReader();
    std::unique_ptr<dataset::segmented::Writer> makeSegmentedWriter();
    std::unique_ptr<dataset::segmented::Checker> makeSegmentedChecker();
    std::unique_ptr<dataset::ondisk2::Reader> makeOndisk2Reader();
    std::unique_ptr<dataset::ondisk2::Writer> makeOndisk2Writer();
    std::unique_ptr<dataset::ondisk2::Checker> makeOndisk2Checker();
    std::unique_ptr<dataset::simple::Reader> makeSimpleReader();
    std::unique_ptr<dataset::simple::Writer> makeSimpleWriter();
    std::unique_ptr<dataset::simple::Checker> makeSimpleChecker();
    std::unique_ptr<dataset::iseg::Reader> makeIsegReader();
    std::unique_ptr<dataset::iseg::Writer> makeIsegWriter();
    std::unique_ptr<dataset::iseg::Checker> makeIsegChecker();

    // Clean the dataset directory
    void clean();

    // Import a file
    void import(const std::string& testfile="inbound/test.grib1");

    // Import a metadata collection
    void import(metadata::Collection& mds);

    // Import a datum
    void import(Metadata& md, dataset::WriterAcquireResult expected_result=dataset::ACQ_OK);

    // Recreate the dataset importing data into it
    void clean_and_import(const std::string& testfile="inbound/test.grib1");

    metadata::Collection query(const dataset::DataQuery& q);

    void ensure_localds_clean(size_t filecount, size_t resultcount, bool quick=true);

    /// Test the state of all segments in the local dataset is clean
    void all_clean(size_t segment_count);

    void import_all(const metadata::Collection& mds);
    void import_all_packed(const metadata::Collection& mds);
    void repack();

    /// Equivalent to calling query_results with an empty query
    void query_results(const std::vector<int>& expected);

    /**
     * Run the query on the test dataset, and make sure that the results are
     * the metadata elements in import_results corresponding to the given
     * sequence of indices
     */
    void query_results(const dataset::DataQuery& q, const std::vector<int>& expected);

    /**
     * Tweak the dataset so that the segment needs rescanning.
     *
     * This is done in a dataset-dependent way
     */
    void make_unaligned(const std::string& segment);

    /**
     * Check if the segment exists online on this dataset, with the given
     * extensions. ".metadata" and ".summary" are added in case of "simple"
     * datasets.
     */
    void online_segment_exists(const std::string& relpath, const std::vector<std::string>& extensions);

    /**
     * Check if the segment exists online on this dataset, with the given
     * extensions. ".metadata" and ".summary" are added automatically.
     */
    void archived_segment_exists(const std::string& relpath, const std::vector<std::string>& extensions);

    /**
     * Raise TestSkipped if the current dataset has type 'simple'
     */
    void skip_if_type_simple();
};

}

namespace tests {

template<typename T>
static std::string nfiles(const T& val)
{
    if (val == 1)
        return std::to_string(val) + " file";
    else
        return std::to_string(val) + " files";
}


struct ReporterExpected
{
    struct OperationMatch
    {
        // Dataset name
        std::string name;
        // Operation name: "check", "repack"
        std::string operation;
        std::string message;

        OperationMatch(const std::string& dsname, const std::string& operation, const std::string& message=std::string());
        std::string error_unmatched(const std::string& type) const;
    };

    struct SegmentMatch
    {
        // "dataset_name:segment_name"
        std::string name;
        std::string message;

        SegmentMatch(const std::string& dsname, const std::string& relpath, const std::string& message=std::string());
        std::string error_unmatched(const std::string& operation) const;
    };

    std::vector<OperationMatch> progress;
    std::vector<OperationMatch> manual_intervention;
    std::vector<OperationMatch> aborted;
    std::vector<OperationMatch> report;

    std::vector<SegmentMatch> repacked;
    std::vector<SegmentMatch> archived;
    std::vector<SegmentMatch> deleted;
    std::vector<SegmentMatch> deindexed;
    std::vector<SegmentMatch> rescanned;
    std::vector<SegmentMatch> tarred;
    std::vector<SegmentMatch> compressed;
    std::vector<SegmentMatch> issue51;

    int count_repacked = -1;
    int count_archived = -1;
    int count_deleted = -1;
    int count_deindexed = -1;
    int count_rescanned = -1;
    int count_tarred = -1;
    int count_compressed = -1;
    int count_issue51 = -1;

    unsigned flags;

    void clear();

    static const unsigned ENFORCE_REPORTS = 1 << 0;

    ReporterExpected(unsigned flags=0);
};


template<typename DatasetWriter>
struct ActualWriter : public arki::utils::tests::Actual<DatasetWriter*>
{
    ActualWriter(DatasetWriter* s) : Actual<DatasetWriter*>(s) {}
    void import(Metadata& md);
    void import(metadata::Collection& mds, dataset::Writer::ReplaceStrategy strategy=dataset::Writer::REPLACE_DEFAULT);
};

inline arki::tests::ActualWriter<dataset::LocalWriter> actual(arki::dataset::LocalWriter* actual)
{
    return arki::tests::ActualWriter<dataset::LocalWriter>(actual);
}
inline arki::tests::ActualWriter<dataset::Writer> actual(arki::dataset::Writer* actual) { return arki::tests::ActualWriter<dataset::Writer>(actual); }
inline arki::tests::ActualWriter<dataset::Writer> actual(arki::dataset::Writer& actual) { return arki::tests::ActualWriter<dataset::Writer>(&actual); }
inline arki::tests::ActualWriter<dataset::Writer> actual(arki::dataset::segmented::Writer* actual) { return arki::tests::ActualWriter<dataset::Writer>(actual); }
inline arki::tests::ActualWriter<dataset::Writer> actual(arki::dataset::segmented::Writer& actual) { return arki::tests::ActualWriter<dataset::Writer>(&actual); }
inline arki::tests::ActualWriter<dataset::Writer> actual(arki::dataset::simple::Writer& actual) { return arki::tests::ActualWriter<dataset::Writer>((arki::dataset::segmented::Writer*)&actual); }
inline arki::tests::ActualWriter<dataset::Writer> actual(arki::dataset::ondisk2::Writer& actual) { return arki::tests::ActualWriter<dataset::Writer>((arki::dataset::segmented::Writer*)&actual); }
inline arki::tests::ActualWriter<dataset::Writer> actual(arki::dataset::iseg::Writer& actual) { return arki::tests::ActualWriter<dataset::Writer>((arki::dataset::segmented::Writer*)&actual); }


template<typename DatasetChecker>
struct ActualChecker : public arki::utils::tests::Actual<DatasetChecker*>
{
    ActualChecker(DatasetChecker* s) : Actual<DatasetChecker*>(s) {}

    void repack(const ReporterExpected& expected, bool write=false);
    void repack_clean(bool write=false);
    void repack_filtered(const Matcher& matcher, const ReporterExpected& expected, bool write=false);
    void repack_filtered_clean(const Matcher& matcher, bool write=false);
    void check(const ReporterExpected& expected, bool write=false, bool quick=true);
    void check(const ReporterExpected& expected, dataset::CheckerConfig& opts);
    void check_clean(bool write=false, bool quick=true);
    void check_filtered(const Matcher& matcher, const ReporterExpected& expected, bool write=false, bool quick=true);
    void check_filtered_clean(const Matcher& matcher, bool write=false, bool quick=true);
    void check_issue51(const ReporterExpected& expected, bool write=false);
    void check_issue51_clean(bool write=false);
    void remove_all(const ReporterExpected& expected, bool write=false);
    void remove_all_filtered(const Matcher& matcher, const ReporterExpected& expected, bool write=false);
};

inline arki::tests::ActualChecker<dataset::LocalChecker> actual(arki::dataset::LocalChecker* actual)
{
    return arki::tests::ActualChecker<dataset::LocalChecker>(actual);
}
inline arki::tests::ActualChecker<dataset::Checker> actual(arki::dataset::Checker* actual) { return arki::tests::ActualChecker<dataset::Checker>(actual); }
inline arki::tests::ActualChecker<dataset::Checker> actual(arki::dataset::Checker& actual) { return arki::tests::ActualChecker<dataset::Checker>(&actual); }
inline arki::tests::ActualChecker<dataset::Checker> actual(arki::dataset::segmented::Checker* actual) { return arki::tests::ActualChecker<dataset::Checker>(actual); }
inline arki::tests::ActualChecker<dataset::Checker> actual(arki::dataset::segmented::Checker& actual) { return arki::tests::ActualChecker<dataset::Checker>(&actual); }
inline arki::tests::ActualChecker<dataset::Checker> actual(arki::dataset::simple::Checker& actual) { return arki::tests::ActualChecker<dataset::Checker>((arki::dataset::segmented::Checker*)&actual); }
inline arki::tests::ActualChecker<dataset::Checker> actual(arki::dataset::ondisk2::Checker& actual) { return arki::tests::ActualChecker<dataset::Checker>((arki::dataset::segmented::Checker*)&actual); }
inline arki::tests::ActualChecker<dataset::Checker> actual(arki::dataset::iseg::Checker& actual) { return arki::tests::ActualChecker<dataset::Checker>((arki::dataset::segmented::Checker*)&actual); }
inline arki::tests::ActualChecker<dataset::Checker> actual(arki::dataset::ArchivesChecker& actual) { return arki::tests::ActualChecker<dataset::Checker>((dataset::Checker*)&actual); }

}
}

#endif
