#ifndef ARKI_DATASET_TESTUTILS_H
#define ARKI_DATASET_TESTUTILS_H

#include <arki/metadata/tests.h>
#include <arki/configfile.h>
#include <arki/types.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/metadata/collection.h>
#include <arki/matcher.h>
#include <arki/dataset.h>
#include <arki/dataset/maintenance.h>
#include <arki/dataset/segment.h>
#include <arki/dataset/segmented.h>
#include <arki/sort.h>
#include <arki/scan/any.h>
#include <arki/utils/string.h>
#include <arki/libconfig.h>
#include <vector>
#include <string>
#include <sstream>

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

namespace testdata {
struct Fixture;
struct Element;
}

namespace tests {

unsigned count_results(dataset::Reader& ds, const dataset::DataQuery& dq);

// Return the number of days passed from the given date until today
int days_since(int year, int month, int day);

// Return the file name of the Manifest index
std::string manifest_idx_fname();

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
    dataset::segment::SegmentManager* segment_manager = nullptr;
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

    dataset::segment::SegmentManager& segments();

    // Return the file name of the index of the current dataset
    std::string idxfname(const ConfigFile* wcfg = 0) const;

    /**
     * Return the segment pathname in the current dataset where el is expected
     * to have been imported
     */
    std::string destfile(const testdata::Element& el) const;

    /**
     * Return the segment pathname in the current dataset where el is expected
     * to have been archived
     */
    std::string archive_destfile(const testdata::Element& el) const;

    /**
     * Return all the distinct segment pathnames in the current dataset after f
     * has been imported
     */
    std::set<std::string> destfiles(const testdata::Fixture& f) const;

    /**
     * Return the number of distinct dataset segments created by importing f in
     * the test dataset
     */
    unsigned count_dataset_files(const testdata::Fixture& f) const;

    /// Scan the dataset and return its state
    dataset::segmented::State scan_state();

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

    // Recreate the dataset importing data into it
    void clean_and_import(const std::string& testfile="inbound/test.grib1");

    metadata::Collection query(const dataset::DataQuery& q);

    void ensure_localds_clean(size_t filecount, size_t resultcount);

    void import_all(const testdata::Fixture& fixture);
    void import_all_packed(const testdata::Fixture& fixture);

    bool has_smallfiles();
};

}

struct MaintenanceCollector
{
    typedef tests::DatasetTest::Counted Counted;

    std::map <std::string, dataset::segment::State> fileStates;
    size_t counts[tests::DatasetTest::COUNTED_MAX];
    static const char* names[];
    std::set<Counted> checked;

    MaintenanceCollector();
    MaintenanceCollector(const MaintenanceCollector&) = delete;
    MaintenanceCollector& operator=(const MaintenanceCollector&) = delete;

    void clear();
    bool isClean() const;
    void operator()(const std::string& file, dataset::segment::State state);
    void dump(std::ostream& out) const;
    size_t count(tests::DatasetTest::Counted state);
    std::string remaining() const;
};

struct OrderCheck
{
    std::shared_ptr<sort::Compare> order;
    Metadata old;
    bool first;

    OrderCheck(const std::string& order);
    ~OrderCheck();
    bool eat(std::unique_ptr<Metadata>&& md);
};

namespace testdata {

struct Element
{
    Metadata md;
    core::Time time;
    Matcher matcher;

    Element() : time(0, 0, 0) {}

    void set(const Metadata& md, const std::string& matcher)
    {
        const types::reftime::Position* rt = md.get<types::reftime::Position>();
        this->md = md;
        this->time = rt->time;
        this->matcher = Matcher::parse(matcher);
    }

    std::string data()
    {
        auto res = md.getData();
        return std::string(res.begin(), res.end());
    }
};

struct Fixture
{
    std::string format;
    // Maximum aggregation period that still generates more than one file
    std::string max_selective_aggregation;
    // Index of metadata item that is in a segment by itself when the
    // aggregation period is max_selective_aggregation
    unsigned max_selective_aggregation_singleton_index;
    Element test_data[3];
    /// Date that falls somewhere inbetween files in the dataset
    core::Time selective_cutoff;

    // Value for "archive age" or "delete age" that would work on part of the
    // dataset, but not all of it
    unsigned selective_days_since() const;
    void finalise_init();

    // Return the element with the earliest reftime
    Element& earliest_element();
};

struct GRIBData : Fixture
{
    GRIBData()
    {
        metadata::Collection mdc("inbound/test.grib1");
        format = "grib";
        max_selective_aggregation = "monthly";
        max_selective_aggregation_singleton_index = 2;
        test_data[0].set(mdc[0], "reftime:=2007-07-08");
        test_data[1].set(mdc[1], "reftime:=2007-07-07");
        test_data[2].set(mdc[2], "reftime:=2007-10-09");
        finalise_init();
    }
};

struct BUFRData : Fixture
{
    BUFRData()
    {
#ifdef HAVE_DBALLE
        metadata::Collection mdc("inbound/test.bufr");
        format = "bufr";
        max_selective_aggregation = "yearly";
        max_selective_aggregation_singleton_index = 0;
        test_data[0].set(mdc[0], "reftime:=2005-12-01");
        test_data[1].set(mdc[1], "reftime:=2004-11-30; proddef:GRIB:blo=60");
        test_data[2].set(mdc[2], "reftime:=2004-11-30; proddef:GRIB:blo=6");
        finalise_init();
#endif
    }
};

struct VM2Data : Fixture
{
    VM2Data()
    {
        metadata::Collection mdc("inbound/test.vm2");
        format = "vm2";
        max_selective_aggregation = "yearly";
        max_selective_aggregation_singleton_index = 2;
        test_data[0].set(mdc[0], "reftime:=1987-10-31; product:VM2,227");
        test_data[1].set(mdc[1], "reftime:=1987-10-31; product:VM2,228");
        test_data[2].set(mdc[2], "reftime:=2011-01-01; product:VM2,1");
        finalise_init();
    }
};

struct ODIMData : Fixture
{
    ODIMData()
    {
        metadata::Collection mdc;
        format = "odimh5";
        max_selective_aggregation = "yearly";
        max_selective_aggregation_singleton_index = 1;
        scan::scan("inbound/odimh5/COMP_CAPPI_v20.h5", mdc.inserter_func());
        scan::scan("inbound/odimh5/PVOL_v20.h5", mdc.inserter_func());
        scan::scan("inbound/odimh5/XSEC_v21.h5", mdc.inserter_func());
        test_data[0].set(mdc[0], "reftime:=2013-03-18");
        test_data[1].set(mdc[1], "reftime:=2000-01-02");
        test_data[2].set(mdc[2], "reftime:=2013-11-04");
        finalise_init();
    }
};

Metadata make_large_mock(const std::string& format, size_t size, unsigned month, unsigned day, unsigned hour=0);

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
        std::string name;
        std::string operation;
        std::string message;

        OperationMatch(const std::string& dsname, const std::string& operation, const std::string& message=std::string());
        std::string error_unmatched(const std::string& type) const;
    };

    struct SegmentMatch
    {
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
    std::vector<SegmentMatch> issue51;

    int count_repacked = -1;
    int count_archived = -1;
    int count_deleted = -1;
    int count_deindexed = -1;
    int count_rescanned = -1;
    int count_issue51 = -1;

    void clear();
};


struct MaintenanceResults
{
    /// 0: dataset is unclean, 1: dataset is clean, -1: don't check
    int is_clean;
    /// Number of files seen during maintenance (-1 == don't check)
    int files_seen;
    /// Number of files expected for each maintenance outcome (-1 == don't check)
    int by_type[tests::DatasetTest::COUNTED_MAX];

    void reset_by_type()
    {
        for (unsigned i = 0; i < tests::DatasetTest::COUNTED_MAX; ++i)
            by_type[i] = -1;
    }

    MaintenanceResults()
        : is_clean(-1), files_seen(-1)
    {
        reset_by_type();
    }

    MaintenanceResults(bool is_clean)
        : is_clean(is_clean), files_seen(-1)
    {
        reset_by_type();
    }

    MaintenanceResults(bool is_clean, unsigned files_seen)
        : is_clean(is_clean), files_seen(files_seen)
    {
        reset_by_type();
    }
};

template<typename DatasetChecker>
struct ActualChecker : public arki::utils::tests::Actual<DatasetChecker*>
{
    ActualChecker(DatasetChecker* s) : Actual<DatasetChecker*>(s) {}

    void repack(const ReporterExpected& expected, bool write=false);
    void repack_clean(bool write=false);
    void check(const ReporterExpected& expected, bool write=false, bool quick=true);
    void check_clean(bool write=false, bool quick=true);
    void check_issue51(const ReporterExpected& expected, bool write=false);
    void check_issue51_clean(bool write=false);
};

struct ActualSegmentedChecker : public ActualChecker<dataset::segmented::Checker>
{
    ActualSegmentedChecker(dataset::segmented::Checker* s) : ActualChecker<dataset::segmented::Checker>(s) {}

    /// Run maintenance and see that the results are as expected
    void maintenance(const MaintenanceResults& expected, bool quick=true);
    /// Check that a check reports all ok, and that there are data_count segments in the dataset
    void maintenance_clean(unsigned data_count, bool quick=true);
};

/// Corrupt a datafile by overwriting the first 4 bytes of its first data
/// element with zeros
void corrupt_datafile(const std::string& absname);

void test_append_transaction_ok(dataset::Segment* dw, Metadata& md, int append_amount_adjust=0);
void test_append_transaction_rollback(dataset::Segment* dw, Metadata& md);

inline arki::tests::ActualChecker<dataset::LocalChecker> actual(arki::dataset::LocalChecker* actual)
{
    return arki::tests::ActualChecker<dataset::LocalChecker>(actual);
}
inline arki::tests::ActualChecker<dataset::Checker> actual(arki::dataset::Checker* actual) { return arki::tests::ActualChecker<dataset::Checker>(actual); }
inline arki::tests::ActualChecker<dataset::Checker> actual(arki::dataset::Checker& actual) { return arki::tests::ActualChecker<dataset::Checker>(&actual); }
inline arki::tests::ActualSegmentedChecker actual(arki::dataset::segmented::Checker* actual) { return arki::tests::ActualSegmentedChecker(actual); }
inline arki::tests::ActualSegmentedChecker actual(arki::dataset::segmented::Checker& actual) { return arki::tests::ActualSegmentedChecker(&actual); }
inline arki::tests::ActualSegmentedChecker actual(arki::dataset::simple::Checker& actual) { return arki::tests::ActualSegmentedChecker((arki::dataset::segmented::Checker*)&actual); }
inline arki::tests::ActualSegmentedChecker actual(arki::dataset::ondisk2::Checker& actual) { return arki::tests::ActualSegmentedChecker((arki::dataset::segmented::Checker*)&actual); }
inline arki::tests::ActualSegmentedChecker actual(arki::dataset::iseg::Checker& actual) { return arki::tests::ActualSegmentedChecker((arki::dataset::segmented::Checker*)&actual); }
inline arki::tests::ActualChecker<dataset::Checker> actual(arki::dataset::ArchivesChecker& actual) { return arki::tests::ActualChecker<dataset::Checker>((dataset::Checker*)&actual); }

}
}

#endif
