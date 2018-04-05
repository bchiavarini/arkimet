#ifndef ARKI_DATASET_H
#define ARKI_DATASET_H

/// Base interface for arkimet datasets
#include <arki/matcher.h>
#include <arki/core/fwd.h>
#include <arki/dataset/fwd.h>
#include <arki/transaction.h>
#include <string>
#include <memory>

struct lua_State;

namespace arki {
class ConfigFile;
class Metadata;
class Summary;

namespace metadata {
class Collection;
}

namespace sort {
class Compare;
}

/**
 * Generic dataset interface.
 *
 * Archived data is stored in datasets.
 *
 * A dataset is a group of data with similar properties, like the output of a
 * given forecast model, or images from a given satellite.
 *
 * Every dataset has a dataset name.
 *
 * Every data inside a dataset has a data identifier (or data ID) that is
 * guaranteed to be unique inside the dataset.  One can obtain a unique
 * identifier on a piece of data across all datasets by simply prepending the
 * dataset name to the data identifier.
 *
 * Every dataset has a dataset index to speed up retrieval of subsets of
 * informations from it.
 *
 * The dataset index needs to be simple and fast, but does not need to support
 * a complex range of queries.
 *
 * The data is stored in the dataset without alteration from its original
 * form.
 *
 * It must be possible to completely regenerate the dataset index by
 * rescanning allthe data stored in the dataset.
 *
 * A consequence of the last two points is that a dataset can always be fully
 * regenerated by just reimporting data previously extracted from it.
 *
 * A dataset must be fully independent from other datasets, and not must not hold
 * any information about them.
 *
 * A dataset should maintain a summary of its contents, that lists the
 * various properties of the data it contains.
 *
 * Since the dataset groups data with similar properties, the summary is intended
 * to change rarely.
 *
 * An archive index can therefore be built, indexing all dataset summaries,
 * to allow complex data searches across datasets.
 */
namespace dataset {

struct DataQuery
{
    /// Matcher used to select data
    Matcher matcher;

    /**
     * Hint for the dataset backend to let them know that we also want the data
     * and not just the metadata.
     *
     * This is currently used:
     *  - by the HTTP client dataset, which will only download data from the
     *    server if this option is set
     *  - by local datasets to read-lock the segments for the duration of the
     *    query
     */
    bool with_data;

    /// Optional compare function to define a custom ordering of the result
    std::shared_ptr<sort::Compare> sorter;

    DataQuery();
    DataQuery(const std::string& matcher, bool with_data=false);
    DataQuery(const Matcher& matcher, bool with_data=false);
    ~DataQuery();

    void lua_from_table(lua_State* L, int idx);
    void lua_push_table(lua_State* L, int idx) const;
};

struct ByteQuery : public DataQuery
{
    enum Type {
        BQ_DATA = 0,
        BQ_POSTPROCESS = 1,
        BQ_REP_METADATA = 2,
        BQ_REP_SUMMARY = 3
    };

    std::string param;
    Type type = BQ_DATA;
    std::function<void(core::NamedFileDescriptor&)> data_start_hook = nullptr;

    ByteQuery() {}

    void setData(const Matcher& m)
    {
        with_data = true;
        type = BQ_DATA;
        matcher = m;
    }

    void setPostprocess(const Matcher& m, const std::string& procname)
    {
        with_data = true;
        type = BQ_POSTPROCESS;
        matcher = m;
        param = procname;
    }

    void setRepMetadata(const Matcher& m, const std::string& repname)
    {
        with_data = false;
        type = BQ_REP_METADATA;
        matcher = m;
        param = repname;
    }

    void setRepSummary(const Matcher& m, const std::string& repname)
    {
        with_data = false;
        type = BQ_REP_SUMMARY;
        matcher = m;
        param = repname;
    }
};

/// Base dataset configuration
struct Config : public std::enable_shared_from_this<Config>
{
    /// Dataset name
    std::string name;

    /// Raw configuration key-value pairs (normally extracted from ConfigFile)
    std::map<std::string, std::string> cfg;

    Config();
    Config(const std::string& name);
    Config(const ConfigFile& cfg);
    virtual ~Config() {}

    virtual std::unique_ptr<Reader> create_reader() const;
    virtual std::unique_ptr<Writer> create_writer() const;
    virtual std::unique_ptr<Checker> create_checker() const;

    static std::shared_ptr<const Config> create(const ConfigFile& cfg);
};

/**
 * Base class for all dataset Readers, Writers and Checkers.
 */
struct Base
{
protected:
    /**
     * Parent dataset.
     *
     * If nullptr, this dataset has no parent.
     */
    Base* m_parent = nullptr;

public:
    Base() {}
    Base(const Base&) = delete;
    Base(const Base&&) = delete;
    virtual ~Base() {}
    Base& operator=(const Base&) = delete;
    Base& operator=(Base&&) = delete;

    /// Return the dataset configuration
    virtual const Config& config() const = 0;

    /// Return the dataset configuration
    const std::map<std::string, std::string>& cfg() const { return config().cfg; }

    /// Return a name identifying the dataset type
    virtual std::string type() const = 0;

    /// Return the dataset name
    std::string name() const;

    /**
     * Set a parent dataset.
     *
     * Datasets can be nested to delegate management of part of the dataset
     * contents to a separate dataset. Hierarchy is tracked so that at least a
     * full dataset name can be computed in error messages.
     */
    void set_parent(Base& p);
};

class Reader : public dataset::Base
{
public:
    using Base::Base;

    /**
     * Query the dataset using the given matcher, and sending the results to
     * the given function.
     *
     * Returns true if dest always returned true, false if iteration stopped
     * because dest returned false.
     */
    virtual bool query_data(const dataset::DataQuery& q, metadata_dest_func dest) = 0;

    /**
     * Add to summary the summary of the data that would be extracted with the
     * given query.
     */
    virtual void query_summary(const Matcher& matcher, Summary& summary) = 0;

    /**
     * Query the dataset obtaining a byte stream, that gets written to a file
     * descriptor.
     *
     * The default implementation in Reader is based on queryData.
     */
    virtual void query_bytes(const dataset::ByteQuery& q, core::NamedFileDescriptor& out);

    /**
     * Expand the given begin and end ranges to include the datetime extremes
     * of this dataset.
     *
     * If begin and end are unset, set them to the datetime extremes of this
     * dataset.
     */
    virtual void expand_date_range(std::unique_ptr<core::Time>& begin, std::unique_ptr<core::Time>& end);

	// LUA functions
	/// Push to the LUA stack a userdata to access this dataset
	void lua_push(lua_State* L);

	/**
	 * Check that the element at \a idx is a Reader userdata
	 *
	 * @return the Reader element, or 0 if the check failed
	 */
	static Reader* lua_check(lua_State* L, int idx);

    /**
     * Instantiate an appropriate Reader for the given configuration
     */
    static std::unique_ptr<Reader> create(const ConfigFile& cfg);

	/**
	 * Read the configuration of the dataset(s) at the given path or URL,
	 * adding new sections to cfg
	 */
	static void readConfig(const std::string& path, ConfigFile& cfg);
};

struct WriterBatchElement
{
    /// Metadata to acquire
    Metadata& md;
    /// Name of the dataset where it has been acquired (empty when not
    /// acquired)
    std::string dataset_name;
    /// Acquire result
    WriterAcquireResult result = ACQ_ERROR;

    WriterBatchElement(Metadata& md) : md(md) {}
    WriterBatchElement(const WriterBatchElement& o) = default;
    WriterBatchElement(WriterBatchElement&& o) = default;
    WriterBatchElement& operator=(const WriterBatchElement& o) = default;
    WriterBatchElement& operator=(WriterBatchElement&& o) = default;
};

struct WriterBatch : public std::vector<std::shared_ptr<WriterBatchElement>>
{
    /**
     * Set all elements in the batch to ACQ_ERROR
     */
    void set_all_error(const std::string& note);
};

class Writer : public dataset::Base
{
public:
    enum ReplaceStrategy {
        /// Default strategy, as configured in the dataset
        REPLACE_DEFAULT,
        /// Never replace
        REPLACE_NEVER,
        /// Always replace
        REPLACE_ALWAYS,
        /**
         * Replace if update sequence number is higher (do not replace if USN
         * not available)
         */
        REPLACE_HIGHER_USN,
    };

protected:
	/**
	 * Insert the given metadata in the dataset.
	 *
	 * In case of conflict, replaces existing data.
	 *
	 * @return true if the data is successfully stored in the dataset, else
	 * false.  If false is returned, a note is added to the dataset explaining
	 * the reason of the failure.
	 */
	//virtual bool replace(Metadata& md) = 0;

public:
    using Base::Base;

	/**
	 * Acquire the given metadata item (and related data) in this dataset.
	 *
	 * After acquiring the data successfully, the data can be retrieved from
	 * the dataset.  Also, information such as the dataset name and the id of
	 * the data in the dataset are added to the Metadata object.
	 *
	 * @return The outcome of the operation.
	 */
	virtual WriterAcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) = 0;

    /**
     * Acquire the given metadata items (and related data) in this dataset.
     *
     * After acquiring the data successfully, the data can be retrieved from
     * the dataset.  Also, information such as the dataset name and the id of
     * the data in the dataset are added to the Metadata in the collection
     *
     * @return The outcome of the operation, as a vector with an WriterAcquireResult
     * for each metadata in the collection.
     */
    virtual void acquire_batch(WriterBatch& batch, ReplaceStrategy replace=REPLACE_DEFAULT) = 0;

	/**
	 * Remove the given metadata from the database.
	 */
	virtual void remove(Metadata& md) = 0;

	/**
	 * Flush pending changes to disk
	 */
	virtual void flush();

    /**
     * Obtain a write lock on the database, held until the Pending is committed
     * or rolled back.
     *
     * This is only used for testing.
     */
    virtual Pending test_writelock();

    /**
     * Instantiate an appropriate Writer for the given configuration
     */
    static std::unique_ptr<Writer> create(const ConfigFile& cfg);

    /**
     * Simulate acquiring the given metadata item (and related data) in this
     * dataset.
     *
     * No change of any kind happens to the dataset.
     */
    static void test_acquire(const ConfigFile& cfg, WriterBatch& batch, std::ostream& out);
};

struct CheckerConfig
{
    /// Reporter that gets notified of check progress and results
    std::shared_ptr<dataset::Reporter> reporter;
    /// If set, work only on segments that could contain data that matches this
    /// matcher
    Matcher segment_filter;
    /// Work on offline archives
    bool offline = true;
    /// Work on online data
    bool online = true;
    /// Simulate, do not write any changes
    bool readonly = true;
    /// Perform optional and time consuming operations
    bool accurate = false;

    CheckerConfig();
    CheckerConfig(std::shared_ptr<dataset::Reporter> reporter, bool readonly=true);
    CheckerConfig(const CheckerConfig&) = default;
    CheckerConfig(CheckerConfig&&) = default;
    CheckerConfig& operator=(const CheckerConfig&) = default;
    CheckerConfig& operator=(CheckerConfig&&) = default;
};

struct Checker : public dataset::Base
{
    using Base::Base;

    /**
     * Repack the dataset.
     *
     * test_flags are used to select alternate and usually undesirable repack
     * behaviours during tests, and should always be 0 outside tests.
     */
    virtual void repack(CheckerConfig& opts, unsigned test_flags=0) = 0;

    /// Check the dataset for errors
    virtual void check(CheckerConfig& opts) = 0;

    /// Remove data from the dataset that is older than `delete age`
    virtual void remove_old(CheckerConfig& opts) = 0;

    /// Remove all data from the dataset
    virtual void remove_all(CheckerConfig& opts) = 0;

    /// Convert directory segments into tar segments
    virtual void tar(CheckerConfig& opts) = 0;

    /// Send the dataset state to the reporter
    virtual void state(CheckerConfig& opts) = 0;

    /**
     * Check consistency of the last byte of GRIB and BUFR data in the archive,
     * optionally fixing it.
     *
     * See https://github.com/ARPAE-SIMC/arkimet/issues/51 for details.
     */
    virtual void check_issue51(CheckerConfig& opts);

    /**
     * Instantiate an appropriate Checker for the given configuration
     */
    static std::unique_ptr<Checker> create(const ConfigFile& cfg);
};

}
}
#endif
