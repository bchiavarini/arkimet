#ifndef ARKI_DATASET_H
#define ARKI_DATASET_H

/*
 * dataset - Handle arkimet datasets
 *
 * Copyright (C) 2007--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Author: Enrico Zini <enrico@enricozini.com>
 */

#include <arki/matcher.h>
#include <arki/transaction.h>
#include <string>
#include <memory>

struct lua_State;

namespace arki {

class ConfigFile;
class Metadata;
class Summary;

namespace metadata {
class Consumer;
class Hook;
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
	Matcher matcher;
	bool withData;
	refcounted::Pointer<sort::Compare> sorter;

	DataQuery();
	DataQuery(const Matcher& matcher, bool withData=false);
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
	Type type;
	metadata::Hook* data_start_hook;

	ByteQuery() : type(BQ_DATA), data_start_hook(0) {}

	void setData(const Matcher& m)
	{
		type = BQ_DATA;
		matcher = m;
		withData = true;
	}

	void setPostprocess(const Matcher& m, const std::string& procname)
	{
		type = BQ_POSTPROCESS;
		matcher = m;
		withData = true;
		param = procname;
	}

	void setRepMetadata(const Matcher& m, const std::string& repname)
	{
		type = BQ_REP_METADATA;
		matcher = m;
		withData = false;
		param = repname;
	}

	void setRepSummary(const Matcher& m, const std::string& repname)
	{
		type = BQ_REP_SUMMARY;
		matcher = m;
		withData = false;
		param = repname;
	}
};

}

class ReadonlyDataset
{
public:
	// Configuration items (normally extracted from ConfigFile)
	std::map<std::string, std::string> cfg;

	virtual ~ReadonlyDataset() {}

	/**
	 * Query the dataset using the given matcher, and sending the results to
	 * the metadata consumer.
	 */
	virtual void queryData(const dataset::DataQuery& q, metadata::Consumer& consumer) = 0;

	/**
	 * Add to summary the summary of the data that would be extracted with the
	 * given query.
	 */
	virtual void querySummary(const Matcher& matcher, Summary& summary) = 0;

	/**
	 * Query the dataset obtaining a byte stream.
	 *
	 * The default implementation in ReadonlyDataset is based on queryData.
	 */
	virtual void queryBytes(const dataset::ByteQuery& q, std::ostream& out);

	// LUA functions
	/// Push to the LUA stack a userdata to access this dataset
	void lua_push(lua_State* L);

	/**
	 * Check that the element at \a idx is a ReadonlyDataset userdata
	 *
	 * @return the ReadonlyDataset element, or 0 if the check failed
	 */
	static ReadonlyDataset* lua_check(lua_State* L, int idx);

	/**
	 * Instantiate an appropriate Dataset for the given configuration
	 */
	static ReadonlyDataset* create(const ConfigFile& cfg);

	/**
	 * Read the configuration of the dataset(s) at the given path or URL,
	 * adding new sections to cfg
	 */
	static void readConfig(const std::string& path, ConfigFile& cfg);
};

class WritableDataset
{
public:
	/// Possible outcomes of acquire
	enum AcquireResult {
		/// Acquire successful
		ACQ_OK,
		/// Acquire failed because the data is already in the database
		ACQ_ERROR_DUPLICATE,
		/// Acquire failed for other reasons than duplicates
		ACQ_ERROR
	};

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
    std::string m_name;

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
    WritableDataset();
    virtual ~WritableDataset();

	// Return the dataset name
	const std::string& name() const { return m_name; }

	/**
	 * Acquire the given metadata item (and related data) in this dataset.
	 *
	 * After acquiring the data successfully, the data can be retrieved from
	 * the dataset.  Also, information such as the dataset name and the id of
	 * the data in the dataset are added to the Metadata object.
	 *
	 * @return The outcome of the operation.
	 */
	virtual AcquireResult acquire(Metadata& md, ReplaceStrategy replace=REPLACE_DEFAULT) = 0;

	/**
	 * Remove the given metadata from the database.
	 */
	virtual void remove(Metadata& md) = 0;

	/**
	 * Reset this dataset, removing all data, indices and caches
	 */
	virtual void removeAll(std::ostream& log, bool writable=false) = 0;

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
	 * Instantiate an appropriate Dataset for the given configuration
	 */
	static WritableDataset* create(const ConfigFile& cfg);

	/**
	 * Simulate acquiring the given metadata item (and related data) in this
	 * dataset.
	 *
	 * No change of any kind happens to the dataset.  Information such as the
	 * dataset name and the id of the data in the dataset are added to the
	 * Metadata object.
	 *
	 * @return The outcome of the operation.
	 */
	static AcquireResult testAcquire(const ConfigFile& cfg, const Metadata& md, std::ostream& out);
};

}

// vim:set ts=4 sw=4:
#endif
