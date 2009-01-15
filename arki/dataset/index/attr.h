#ifndef ARKI_DATASET_INDEX_ATTR_H
#define ARKI_DATASET_INDEX_ATTR_H

/*
 * dataset/index/attr - Generic index for metadata items
 *
 * Copyright (C) 2007,2008  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <wibble/exception.h>
#include <arki/dataset/index/sqlite.h>
#include <arki/dataset/index/base.h>
#include <arki/matcher.h>
#include <string>
#include <vector>
#include <map>

namespace arki {
namespace dataset {
namespace index {

// Precompiled query to get the ID from an encoded type
class GetAttrID : public Query
{
	std::string m_table;

public:
	GetAttrID(SQLiteDB& db, const std::string& table) : Query("get-blob-id", db), m_table(table) {}

	void initQueries();

	// Run the query, returning the id or -1 if not found
	int operator()(const std::string& blob) const;
};


class AttrSubIndex
{
public:
	// Name of the metadata we index
	std::string name;

protected:
	// This is just a copy of what is in the main index
	SQLiteDB& m_db;
	// Precompiled insert statement
	sqlite3_stmt* m_stm_insert;
	// Precompiled select all statement
	sqlite3_stmt* m_stm_select_all;
	// Precompiled query to get the ID given a blob
	GetAttrID m_get_blob_id;
	// Cache of known IDs
	std::map<std::string, int> m_id_cache;
	// Serialisation code of the item type that we index
	types::Code serCode;

public:
	AttrSubIndex(SQLiteDB& db, types::Code serCode);
	~AttrSubIndex();

	void initDB();

	void initQueries();

	std::vector<int> query(const matcher::OR& m) const;

	int insert(const Metadata& md);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
