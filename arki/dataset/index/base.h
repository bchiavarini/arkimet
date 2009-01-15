#ifndef ARKI_DATASET_INDEX_BASE_H
#define ARKI_DATASET_INDEX_BASE_H

/*
 * dataset/index - Dataset index infrastructure
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
#include <arki/metadata.h>
#include <arki/types/origin.h>
#include <arki/types/product.h>
#include <arki/types/level.h>
#include <string>
#include <set>
#include <sstream>

// Enable compatibility with old sqlite.
// TODO: use #if macros to test sqlite version instead
#define LEGACY_SQLITE

namespace arki {
class Metadata;
class Matcher;

namespace dataset {
namespace index {

// Pass through metadata to a consumer if it matches a Matcher expression
struct FilteredMetadataConsumer : public MetadataConsumer
{
	const Matcher& matcher;
	MetadataConsumer& chained;

	FilteredMetadataConsumer(const Matcher& matcher, MetadataConsumer& chained)
		: matcher(matcher), chained(chained) {}
	~FilteredMetadataConsumer() {}

	bool operator()(Metadata& m);
};

/**
 * Convert a [comma and optional spaces]-separated string with metadata
 * component names into a MetadataComponents bitfield
 */
std::set<types::Code> parseMetadataBitmask(const std::string& components);

/**
 * Configurable creator of unique database IDs from metadata
 */
struct IDMaker
{
	std::set<types::Code> m_id_components;
	
	IDMaker() {}
	IDMaker(const std::string& components) { init(components); }

	void init(const std::string& components);

	std::string id(const Metadata& md) const;
};


// Exception we can throw if an element was not found
struct NotFound {};

/**
 * Format a vector of ints to a SQL match expression.
 *
 * If the vector has only one item, it becomes =item.
 *
 * If the vector has more than one item, it becomes IN(val1,val2...)
 */
std::string fmtin(const std::vector<int>& vals);

}
}
}

// vim:set ts=4 sw=4:
#endif
