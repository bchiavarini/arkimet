#ifndef ARKI_TYPES_AREA_H
#define ARKI_TYPES_AREA_H

/*
 * types/area - Geographical area
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

#include <arki/types.h>
#include <arki/values.h>
#include <arki/utils/geosfwd.h>

struct lua_State;

namespace arki {
namespace types {

/**
 * The vertical area or layer of some data
 *
 * It can contain information like areatype and area value.
 */
struct Area : public types::Type
{
	typedef unsigned char Style;

	mutable ARKI_GEOS_GEOMETRY* cached_bbox;

	/// Style values
	static const Style GRIB = 1;

	Area();

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);
	/// Area style
	virtual Style style() const = 0;

	virtual int compare(const Type& o) const;
	virtual int compare(const Area& o) const;

	virtual std::string tag() const;

	/// CODEC functions
	virtual types::Code serialisationCode() const;
	virtual size_t serialisationSizeLength() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	static Item<Area> decode(const unsigned char* buf, size_t len);
	static Item<Area> decodeString(const std::string& val);
	static types::Code typecode();

	/// Return the geographical bounding box
	const ARKI_GEOS_GEOMETRY* bbox() const;

	// LUA functions
	/// Push to the LUA stack a userdata to access this Area
	virtual void lua_push(lua_State* L) const;
	/// Callback used for the __index function of the Area LUA object
	static int lua_lookup(lua_State* L);
};

namespace area {

class GRIB : public Area
{
protected:
	ValueBag m_values;

public:
	virtual ~GRIB();

	const ValueBag& values() const { return m_values; }

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;

	virtual int compare(const Area& o) const;
	virtual int compare(const GRIB& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<GRIB> create(const ValueBag& values);
};

}

}
}

// vim:set ts=4 sw=4:
#endif
