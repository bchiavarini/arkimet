#ifndef ARKI_TYPES_RUN_H
#define ARKI_TYPES_RUN_H

/*
 * types/run - Daily run identification for a periodic data source
 *
 * Copyright (C) 2008--2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

struct lua_State;

namespace arki {
namespace types {

struct Run;

template<>
struct traits<Run>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;

	typedef unsigned char Style;
};

/**
 * The run of some data.
 *
 * It can contain information like centre, process, subcentre, subprocess and
 * other similar data.
 */
struct Run : public types::StyledType<Run>
{
	/// Style values
	//static const Style NONE = 0;
	static const Style MINUTE = 1;

	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

	/// CODEC functions
	static Item<Run> decode(const unsigned char* buf, size_t len);
	static Item<Run> decodeString(const std::string& val);

	static void lua_loadlib(lua_State* L);
};

namespace run {

class Minute : public Run
{
protected:
	unsigned int m_minute;

public:
	unsigned minute() const { return m_minute; }

	virtual Style style() const;
	virtual void encodeWithoutEnvelope(utils::codec::Encoder& enc) const;
	virtual std::ostream& writeToOstream(std::ostream& o) const;
	virtual std::string exactQuery() const;
	virtual const char* lua_type_name() const;
	virtual bool lua_lookup(lua_State* L, const std::string& name) const;

	virtual int compare_local(const Run& o) const;
	virtual bool operator==(const Type& o) const;

	static Item<Minute> create(unsigned int hour, unsigned int minute = 0);
};

}

}
}

#undef ARKI_GEOS_GEOMETRY

// vim:set ts=4 sw=4:
#endif
