#ifndef ARKI_TYPES_UTILS_H
#define ARKI_TYPES_UTILS_H

/*
 * types/utils - arkimet metadata type system implementation utilities
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
#include <wibble/exception.h>
#include <string>
#include <set>
#include <cstring>

#include "config.h"
#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define LUATAG_TYPES "arki.types"

struct lua_State;

namespace arki {
namespace types {

template<typename T>
class TypeCache
{
protected:
	// TODO: use unordered_set when it becomes available
	std::set< Item<T> > m_cache;
	size_t m_reused;

public:
	TypeCache() : m_reused(0) {}

	size_t size() const { return m_cache.size(); }
	size_t reused() const { return m_reused; }

	/**
	 * If \a item exists in the cache, return the cached version.
	 * Else, enter \a item in the cache and return it
	 */
	Item<T> intern(Item<T> item)
	{
		typename std::set< Item<T> >::const_iterator i = m_cache.find(item);
		if (i != m_cache.end())
		{
			++m_reused;
			return *i;
		}
		m_cache.insert(item);
		return item;
	}

	void uncache(Item<T> item)
	{
		m_cache.erase(item);
	}
};

/**
 * This class is used to register types with the arkimet metadata type system.
 *
 * Registration is done by declaring a static RegisterItem object, passing the
 * metadata details in the constructor.
 */
struct MetadataType
{
	typedef Item<Type> (*item_decoder)(const unsigned char* start, size_t len);
	typedef Item<Type> (*string_decoder)(const std::string& val);
	typedef void (*lua_libloader)(lua_State* L);
	typedef void (*intern_stats)();

	types::Code serialisationCode;
	int serialisationSizeLen;
	std::string tag;
	item_decoder decode_func;
	string_decoder string_decode_func;
	lua_libloader lua_loadlib_func;
	intern_stats intern_stats_func;
	
	MetadataType(
		types::Code serialisationCode,
		int serialisationSizeLen,
		const std::string& tag,
		item_decoder decode_func,
		string_decoder string_decode_func,
		lua_libloader lua_loadlib_func,
		intern_stats intern_stats_func = 0
		);
	~MetadataType();

	// Get information about the given metadata
	static const MetadataType* get(types::Code);

	template<typename T>
	static MetadataType create(intern_stats intern_stats_func = 0)
	{
		return MetadataType(
			traits<T>::type_code,
			traits<T>::type_sersize_bytes,
			traits<T>::type_tag,
			(MetadataType::item_decoder)T::decode,
			(MetadataType::string_decoder)T::decodeString,
			T::lua_loadlib,
			intern_stats_func
		);
	}

	static void lua_loadlib(lua_State* L);
};

void debug_intern_stats();

// Parse the outer style of a TYPE(val1, val2...) string
template<typename T>
static typename T::Style outerParse(const std::string& str, std::string& inner)
{
	using namespace std;

	if (str.empty())
		throw wibble::exception::Consistency(string("parsing ") + typeid(T).name(), "string is empty");
	size_t pos = str.find('(');
	if (pos == string::npos)
		throw wibble::exception::Consistency(string("parsing ") + typeid(T).name(), "no open parentesis found in \""+str+"\"");
	if (str[str.size() - 1] != ')')
		throw wibble::exception::Consistency(string("parsing ") + typeid(T).name(), "string \""+str+"\" does not end with closed parentesis");
	inner = str.substr(pos+1, str.size() - pos - 2);
	return T::parseStyle(str.substr(0, pos));
}

// Parse a list of numbers of the given size
template<int SIZE>
struct NumberList
{
	int vals[SIZE];
	std::string tail;

	unsigned size() const { return SIZE; }

	NumberList(const std::string& str, const std::string& what, bool has_tail=false)
	{
		using namespace std;
		using namespace wibble::str;

		const char* start = str.c_str();
		for (unsigned i = 0; i < SIZE; ++i)
		{
			// Skip colons and spaces, if any
			while (*start && (::isspace(*start) || *start == ','))
				++start;
			if (!*start)
				throw wibble::exception::Consistency(string("parsing ") + what,
					"found " + fmt(i) + " values instead of " + fmt(SIZE));

			// Parse the number
			char* endptr;
			vals[i] = strtol(start, &endptr, 10);
			if (endptr == start)
				throw wibble::exception::Consistency(string("parsing ") + what,
					"expected a number, but found \"" + str.substr(start - str.c_str()) + "\"");

			start = endptr;
		}
		if (!has_tail && *start)
			throw wibble::exception::Consistency(string("parsing ") + what,
				"found trailing characters at the end: \"" + str.substr(start - str.c_str()) + "\"");
		else if (has_tail && *start)
		{
			// Skip colons and spaces, if any
			while (*start && (::isspace(*start) || *start == ','))
				++start;
			tail = start;
		}
	}
};

// Parse a list of floats of the given size
template<int SIZE>
struct FloatList
{
	float vals[SIZE];

	unsigned size() const { return SIZE; }

	FloatList(const std::string& str, const std::string& what)
	{
		using namespace std;
		using namespace wibble::str;

		const char* start = str.c_str();
		for (unsigned i = 0; i < SIZE; ++i)
		{
			// Skip colons and spaces, if any
			while (*start && (::isspace(*start) || *start == ','))
				++start;
			if (!*start)
				throw wibble::exception::Consistency(string("parsing ") + what,
					"found " + fmt(i) + " values instead of " + fmt(SIZE));

			// Parse the number
			char* endptr;
			vals[i] = strtof(start, &endptr);
			if (endptr == start)
				throw wibble::exception::Consistency(string("parsing ") + what,
					"expected a number, but found \"" + str.substr(start - str.c_str()) + "\"");

			start = endptr;
		}
		if (*start)
			throw wibble::exception::Consistency(string("parsing ") + what,
				"found trailing characters at the end: \"" + str.substr(start - str.c_str()) + "\"");
	}
};


// Parse a list of floats of the given size
template<int SIZE> struct DoubleList
{
	double vals[SIZE];

	unsigned size() const { return SIZE; }

	DoubleList(const std::string& str, const std::string& what)
	{
		using namespace std;
		using namespace wibble::str;

		const char* start = str.c_str();
		for (unsigned i = 0; i < SIZE; ++i)
		{
			// Skip colons and spaces, if any
			while (*start && (::isspace(*start) || *start == ','))
				++start;
			if (!*start)
				throw wibble::exception::Consistency(string("parsing ") + what,
					"found " + fmt(i) + " values instead of " + fmt(SIZE));

			// Parse the number
			char* endptr;
			vals[i] = strtof(start, &endptr);
			if (endptr == start)
				throw wibble::exception::Consistency(string("parsing ") + what,
					"expected a number, but found \"" + str.substr(start - str.c_str()) + "\"");

			start = endptr;
		}
		if (*start)
			throw wibble::exception::Consistency(string("parsing ") + what,
				"found trailing characters at the end: \"" + str.substr(start - str.c_str()) + "\"");
	}
};

/* functions to split strings */
void split(const std::string& str, std::set<std::string>& result, const std::string& delimiters = ",");
void split(const std::string& str, std::vector<std::string>& result, const std::string& delimiters = ",");


}
}

// vim:set ts=4 sw=4:
#endif
