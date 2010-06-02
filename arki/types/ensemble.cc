/*
 * types/ensemble - Geographical ensemble
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

#include <wibble/exception.h>
#include <wibble/string.h>
#include <arki/types/ensemble.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include "config.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_ENSEMBLE
#define TAG "ensemble"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;

namespace arki {
namespace types {

const char* traits<Ensemble>::type_tag = TAG;
const types::Code traits<Ensemble>::type_code = CODE;
const size_t traits<Ensemble>::type_sersize_bytes = SERSIZELEN;
const char* traits<Ensemble>::type_lua_tag = LUATAG_TYPES ".ensemble";

// Style constants
const unsigned char Ensemble::GRIB;

Ensemble::Style Ensemble::parseStyle(const std::string& str)
{
	if (str == "GRIB") return GRIB;
	throw wibble::exception::Consistency("parsing Ensemble style", "cannot parse Ensemble style '"+str+"': only GRIB is supported");
}

std::string Ensemble::formatStyle(Ensemble::Style s)
{
	switch (s)
	{
		case Ensemble::GRIB: return "GRIB";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

Item<Ensemble> Ensemble::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	ensureSize(len, 1, "Ensemble");
	Style s = (Style)decodeUInt(buf, 1);
	switch (s)
	{
		case GRIB:
			return ensemble::GRIB::create(ValueBag::decode(buf+1, len-1));
		default:
			throw wibble::exception::Consistency("parsing Ensemble", "style is " + formatStyle(s) + " but we can only decode GRIB");
	}
}

Item<Ensemble> Ensemble::decodeString(const std::string& val)
{
	string inner;
	Ensemble::Style style = outerParse<Ensemble>(val, inner);
	switch (style)
	{
		case Ensemble::GRIB: return ensemble::GRIB::create(ValueBag::parse(inner)); 
		default:
			throw wibble::exception::Consistency("parsing Ensemble", "unknown Ensemble style " + formatStyle(style));
	}
}

namespace ensemble {

static TypeCache<GRIB> cache_grib;

GRIB::~GRIB() { /* cache_grib.uncache(this); */ }

Ensemble::Style GRIB::style() const { return Ensemble::GRIB; }

void GRIB::encodeWithoutEnvelope(Encoder& enc) const
{
	Ensemble::encodeWithoutEnvelope(enc);
	m_values.encode(enc);
}
std::ostream& GRIB::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "(" << m_values.toString() << ")";
}
std::string GRIB::exactQuery() const
{
    return "GRIB:" + m_values.toString();
}
const char* GRIB::lua_type_name() const { return "arki.types.ensemble.grib"; }

#ifdef HAVE_LUA
bool GRIB::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "val")
		values().lua_push(L);
	else
		return Ensemble::lua_lookup(L, name);
	return true;
}
#endif

int GRIB::compare_local(const Ensemble& o) const
{
	// We should be the same kind, so upcast
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB Ensemble, but is a ") + typeid(&o).name() + " instead");

	return m_values.compare(v->m_values);
}

bool GRIB::operator==(const Type& o) const
{
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v) return false;
	return m_values == v->m_values;
}

Item<GRIB> GRIB::create(const ValueBag& values)
{
	GRIB* res = new GRIB;
	res->m_values = values;
	return cache_grib.intern(res);
}

static void debug_interns()
{
	fprintf(stderr, "Ensemble GRIB: sz %zd reused %zd\n", cache_grib.size(), cache_grib.reused());
}

}

static MetadataType ensembleType = MetadataType::create<Ensemble>(ensemble::debug_interns);

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:


#if 0
	-- Move to the matcher
	virtual bool match(const ValueBag& wanted) const
	{
		// Both a and b are sorted, so we can iterate them linearly together

		values_t::const_iterator a = values.begin();
		ValueBag::const_iterator b = wanted.begin();

		while (a != values.end())
		{
			// Nothing else wanted anymore
			if (b == wanted.end())
				return true;
			if (a->first < b->first)
				// This value is not in the match expression
				++a;
			else if (b->first < a->first)
				// This value is wanted but we don't have it
				return false;
			else if (*a->second != *b->second)
				// Same key, check if the value is the same
				return false;
			else
			{
				// If also the value is the same, move on to the next item
				++a;
				++b;
			}
		}
		// We got to the end of a.  If there are still things in b, we don't
		// match.  If we are also to the end of b, then we matched everything
		return b == wanted.end();
	}
#endif
