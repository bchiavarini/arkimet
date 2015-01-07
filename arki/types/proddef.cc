/*
 * types/proddef - Product definition
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/types/proddef.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include "config.h"
#include <sstream>
#include <cmath>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_PRODDEF
#define TAG "proddef"
#define SERSIZELEN 2

using namespace std;
using namespace arki::utils;
using namespace arki::utils::codec;

namespace arki {
namespace types {

const char* traits<Proddef>::type_tag = TAG;
const types::Code traits<Proddef>::type_code = CODE;
const size_t traits<Proddef>::type_sersize_bytes = SERSIZELEN;
const char* traits<Proddef>::type_lua_tag = LUATAG_TYPES ".proddef";

// Style constants
const unsigned char Proddef::GRIB;

Proddef::Style Proddef::parseStyle(const std::string& str)
{
	if (str == "GRIB") return GRIB;
	throw wibble::exception::Consistency("parsing Proddef style", "cannot parse Proddef style '"+str+"': only GRIB is supported");
}

std::string Proddef::formatStyle(Proddef::Style s)
{
	switch (s)
	{
		case Proddef::GRIB: return "GRIB";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

auto_ptr<Proddef> Proddef::decode(const unsigned char* buf, size_t len)
{
    using namespace utils::codec;
    ensureSize(len, 1, "Proddef");
    Style s = (Style)decodeUInt(buf, 1);
    switch (s)
    {
        case GRIB:
            return createGRIB(ValueBag::decode(buf+1, len-1));
        default:
            throw wibble::exception::Consistency("parsing Proddef", "style is " + formatStyle(s) + " but we can only decode GRIB");
    }
}

auto_ptr<Proddef> Proddef::decodeString(const std::string& val)
{
    string inner;
    Proddef::Style style = outerParse<Proddef>(val, inner);
    switch (style)
    {
        case Proddef::GRIB: return createGRIB(ValueBag::parse(inner));
        default:
            throw wibble::exception::Consistency("parsing Proddef", "unknown Proddef style " + formatStyle(style));
    }
}

auto_ptr<Proddef> Proddef::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    switch (style_from_mapping(val))
    {
        case Proddef::GRIB: return upcast<Proddef>(proddef::GRIB::decodeMapping(val));
        default:
            throw wibble::exception::Consistency("parsing Proddef", "unknown Proddef style " + val.get_string());
    }
}

#ifdef HAVE_LUA
static int arkilua_new_grib(lua_State* L)
{
	luaL_checktype(L, 1, LUA_TTABLE);
	ValueBag vals;
	vals.load_lua_table(L);
	proddef::GRIB::create(vals)->lua_push(L);
	return 1;
}

void Proddef::lua_loadlib(lua_State* L)
{
	static const struct luaL_Reg lib [] = {
		{ "grib", arkilua_new_grib },
		{ NULL, NULL }
	};
    utils::lua::add_global_library(L, "arki_proddef", lib);
}
#endif

auto_ptr<Proddef> Proddef::createGRIB(const ValueBag& values)
{
    return upcast<Proddef>(proddef::GRIB::create(values));
}

namespace proddef {

GRIB::~GRIB() { /* cache_grib.uncache(this); */ }

Proddef::Style GRIB::style() const { return Proddef::GRIB; }

void GRIB::encodeWithoutEnvelope(Encoder& enc) const
{
	Proddef::encodeWithoutEnvelope(enc);
	m_values.encode(enc);
}
std::ostream& GRIB::writeToOstream(std::ostream& o) const
{
    return o << formatStyle(style()) << "(" << m_values.toString() << ")";
}
void GRIB::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Proddef::serialiseLocal(e, f);
    e.add("va");
    m_values.serialise(e);
}
auto_ptr<GRIB> GRIB::decodeMapping(const emitter::memory::Mapping& val)
{
    return GRIB::create(ValueBag::parse(val["va"].get_mapping()));
}
std::string GRIB::exactQuery() const
{
    return "GRIB:" + m_values.toString();
}
const char* GRIB::lua_type_name() const { return "arki.types.proddef.grib"; }

#ifdef HAVE_LUA
bool GRIB::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "val")
		values().lua_push(L);
	else
		return Proddef::lua_lookup(L, name);
	return true;
}
#endif

int GRIB::compare_local(const Proddef& o) const
{
	// We should be the same kind, so upcast
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB Proddef, but is a ") + typeid(&o).name() + " instead");

	return m_values.compare(v->m_values);
}

bool GRIB::equals(const Type& o) const
{
	const GRIB* v = dynamic_cast<const GRIB*>(&o);
	if (!v) return false;
	return m_values == v->m_values;
}

GRIB* GRIB::clone() const
{
    GRIB* res = new GRIB;
    res->m_values = m_values;
    return res;
}

auto_ptr<GRIB> GRIB::create(const ValueBag& values)
{
    GRIB* res = new GRIB;
    res->m_values = values;
    return auto_ptr<GRIB>(res);
}

}

void Proddef::init()
{
    MetadataType::register_type<Proddef>();
}

}
}
#include <arki/types.tcc>
