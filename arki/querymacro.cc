/*
 * arki/querymacro - Macros implementing special query strategies
 *
 * Copyright (C) 2010--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "config.h"

#include <arki/querymacro.h>
#include <arki/configfile.h>
#include <arki/metadata.h>
#include <arki/metadata/consumer.h>
#include <arki/summary.h>
#include <arki/dataset/gridquery.h>
#include <arki/nag.h>
#include <arki/runtime/config.h>
#include <arki/runtime/io.h>
#include <arki/utils/dataset.h>
#include <arki/utils/lua.h>
#include <arki/sort.h>
#include <wibble/exception.h>
#include <wibble/string.h>

using namespace std;
using namespace wibble;

namespace arki {

static Querymacro* checkqmacro(lua_State *L, int idx = 1)
{
	void* ud = luaL_checkudata(L, idx, "arki.querymacro");
	luaL_argcheck(L, ud != NULL, idx, "`querymacro' expected");
	return *(Querymacro**)ud;
}

static int arkilua_dataset(lua_State *L)
{
	Querymacro* rd = checkqmacro(L);
	const char* name = luaL_checkstring(L, 2);
	ReadonlyDataset* ds = rd->dataset(name);
	if (ds) 
	{
		ds->lua_push(L);
		return 1;
	} else {
		lua_pushnil(L);
		return 1;
	}
}

static int arkilua_metadataconsumer(lua_State *L)
{
	Metadata* md = Metadata::lua_check(L, 1);
	luaL_argcheck(L, md != NULL, 1, "`arki.metadata' expected");

	int considx = lua_upvalueindex(1);
	metadata::Consumer* cons = (metadata::Consumer*)lua_touserdata(L, considx);

	lua_pushboolean(L, (*cons)(*md));
	return 1;
}

static int arkilua_tostring(lua_State *L)
{
	lua_pushstring(L, "querymacro");
	return 1;
}

static const struct luaL_Reg querymacrolib[] = {
	{ "dataset", arkilua_dataset },	                // qm:dataset(name) -> dataset
	{ "__tostring", arkilua_tostring },
	{NULL, NULL}
};

Querymacro::Querymacro(const ConfigFile& cfg, const std::string& name, const std::string& query)
	: cfg(cfg), L(new Lua), funcid_querydata(-1), funcid_querysummary(-1)
{
	Summary::lua_openlib(*L);
	Matcher::lua_openlib(*L);
	dataset::GridQuery::lua_openlib(*L);

    // Create the Querymacro object
    utils::lua::push_object_ptr(*L, this, "arki.querymacro", querymacrolib);

	// global qmacro = our userdata object
	lua_setglobal(*L, "qmacro");

	// Load the data as a global variable
	lua_pushstring(*L, query.c_str());
	lua_setglobal(*L, "query");

	// Set 'debug' and 'verbose' globals
	lua_pushboolean(*L, nag::is_verbose());
	lua_setglobal(*L, "verbose");
	lua_pushboolean(*L, nag::is_debug());
	lua_setglobal(*L, "debug");
	
	// Split macro name and arguments
	string macroname;
	size_t pos = name.find(" ");
	if (pos == string::npos)
	{
		macroname = name;
		lua_pushnil(*L);
	} else {
		macroname = name.substr(0, pos);
		string macroargs = str::trim(name.substr(pos + 1));
		lua_pushstring(*L, macroargs.c_str());
	}
	// Load the arguments as a global variable
	lua_setglobal(*L, "args");

	/// Load the right qmacro file
	string fname = runtime::Config::get().dir_qmacro.find_file(macroname + ".lua");
	if (luaL_dofile(*L, fname.c_str()))
	{
		// Copy the error, so that it will exist after the pop
		string error = lua_tostring(*L, -1);
		// Pop the error from the stack
		lua_pop(*L, 1);
		throw wibble::exception::Consistency("parsing " + fname, error);
	}

	/// Index the queryData function
	lua_getglobal(*L, "queryData");
	if (lua_isfunction(*L, -1))
		funcid_querydata = luaL_ref(*L, LUA_REGISTRYINDEX);

	/// Index the querySummary function
	lua_getglobal(*L, "querySummary");
	if (lua_isfunction(*L, -1))
		funcid_querysummary = luaL_ref(*L, LUA_REGISTRYINDEX);

	// utils::lua::dumpstack(*L, "Afterinit", cerr);
}

Querymacro::~Querymacro()
{
	if (L) delete L;
	for (std::map<std::string, ReadonlyDataset*>::iterator i = ds_cache.begin();
			i != ds_cache.end(); ++i)
		delete i->second;
}

ReadonlyDataset* Querymacro::dataset(const std::string& name)
{
	std::map<std::string, ReadonlyDataset*>::iterator i = ds_cache.find(name);
	if (i == ds_cache.end())
	{
		ConfigFile* dscfg = cfg.section(name);
		if (!dscfg) return 0;
		ReadonlyDataset* ds = ReadonlyDataset::create(*dscfg);
		pair<map<string, ReadonlyDataset*>::iterator, bool> res = ds_cache.insert(make_pair(name, ds));
		i = res.first;
	}
	return i->second;
}

void Querymacro::queryData(const dataset::DataQuery& q, metadata::Consumer& consumer)
{
  metadata::Consumer *c = &consumer;

	if (funcid_querydata == -1) return;

	// Retrieve the Lua function registered for this
	lua_rawgeti(*L, LUA_REGISTRYINDEX, funcid_querydata);

	// Pass DataQuery
	lua_newtable(*L);
	q.lua_push_table(*L, -1);

	// Push consumer C closure
    auto_ptr<utils::ds::DataInliner> inliner;
    auto_ptr<sort::Stream> sorter;
    if (q.withData)
    {
        inliner.reset(new utils::ds::DataInliner(*c));
        c = inliner.get();
    }
    if (q.sorter)
    {
        sorter.reset(new sort::Stream(*q.sorter, *c));
        c = sorter.get();
    }
	lua_pushlightuserdata(*L, c);
	lua_pushcclosure(*L, arkilua_metadataconsumer, 1);

	// Call the function
	if (lua_pcall(*L, 2, 0, 0))
	{
		string error = lua_tostring(*L, -1);
		lua_pop(*L, 1);
		throw wibble::exception::Consistency("running queryData function", error);
	}
}

void Querymacro::querySummary(const Matcher& matcher, Summary& summary)
{
	if (funcid_querysummary == -1) return;

	// Retrieve the Lua function registered for this
	lua_rawgeti(*L, LUA_REGISTRYINDEX, funcid_querysummary);

	// Pass matcher
	string m = matcher.toString();
	lua_pushstring(*L, m.c_str());

	// Pass summary
	summary.lua_push(*L);

	// Call the function
	if (lua_pcall(*L, 2, 0, 0))
	{
		string error = lua_tostring(*L, -1);
		lua_pop(*L, 1);
		throw wibble::exception::Consistency("running querySummary function", error);
	}
}

#if 0
Querymacro::Func Querymacro::get(const std::string& def)
{
	std::map<std::string, int>::iterator i = ref_cache.find(def);

	if (i == ref_cache.end())
	{
		size_t pos = def.find(':');
		if (pos == string::npos)
			throw wibble::exception::Consistency(
					"parsing targetfile definition \""+def+"\"",
					"definition not in the form type:parms");
		string type = def.substr(0, pos);
		string parms = def.substr(pos+1);

		// Get targetfile[type]
		lua_getglobal(*L, "targetfile");
		lua_pushlstring(*L, type.data(), type.size());
		lua_gettable(*L, -2);
		if (lua_type(*L, -1) == LUA_TNIL)
		{
			lua_pop(*L, 2);
			throw wibble::exception::Consistency(
					"parsing targetfile definition \""+def+"\"",
					"no targetfile found of type \""+type+"\"");
		}

		// Call targetfile[type](parms)
		lua_pushlstring(*L, parms.data(), parms.size());
		if (lua_pcall(*L, 1, 1, 0))
		{
			string error = lua_tostring(*L, -1);
			lua_pop(*L, 2);
			throw wibble::exception::Consistency(
					"creating targetfile function \""+def+"\"",
					error);
		}
		
		// Ref the created function into the registry
		int idx = luaL_ref(*L, LUA_REGISTRYINDEX);
		lua_pop(*L, 1);

		pair< std::map<std::string, int>::iterator, bool > res =
			ref_cache.insert(make_pair(def, idx));
		i = res.first;
	}

	// Return the functor wrapper to the function
	return Func(L, i->second);
}

std::string Querymacro::Func::operator()(const Metadata& md)
{
	// Get the function
	lua_rawgeti(*L, LUA_REGISTRYINDEX, idx);
	
	// Push the metadata
	md.lua_push(*L);

	// Call the function
	if (lua_pcall(*L, 1, 1, 0))
	{
		string error = lua_tostring(*L, -1);
		lua_pop(*L, 1);
		throw wibble::exception::Consistency("running targetfile function", error);
	}

	string res = lua_tostring(*L, -1);
	lua_pop(*L, 1);
	return res;
}
#endif

}
// vim:set ts=4 sw=4:
