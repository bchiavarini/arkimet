/*
 * scan/bufrlua - Use Lua macros for scanning contents of BUFR messages
 *
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/scan/bufrlua.h>
#include <arki/metadata.h>
#include <arki/runtime/config.h>
#include <wibble/string.h>
#include <wibble/sys/fs.h>

using namespace std;
using namespace wibble;
using namespace dballe;

namespace arki {
namespace scan {
namespace bufr {

BufrLua::BufrLua()
{
    // Load common library functions, if they exist
    string dirname = runtime::rcDirName("scan-bufr", "ARKI_SCAN_BUFR");
    string fname = str::joinpath(dirname, "common.lua");
    if (sys::fs::access(fname, F_OK))
    {
        if (luaL_dofile(L, fname.c_str()))
        {
            // Copy the error, so that it will exist after the pop
            string error = lua_tostring(L, -1);
            // Pop the error from the stack
            lua_pop(L, 1);
            throw wibble::exception::Consistency("parsing " + fname, error);
        }
    }
}

BufrLua::~BufrLua()
{
}

int BufrLua::get_scan_func(MsgType type)
{
	// First look up in cache
	std::map<MsgType, int>::iterator i = scan_funcs.find(type);
	if (i != scan_funcs.end()) return i->second;

	// Else try to load it
	string name = str::tolower(msg_type_name(type));

        // Load the right bufr scan file
        string dirname = runtime::rcDirName("scan-bufr", "ARKI_SCAN_BUFR");
        string fname = str::joinpath(dirname, name + ".lua");

	// If the fine does not exist, we are done
	if (!sys::fs::access(fname, F_OK))
		return scan_funcs[type] = -1;

	// Compile the macro
        if (luaL_dofile(L, fname.c_str()))
        {
                // Copy the error, so that it will exist after the pop
                string error = lua_tostring(L, -1);
                // Pop the error from the stack
                lua_pop(L, 1);
                throw wibble::exception::Consistency("parsing " + fname, error);
        }

	// Index the queryData function
	int id = -1;
        lua_getglobal(L, "scan");
        if (lua_isfunction(L, -1))
                id = luaL_ref(L, LUA_REGISTRYINDEX);

	// Save it in cache
	return scan_funcs[type] = id;
}

void BufrLua::scan(Msg& msg, Metadata& md)
{
	int funcid = get_scan_func(msg.type);

	// If we do not have a scan function for this message type, we are done
	if (funcid == -1) return;

        // Retrieve the Lua function registered for this
        lua_rawgeti(L, LUA_REGISTRYINDEX, funcid);

        // Pass msg
	msg.lua_push(L);

	// Pass md
	md.lua_push(L);

        // Call the function
        if (lua_pcall(L, 2, 0, 0))
        {
                string error = lua_tostring(L, -1);
                lua_pop(L, 1);
                throw wibble::exception::Consistency("running BUFR scan function", error);
        }
}

}
}
}
// vim:set ts=4 sw=4:
