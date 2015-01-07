#ifndef ARKI_FORMATTER_LUA_H
#define ARKI_FORMATTER_LUA_H

/*
 * arki/formatter/lua - Format arkimet values via a LUA script
 *
 * Copyright (C) 2008--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <string>
#include <arki/formatter.h>

namespace arki {
struct Lua;
}

namespace arki {
namespace formatter {

class Lua : public Formatter
{
	template<typename T>
	std::string invoke(const char* func, const char* type, const T& v) const;

protected:
	mutable arki::Lua *L;

public:
	Lua();
	virtual ~Lua();

    std::string operator()(const types::Type& v) const override;
};

}
}

// vim:set ts=4 sw=4:
#endif
