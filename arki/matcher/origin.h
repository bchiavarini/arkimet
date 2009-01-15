#ifndef ARKI_MATCHER_ORIGIN
#define ARKI_MATCHER_ORIGIN

/*
 * matcher/origin - Origin matcher
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

#include <arki/matcher.h>
#include <arki/types/origin.h>

namespace arki {
namespace matcher {

/**
 * Match Origins
 */
struct MatchOrigin : public Implementation
{
	//MatchType type() const { return MATCH_ORIGIN; }
	std::string name() const;

	static MatchOrigin* parse(const std::string& pattern);
};

struct MatchOriginGRIB1 : public MatchOrigin
{
	// These are -1 when they should be ignored in the match
	int centre;
	int subcentre;
	int process;

	MatchOriginGRIB1(const std::string& pattern);
	bool matchItem(const Item<>& o) const;
	std::string toString() const;
};

struct MatchOriginGRIB2 : public MatchOrigin
{
	// These are -1 when they should be ignored in the match
	int centre;
	int subcentre;
	int processtype;
	int bgprocessid;
	int processid;

	MatchOriginGRIB2(const std::string& pattern);
	bool matchItem(const Item<>& o) const;
	std::string toString() const;
};

struct MatchOriginBUFR : public MatchOrigin
{
	// These are -1 when they should be ignored in the match
	int centre;
	int subcentre;

	MatchOriginBUFR(const std::string& pattern);
	bool matchItem(const Item<>& o) const;
	std::string toString() const;
};

}
}

// vim:set ts=4 sw=4:
#endif
