/*
 * matcher/level - Level matcher
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

#include <arki/matcher/level.h>
#include <arki/matcher/utils.h>
#include <arki/metadata.h>

using namespace std;
using namespace wibble;

namespace arki {
namespace matcher {

std::string MatchLevel::name() const { return "level"; }

MatchLevelGRIB1::MatchLevelGRIB1(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	type = args.getInt(0, -1);
	l1 = args.getInt(1, -1);
	l2 = args.getInt(2, -1);
}

bool MatchLevelGRIB1::matchItem(const Item<>& o) const
{
	const types::level::GRIB1* v = dynamic_cast<const types::level::GRIB1*>(o.ptr());
	if (!v) return false;
	if (type != -1 && type != v->type) return false;
	int ol1 = -1, ol2 = -1;
	switch (v->valType())
	{
		case 0: break;
		case 1: ol1 = v->l1; break;
		case 2: ol1 = v->l1; ol2 = v->l2; break;
	}
	if (l1 != -1 && l1 != ol1) return false;
	if (l2 != -1 && l2 != ol2) return false;
	return true;
}

std::string MatchLevelGRIB1::toString() const
{
	CommaJoiner res;
	res.add("GRIB1");
	if (type != -1) res.add(type); else res.addUndef();
	if (l1 != -1) res.add(l1); else res.addUndef();
	if (l2 != -1) res.add(l2); else res.addUndef();
	return res.join();
}


MatchLevelGRIB2S::MatchLevelGRIB2S(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	type = args.getInt(0, -1);
	scale = args.getInt(1, -1);
	value = args.getInt(2, -1);
}

bool MatchLevelGRIB2S::matchItem(const Item<>& o) const
{
	const types::level::GRIB2S* v = dynamic_cast<const types::level::GRIB2S*>(o.ptr());
	if (!v) return false;
	if (type != -1 && type != v->type) return false;
	if (scale != -1 && scale != v->scale) return false;
	if (value >= 0 && (unsigned)value != v->value) return false;
	return true;
}

std::string MatchLevelGRIB2S::toString() const
{
	CommaJoiner res;
	res.add("GRIB2S");
	if (type != -1) res.add(type); else res.addUndef();
	if (scale != -1) res.add(scale); else res.addUndef();
	if (value != -1) res.add(value); else res.addUndef();
	return res.join();
}


MatchLevelGRIB2D::MatchLevelGRIB2D(const std::string& pattern)
{
	OptionalCommaList args(pattern);
	type1 = args.getInt(0, -1);
	scale1 = args.getInt(1, -1);
	value1 = args.getInt(2, -1);
	type2 = args.getInt(3, -1);
	scale2 = args.getInt(4, -1);
	value2 = args.getInt(5, -1);
}

bool MatchLevelGRIB2D::matchItem(const Item<>& o) const
{
	const types::level::GRIB2D* v = dynamic_cast<const types::level::GRIB2D*>(o.ptr());
	if (!v) return false;
	if (type1 != -1 && type1 != v->type1) return false;
	if (scale1 != -1 && scale1 != v->scale1) return false;
	if (value1 >= 0 && (unsigned)value1 != v->value1) return false;
	if (type2 != -1 && type2 != v->type2) return false;
	if (scale2 != -1 && scale2 != v->scale2) return false;
	if (value2 >= 0 && (unsigned)value2 != v->value2) return false;
	return true;
}

std::string MatchLevelGRIB2D::toString() const
{
	CommaJoiner res;
	res.add("GRIB2D");
	if (type1 != -1) res.add(type1); else res.addUndef();
	if (scale1 != -1) res.add(scale1); else res.addUndef();
	if (value1 != -1) res.add(value1); else res.addUndef();
	if (type2 != -1) res.add(type2); else res.addUndef();
	if (scale2 != -1) res.add(scale2); else res.addUndef();
	if (value2 != -1) res.add(value2); else res.addUndef();
	return res.join();
}



MatchLevel* MatchLevel::parse(const std::string& pattern)
{
	size_t beg = 0;
	size_t pos = pattern.find(',', beg);
	string name;
	string rest;
	if (pos == string::npos)
		name = str::trim(pattern.substr(beg));
	else {
		name = str::trim(pattern.substr(beg, pos-beg));
		rest = pattern.substr(pos+1);
	}
	switch (types::Level::parseStyle(name))
	{
		case types::Level::GRIB1: return new MatchLevelGRIB1(rest);
		case types::Level::GRIB2S: return new MatchLevelGRIB2S(rest);
		case types::Level::GRIB2D: return new MatchLevelGRIB2D(rest);
		default:
			throw wibble::exception::Consistency("parsing type of level to match", "unsupported level style: " + name);
	}
}

MatcherType level("level", types::TYPE_LEVEL, (MatcherType::subexpr_parser)MatchLevel::parse);

}
}

// vim:set ts=4 sw=4:
