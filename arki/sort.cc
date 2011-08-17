/*
 * arki/sort - Sorting routines for metadata
 *
 * Copyright (C) 2007--2011  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/sort.h>
#include <arki/metadata.h>
#include <arki/types/reftime.h>
#include <arki/metadata.h>

#include <wibble/exception.h>
#include <wibble/string.h>
#include <wibble/regexp.h>
#include <wibble/grcal/grcal.h>

#include <vector>
#include <cctype>
#include <cstring>

using namespace std;
using namespace wibble;

namespace arki {
namespace sort {

/// Comparison function for one metadata item
struct Item
{
	types::Code code;
	bool reverse;

	Item(const std::string& expr)
	{
		if (expr.empty())
			throw wibble::exception::Consistency("parsing sort expression", "metadata name is the empty string");
		size_t start = 0;
		switch (expr[0])
		{
			case '+': reverse = false; start = 1; break;
			case '-': reverse = true; start = 1; break;
			default: reverse = false; start = 0; break;
		}
		while (start < expr.size() && isspace(expr[start]))
			++start;
		code = types::parseCodeName(expr.substr(start));
	}
	Item(types::Code code, bool reverse) : code(code), reverse(reverse) {}

	int compare(const Metadata& a, const Metadata& b) const
	{
		int res = a.get(code).compare(b.get(code));
		return reverse ? -res : res;
	}
};

/// Serializer for Item
ostream& operator<<(ostream& out, const Item& i)
{
	if (i.reverse) out << "-";
	out << str::tolower(types::formatCode(i.code));
	return out;
}

/**
 * Compare metadata according to sequence of sort items
 */
class Items : public std::vector<Item>, public Compare
{
public:
	Items(const std::string& expr)
	{
		Splitter splitter("[ \t]*,[ \t]*", REG_EXTENDED);
		for (Splitter::const_iterator i = splitter.begin(expr);
				i != splitter.end(); ++i)
			push_back(Item(*i));
		if (empty())
			push_back(Item("reftime"));
	}
	virtual ~Items() {}

	virtual int compare(const Metadata& a, const Metadata& b) const
	{
		for (const_iterator i = begin(); i != end(); ++i)
		{
			int cmp = i->compare(a, b);
			if (cmp != 0) return cmp;
		}
		return 0;
	}

	virtual std::string toString() const
	{
		return str::join(begin(), end(), ",");
	}
};

struct IntervalCompare : public Items
{
	Interval m_interval;

	IntervalCompare(Interval interval, const std::string& expr)
		: Items(expr), m_interval(interval) {}

	virtual Interval interval() const { return m_interval; }

	virtual int compare(const Metadata& a, const Metadata& b) const
	{
		return Items::compare(a, b);
	}

	virtual std::string toString() const
	{
		switch (m_interval)
		{
			case NONE: return Items::toString();
			case MINUTE: return "minute:"+Items::toString();
			case HOUR: return "hour:"+Items::toString();
			case DAY: return "day:"+Items::toString();
			case MONTH: return "month:"+Items::toString();
			case YEAR: return "year:"+Items::toString();
			default:
				   throw wibble::exception::Consistency("formatting sort expression", "interval code " + str::fmt((int)m_interval) + " is not valid");
		}
		return Items::toString();
	}
};

static Compare::Interval parseInterval(const std::string& name)
{
	using namespace str;

	// TODO: convert into something faster, like a hash lookup or a gperf lookup
	string nname = trim(tolower(name));
	if (nname == "minute") return Compare::MINUTE;
	if (nname == "hour") return Compare::HOUR;
	if (nname == "day") return Compare::DAY;
	if (nname == "month") return Compare::MONTH;
	if (nname == "year") return Compare::YEAR;
	throw wibble::exception::Consistency("parsing interval name", "unsupported interval: " + name + ".  Valid intervals are minute, hour, day, month and year");
}

refcounted::Pointer<Compare> Compare::parse(const std::string& expr)
{
	size_t pos = expr.find(':');
	if (pos == string::npos)
	{
//cerr << "creating intervalless: " << expr << endl;		
		return refcounted::Pointer<Compare>(new sort::Items(expr));
	} else {
//cerr << "creating with interval " << expr.substr(0, pos) << ": " << expr.substr(pos+1) << endl;
		return refcounted::Pointer<Compare>(new sort::IntervalCompare(sort::parseInterval(expr.substr(0, pos)), expr.substr(pos+1)));
	}
}

void Stream::setEndOfPeriod(const UItem<types::Reftime>& rt)
{
	UItem<types::Time> t;
	switch (rt->style())
	{
		case types::Reftime::POSITION: t = rt.upcast<types::reftime::Position>()->time; break;
		case types::Reftime::PERIOD: t = rt.upcast<types::reftime::Period>()->end; break;
		default:
			throw wibble::exception::Consistency("setting end of period", "reference time has invalid style: " + types::Reftime::formatStyle(rt->style()));
	}
	int vals[6];
	memcpy(vals, t->vals, 6 * sizeof(int));
	switch (sorter.interval())
	{
		case Compare::YEAR: vals[1] = -1;
		case Compare::MONTH: vals[2] = -1;
		case Compare::DAY: vals[3] = -1;
		case Compare::HOUR: vals[4] = -1;
		case Compare::MINUTE: vals[5] = -1; break;
		default:
			throw wibble::exception::Consistency("setting end of period", "interval type has invalid value: " + str::fmt((int)sorter.interval()));
	}
	wibble::grcal::date::upperbound(vals);
	endofperiod = types::reftime::Position::create(types::Time::create(vals));
//cerr << "Set end of period to " << endofperiod << endl;
}

bool Stream::operator()(Metadata& m)
{
//cerr << "stream with " << sorter.toString() << endl;
//cerr << "hasInterval: " << hasInterval << " got " << m.get(types::TYPE_REFTIME) << " eop: " << endofperiod << endl;
	if (hasInterval && (!endofperiod.defined() || m.get(types::TYPE_REFTIME) > endofperiod))
	{
//cerr << "Flushing." << endl;
		flush();
		buffer.push_back(m);
		setEndOfPeriod(m.get(types::TYPE_REFTIME).upcast<types::Reftime>());
	}
	else
		buffer.push_back(m);
//cerr << "Buffering " << m.get(types::TYPE_REFTIME) << endl;		
	return true;
}

void Stream::flush()
{
	if (buffer.empty()) return;
	std::sort(buffer.begin(), buffer.end(), STLCompare(sorter));
	for (vector<Metadata>::iterator i = buffer.begin();
			i != buffer.end(); ++i)
		nextConsumer(*i);
	buffer.clear();
}

}
}


// vim:set ts=4 sw=4:
