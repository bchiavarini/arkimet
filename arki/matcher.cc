/*
 * matcher - Match metadata expressions
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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
#include <arki/metadata.h>
#include <arki/summary.h>
#include <arki/summary-old.h>
#include <arki/configfile.h>
#include <wibble/regexp.h>
#include <wibble/string.h>
#include <memory>

using namespace std;
using namespace wibble;

namespace arki {

namespace matcher {

// Registry of matcher information
static std::map<std::string, MatcherType*>* matchers = 0;

MatcherType::MatcherType(const std::string& name, types::Code code, subexpr_parser parse_func)
    : name(name), code(code), parse_func(parse_func)
{
	// Ensure that the map is created before we add items to it
	if (!matchers)
	{
		matchers = new map<string, MatcherType*>;
	}

	(*matchers)[name] = this;
}

MatcherType::~MatcherType()
{
	if (!matchers)
		return;
	
	matchers->erase(name);
}

MatcherType* MatcherType::find(const std::string& name)
{
	if (!matchers) return 0;
	std::map<std::string, MatcherType*>::const_iterator i = matchers->find(name);
	if (i == matchers->end()) return 0;
	return i->second;
}

std::vector<std::string> MatcherType::matcherNames()
{
	vector<string> res;
	for (std::map<std::string, MatcherType*>::const_iterator i = matchers->begin();
			i != matchers->end(); ++i)
		res.push_back(i->first);
	return res;
}

static MatcherAliasDatabase* aliasdb = 0;


std::string OR::name() const
{
	if (empty()) return string();
	return front()->name();
}

bool OR::matchItem(const Item<>& t) const
{
	if (empty()) return true;

	for (const_iterator i = begin(); i != end(); ++i)
		if ((*i)->matchItem(t))
			return true;
	return false;
}

std::string OR::toString() const
{
	if (empty()) return string();

	if (not unparsed.empty()) return front()->name() + ":" + unparsed;

	std::string res = front()->name() + ":";
	for (const_iterator i = begin(); i != end(); ++i)
	{
		if (i != begin())
			res += " or ";
		res += (*i)->toString();
	}
	return res;
}

OR* OR::parse(const MatcherType& mt, const std::string& pattern)
{
	auto_ptr<OR> res(new OR(pattern));

	// Fetch the alias database for this type
	const Aliases* aliases = MatcherAliasDatabase::get(mt.name);

	// Split 'patterns' on /\s*or\s*/i
	Splitter splitter("[ \t]+or[ \t]+", REG_EXTENDED | REG_ICASE);

	for (Splitter::const_iterator i = splitter.begin(pattern);
			i != splitter.end(); ++i)
	{
		const OR* exprs = aliases ? aliases->get(*i) : 0;
		if (exprs)
			for (OR::const_iterator j = exprs->begin(); j != exprs->end(); ++j)
				res->push_back(*j);
		else
			res->push_back(mt.parse_func(*i));
	}

	return res.release();
}


std::string AND::name() const
{
	return "matcher";
}

bool AND::matchItem(const Item<>& t) const
{
	if (empty()) return true;

	const_iterator i = find(t->serialisationCode());
	if (i == end()) return true;

	return i->second->matchItem(t);
}

template<typename COLL>
static bool mdmatch(const Implementation& matcher, const COLL& c)
{
	for (typename COLL::const_iterator i = c.begin(); i != c.end(); ++i)
		if (matcher.matchItem(*i))
			return true;
	return false;
}

bool AND::matchMetadata(const Metadata& md) const
{
	if (empty()) return true;

	for (const_iterator i = begin(); i != end(); ++i)
	{
		UItem<> item = md.get(i->first);
		if (!item.defined()) return false;
		if (!i->second->matchItem(item)) return false;
	}
	return true;
}

bool AND::matchSummaryItem(const oldsummary::Item& si) const
{
	if (empty()) return true;

	for (const_iterator i = begin(); i != end(); ++i)
	{
		if (!oldsummary::Item::accepts(i->first)) continue;
		UItem<> item = si.get(i->first);
		if (!item.defined()) return false;
		if (!i->second->matchItem(item)) return false;
	}
	return true;
}

bool AND::matchSummary(const OldSummary& s) const
{
	if (empty()) return true;

	for (OldSummary::map_t::const_iterator i = s.m_items.begin();
			i != s.m_items.end(); ++i)
		if (matchItem(i->second->reftimeMerger.makeReftime()) && matchSummaryItem(*i->first))
			return true;
	return false;
}

const OR* AND::get(types::Code code) const
{
	const_iterator i = find(code);
	if (i == end()) return 0;
	return i->second->upcast<OR>();
}

std::string AND::toString() const
{
	if (empty()) return string();

	std::string res;
	for (const_iterator i = begin(); i != end(); ++i)
	{
		if (i != begin())
			res += "; ";
		res += i->second->toString();
	}
	return res;
}

void AND::split(const std::set<types::Code>& codes, AND& with, AND& without) const
{
	for (const_iterator i = begin(); i != end(); ++i)
	{
		if (codes.find(i->first) != codes.end())
		{
			with.insert(*i);
		} else {
			without.insert(*i);
		}
	}
}

AND* AND::parse(const std::string& pattern)
{
	auto_ptr<AND> res(new AND);

	// Split on newlines or semicolons
	wibble::Tokenizer tok(pattern, "[^\n;]+", REG_EXTENDED);

	for (wibble::Tokenizer::const_iterator i = tok.begin();
			i != tok.end(); ++i)
	{
		string expr = *i;
		size_t pos = expr.find(':');
		if (pos == string::npos)
			throw wibble::exception::Consistency(
				"parsing matcher subexpression",
				"subexpression '" + expr + "' does not contain a colon (':')");

		string type = str::tolower(str::trim(expr.substr(0, pos)));
		string patterns = str::trim(expr.substr(pos+1));

		MatcherType* mt = MatcherType::find(type);
		if (mt == 0)
			throw wibble::exception::Consistency(
				"parsing matcher subexpression",
				"unknown match type: '" + type + "'");

		res->insert(make_pair(mt->code, OR::parse(*mt, patterns)));
	}

	return res.release();
}

Aliases::~Aliases()
{
	reset();
}
void Aliases::reset()
{
	for (map<string, const OR*>::iterator i = db.begin();
			i != db.end(); ++i)
		if (i->second->unref())
			delete i->second;
	db.clear();
}

void Aliases::add(const MatcherType& type, const ConfigFile& entries)
{
	vector< pair<string, string> > aliases;
	vector< pair<string, string> > failed;
	for (ConfigFile::const_iterator i = entries.begin(); i != entries.end(); ++i)
		aliases.push_back(*i);
	
	/*
	 * Try multiple times to catch aliases referring to other aliases.
	 * We continue until the number of aliases to parse stops decreasing.
	 */
	for (size_t pass = 0; !aliases.empty(); ++pass)
	{
		failed.clear();
		for (vector< pair<string, string> >::const_iterator i = aliases.begin();
				i != aliases.end(); ++i)
		{
			auto_ptr<OR> val;

			// If instantiation fails, try it again later
			try {
				val.reset(OR::parse(type, i->second));
			} catch (std::exception& e) {
				failed.push_back(*i);
				continue;
			}

			val->ref();

			map<string, const OR*>::iterator j = db.find(i->first);
			if (j == db.end())
			{
				db.insert(make_pair(i->first, val.release()));
			} else {
				if (j->second->unref())
					delete j->second;
				j->second = val.release();
			}
		}
		if (!failed.empty() && failed.size() == aliases.size())
			// If no new aliases were successfully parsed, reparse one of the
			// failing ones to raise the appropriate exception
			OR::parse(type, failed.front().second);
		aliases = failed;
	}
}

}

void Matcher::split(const std::set<types::Code>& codes, Matcher& with, Matcher& without) const
{
	if (!m_impl)
	{
		with = 0;
		without = 0;
	} else {
		// Create the empty matchers and assign them right away, so we sort out
		// memory management
		matcher::AND* awith = new matcher::AND;
		with = awith;
		matcher::AND* awithout = new matcher::AND;
		without = awithout;
		m_impl->split(codes, *awith, *awithout);
		if (awith->empty()) with = 0;
		if (awithout->empty()) without = 0;
	}
}

Matcher Matcher::parse(const std::string& pattern)
{
	return matcher::AND::parse(pattern);
}

bool Matcher::operator()(const Summary& i) const
{
	if (m_impl) return i.match(*this);
	// An empty matcher always matches
	return true;
}

std::ostream& operator<<(std::ostream& o, const Matcher& m)
{
	return o << m.toString();
}

#if 0
bool Matcher::match(const Metadata& md) const
{
	if (!origin.match(md.origins)) return false;
	if (!product.match(md.products)) return false;
	if (!level.match(md.levels)) return false;
	if (!timerange.match(md.timeranges)) return false;
	if (!area.match(md.areas)) return false;
	if (!ensemble.match(md.ensembles)) return false;
	if (!reftime.match(md.reftime)) return false;
	return true;
}

static inline void appendList(std::string& str, char sep, const std::string& trail)
{
	if (!str.empty())
		str += sep;
	str += trail;
}

std::string Matcher::toString(bool formatted) const
{
	string res;
	char sep = formatted ? '\n' : ';';

	if (!origin.empty()) appendList(res, sep, origin.toString());
	if (!product.empty()) appendList(res, sep, product.toString());
	if (!level.empty()) appendList(res, sep, level.toString());
	if (!timerange.empty()) appendList(res, sep, timerange.toString());
	if (!area.empty()) appendList(res, sep, area.toString());
	if (!ensemble.empty()) appendList(res, sep, ensemble.toString());
	if (!reftime.empty()) appendList(res, sep, reftime.toString());

	return res;
}
#endif

MatcherAliasDatabase::MatcherAliasDatabase() {}
MatcherAliasDatabase::MatcherAliasDatabase(const ConfigFile& cfg) { add(cfg); }

void MatcherAliasDatabase::add(const ConfigFile& cfg)
{
	for (ConfigFile::const_section_iterator sec = cfg.sectionBegin();
			sec != cfg.sectionEnd(); ++sec)
	{
		matcher::MatcherType* mt = matcher::MatcherType::find(sec->first);
		if (!mt)
			// Ignore unwanted sections
			continue;
		aliasDatabase[sec->first].add(*mt, *sec->second);
	}
}

void MatcherAliasDatabase::addGlobal(const ConfigFile& cfg)
{
	if (!matcher::aliasdb)
		matcher::aliasdb = new MatcherAliasDatabase(cfg);
	else
		matcher::aliasdb->add(cfg);
}

const matcher::Aliases* MatcherAliasDatabase::get(const std::string& type)
{
	if (!matcher::aliasdb)
		return 0;

	const std::map<std::string, matcher::Aliases>& aliasDatabase = matcher::aliasdb->aliasDatabase;

	std::map<std::string, matcher::Aliases>::const_iterator i = aliasDatabase.find(type);
	if (i == aliasDatabase.end())
		return 0;
	return &(i->second);
}

const void MatcherAliasDatabase::reset()
{
	if (matcher::aliasdb)
		matcher::aliasdb->aliasDatabase.clear();
}

}

// vim:set ts=4 sw=4:
