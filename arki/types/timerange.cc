/*
 * types/timerange - Time span information
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
#include <arki/types/timerange.h>
#include <arki/types/utils.h>
#include <arki/utils/codec.h>
#include "config.h"
#include <sstream>
#include <iomanip>
#include <cmath>
#include <cstring>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE types::TYPE_TIMERANGE
#define TAG "timerange"
#define SERSIZELEN 1
#define LUATAG_TIMERANGE LUATAG_TYPES ".timerange"
#define LUATAG_GRIB1 LUATAG_TIMERANGE ".grib1"
#define LUATAG_GRIB2 LUATAG_TIMERANGE ".grib2"

using namespace std;
using namespace wibble;
using namespace arki::utils;
using namespace arki::utils::codec;

namespace arki {
namespace types {

const char* traits<Timerange>::type_tag = TAG;
const types::Code traits<Timerange>::type_code = CODE;
const size_t traits<Timerange>::type_sersize_bytes = SERSIZELEN;
const char* traits<Timerange>::type_lua_tag = LUATAG_TIMERANGE;

// Style constants
const unsigned char Timerange::GRIB1;
const unsigned char Timerange::GRIB2;
const unsigned char Timerange::BUFR;

// Constants from meteosatlib's libgrib
/// Time ranges
typedef enum t_enum_GRIB_TIMERANGE {
  GRIB_TIMERANGE_UNKNOWN                                                  = -1,
  GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1                              = 0,
  GRIB_TIMERANGE_ANALYSIS_AT_REFTIME                                      = 1,
  GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2                 = 2,
  GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2               = 3,
  GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2     = 4,
  GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1               = 5,
  GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2             = 6,
  GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2              = 7,
  GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2                               = 10,
  GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2           = 51,
  GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2     = 113,
  GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2    = 114,
  GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2       = 115,
  GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2     = 116,
  GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED          = 117,
  GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2    = 118,
  GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED            = 119,
  GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2                 = 123,
  GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2            = 124,
  GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY       = 125,
  GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS                  = 128,
  GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS             = 129,
  GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES                       = 130,
  GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES                  = 131
} t_enum_GRIB_TIMERANGE;

/// Time units
typedef enum t_enum_GRIB_TIMEUNIT {
  GRIB_TIMEUNIT_UNKNOWN = -1,  ///< software internal use
  GRIB_TIMEUNIT_MINUTE  = 0,   ///< minute
  GRIB_TIMEUNIT_HOUR    = 1,   ///< hour
  GRIB_TIMEUNIT_DAY     = 2,   ///< day
  GRIB_TIMEUNIT_MONTH   = 3,   ///< month
  GRIB_TIMEUNIT_YEAR    = 4,   ///< year
  GRIB_TIMEUNIT_DECADE  = 5,   ///< 10 years
  GRIB_TIMEUNIT_NORMAL  = 6,   ///< 30 years
  GRIB_TIMEUNIT_CENTURY = 7,   ///< century
  GRIB_TIMEUNIT_HOURS3  = 10,  ///< 3 hours
  GRIB_TIMEUNIT_HOURS6  = 11,  ///< 6 hours
  GRIB_TIMEUNIT_HOURS12 = 12,  ///< 12 hours
  GRIB_TIMEUNIT_SECOND  = 254  ///< seconds
} t_enum_GRIB_TIMEUNIT;

static std::string formatTimeUnit(t_enum_GRIB_TIMEUNIT unit)
{
	switch (unit)
	{
		case GRIB_TIMEUNIT_UNKNOWN:
			throw wibble::exception::Consistency("formatting TimeRange unit", "time unit is UNKNOWN (-1)");
		case GRIB_TIMEUNIT_MINUTE: return "m";
		case GRIB_TIMEUNIT_HOUR: return "h";
		case GRIB_TIMEUNIT_DAY: return "d";
		case GRIB_TIMEUNIT_MONTH: return "mo";
		case GRIB_TIMEUNIT_YEAR: return "y";
		case GRIB_TIMEUNIT_DECADE: return "de";
		case GRIB_TIMEUNIT_NORMAL: return "no";
		case GRIB_TIMEUNIT_CENTURY: return "ce";
		case GRIB_TIMEUNIT_HOURS3: return "h3";
		case GRIB_TIMEUNIT_HOURS6: return "h6";
		case GRIB_TIMEUNIT_HOURS12: return "h12";
		case GRIB_TIMEUNIT_SECOND: return "s";
		default:
			throw wibble::exception::Consistency("normalising TimeRange", "time unit is unknown ("+str::fmt(unit)+")");
	}
}

static t_enum_GRIB_TIMEUNIT parseTimeUnit(const std::string& tu)
{
	if (tu == "m") return GRIB_TIMEUNIT_MINUTE;
	if (tu == "h") return GRIB_TIMEUNIT_HOUR;
	if (tu == "d") return GRIB_TIMEUNIT_DAY;
	if (tu == "mo") return GRIB_TIMEUNIT_MONTH;
	if (tu == "y") return GRIB_TIMEUNIT_YEAR;
	if (tu == "de") return GRIB_TIMEUNIT_DECADE;
	if (tu == "no") return GRIB_TIMEUNIT_NORMAL;
	if (tu == "ce") return GRIB_TIMEUNIT_CENTURY;
	if (tu == "h3") return GRIB_TIMEUNIT_HOURS3;
	if (tu == "h6") return GRIB_TIMEUNIT_HOURS6;
	if (tu == "h12") return GRIB_TIMEUNIT_HOURS12;
	if (tu == "s") return GRIB_TIMEUNIT_SECOND;
	throw wibble::exception::Consistency("parsing TimeRange unit", "unknown time unit \""+tu+"\"");
}

Timerange::Style Timerange::parseStyle(const std::string& str)
{
	if (str == "GRIB1") return GRIB1;
	if (str == "GRIB2") return GRIB2;
	if (str == "BUFR") return BUFR;
	throw wibble::exception::Consistency("parsing Timerange style", "cannot parse Timerange style '"+str+"': only GRIB1, GRIB2 and BUFR are supported");
}

std::string Timerange::formatStyle(Timerange::Style s)
{
	switch (s)
	{
		case Timerange::GRIB1: return "GRIB1";
		case Timerange::GRIB2: return "GRIB2";
		case Timerange::BUFR: return "BUFR";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

Item<Timerange> Timerange::decode(const unsigned char* buf, size_t len)
{
	using namespace utils::codec;
	Decoder dec(buf, len);
	Style s = (Style)dec.popUInt(1, "timerange");
	switch (s)
	{
		case GRIB1: {
			uint8_t type = dec.popUInt(1, "GRIB1 type"),
				unit = dec.popUInt(1, "GRIB1 unit");
			int8_t  p1   = dec.popSInt(1, "GRIB1 p1"),
			        p2   = dec.popSInt(1, "GRIB1 p2");
			return timerange::GRIB1::create(type, unit, p1, p2);
	        }
		case GRIB2: {
			uint8_t type = dec.popUInt(1, "GRIB2 type"),
				unit = dec.popUInt(1, "GRIB2 unit");
			int32_t p1   = dec.popSInt(4, "GRIB2 p1"),
			        p2   = dec.popSInt(4, "GRIB2 p2");
			return timerange::GRIB2::create(type, unit, p1, p2);
		}
		case BUFR: {
			uint8_t unit   = dec.popUInt(1, "BUFR unit");
			unsigned value = dec.popVarint<unsigned>("BUFR value");
			return timerange::BUFR::create(value, unit);
		}
		default:
			throw wibble::exception::Consistency("parsing Timerange", "style is " + formatStyle(s) + " but we can only decode GRIB1, GRIB2 and BUFR");
	}
}

static int getNumber(const char * & start, const char* what)
{
	char* endptr;

	if (!*start)
		throw wibble::exception::Consistency("parsing TimeRange", string("no ") + what);

	int res = strtol(start, &endptr, 10);
	if (endptr == start)
		throw wibble::exception::Consistency("parsing TimeRange",
				string("expected ") + what + ", but found \"" + start + "\"");
	start = endptr;

	// Skip commas and spaces, if any
	while (*start && (::isspace(*start) || *start == ','))
		++start;

	return res;
}

static void skipSuffix(const char*& start)
{
	// Skip anything until the next space or comma
	while (*start && !::isspace(*start) && *start != ',')
		++start;

	// Skip further commas and spaces
	while (*start && (::isspace(*start) || *start == ','))
		++start;
}

Item<Timerange> Timerange::decodeString(const std::string& val)
{
	string inner;
	Timerange::Style style = outerParse<Timerange>(val, inner);
	switch (style)
	{
		case Timerange::GRIB1: {
			const char* start = inner.c_str();
			char* endptr;
			int p1 = -1, p2 = -1;
			string p1tu, p2tu;

			if (!*start)
				throw wibble::exception::Consistency("parsing TimeRange", "value is empty");

			// Parse the type
			int type = strtol(start, &endptr, 10);
			if (endptr == start)
				throw wibble::exception::Consistency("parsing TimeRange",
						"expected type, but found \"" + inner.substr(start - inner.c_str()) + "\"");
			start = endptr;

			// Skip colons and spaces, if any
			while (*start && (::isspace(*start) || *start == ','))
				++start;

			if (*start)
			{
				const char* sfxend;

				// Parse p1
				p1 = strtol(start, &endptr, 10);
				if (endptr == start)
					throw wibble::exception::Consistency("parsing TimeRange",
							"expected p1, but found \"" + inner.substr(start - inner.c_str()) + "\"");
				start = endptr;

				// Parse p1 suffix
				sfxend = start;
				while (*sfxend && isalnum(*sfxend))
					++sfxend;
				if (sfxend != start)
				{
					p1tu = inner.substr(start-inner.c_str(), sfxend-start);
					start = sfxend;
				}

				// Skip colons and spaces, if any
				while (*start && (::isspace(*start) || *start == ','))
					++start;

				if (*start)
				{
					// Parse p2
					p2 = strtol(start, &endptr, 10);
					if (endptr == start)
						throw wibble::exception::Consistency("parsing TimeRange",
								"expected p2, but found \"" + inner.substr(start - inner.c_str()) + "\"");
					start = endptr;

					// Parse p2 suffix
					sfxend = start;
					while (*sfxend && isalnum(*sfxend))
						++sfxend;
					if (sfxend != start)
					{
						p2tu = inner.substr(start-inner.c_str(), sfxend-start);
						start = sfxend;
					}
				}
			}

			if (*start)
				throw wibble::exception::Consistency("parsing TimeRange",
					"found trailing characters at the end: \"" + inner.substr(start - inner.c_str()) + "\"");

			switch ((t_enum_GRIB_TIMERANGE)type)
			{
				case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1:
					if (p1 == -1)
						throw wibble::exception::Consistency("parsing TimeRange",
								"p1 not found, but it is required");
					if (p2 != -1)
						throw wibble::exception::Consistency("parsing TimeRange",
								"found an extra p2 when it was not required");
					return timerange::GRIB1::create(type, parseTimeUnit(p1tu), p1, 0);
				case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
					if (p1 != -1)
						throw wibble::exception::Consistency("parsing TimeRange",
								"found an extra p1 when it was not required");
					if (p2 != -1)
						throw wibble::exception::Consistency("parsing TimeRange",
								"found an extra p2 when it was not required");
					return timerange::GRIB1::create(type, 254, 0, 0);
				case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
				case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
				case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
				case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
				case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2:
				case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2:
				case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2:
				case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2:
				case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2:
				case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2:
				case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2:
				case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED:
				case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED:
				case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY:
				case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS:
				case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS:
				case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES:
				case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES:
					if (p1 == -1)
						throw wibble::exception::Consistency("parsing TimeRange",
								"p1 not found, but it is required");
					if (p2 == -1)
						throw wibble::exception::Consistency("parsing TimeRange",
								"p2 not found, but it is required");
					if (p1tu != p2tu)
						throw wibble::exception::Consistency("parsing TimeRange",
								"time unit for p1 (\""+p1tu+"\") is different than time unit of p2 (\""+p2tu+"\")");
					return timerange::GRIB1::create(type, parseTimeUnit(p1tu), p1, p2);
				case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2: {
					if (p1 == -1)
						throw wibble::exception::Consistency("parsing TimeRange",
								"p1 not found, but it is required");
					if (p2 != -1)
						throw wibble::exception::Consistency("parsing TimeRange",
								"found an extra p2 when it was not required");
					p2 = p1 & 0xff;
					p1 = p1 >> 8;
					return timerange::GRIB1::create(type, parseTimeUnit(p1tu), p1, p2);
				}
				case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2:
				case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2:
				case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2:
					if (p1 == -1)
						throw wibble::exception::Consistency("parsing TimeRange",
								"p1 not found, but it is required");
					if (p2 != -1)
						throw wibble::exception::Consistency("parsing TimeRange",
								"found an extra p2 when it was not required");
					return timerange::GRIB1::create(type, parseTimeUnit(p1tu), 0, p1);
				default:
					// Fallback for unknown types
					if (p1 == -1)
						throw wibble::exception::Consistency("parsing TimeRange",
								"p1 not found, but it is required");
					if (p2 == -1)
						throw wibble::exception::Consistency("parsing TimeRange",
								"p2 not found, but it is required");
					if (p1tu != p2tu)
						throw wibble::exception::Consistency("parsing TimeRange",
								"time unit for p1 (\""+p1tu+"\") is different than time unit of p2 (\""+p2tu+"\")");
					return timerange::GRIB1::create(type, parseTimeUnit(p1tu), p1, p2);
			}
		}
		case Timerange::GRIB2: {
			const char* start = inner.c_str();
			int type = getNumber(start, "time range type");
			int unit = getNumber(start, "unit of time range values");
			int p1 = getNumber(start, "first time range value");
			skipSuffix(start);
			int p2 = getNumber(start, "second time range value");
			skipSuffix(start);
			return timerange::GRIB2::create(type, unit, p1, p2);
		}
		case Timerange::BUFR: {
			const char* start = inner.c_str();
			unsigned value = getNumber(start, "forecast seconds");
			unsigned unit = parseTimeUnit(start);
			return timerange::BUFR::create(value, unit);
		}
		default:
			throw wibble::exception::Consistency("parsing Timerange", "unknown Timerange style " + str::fmt(style));
	}
}

static int arkilua_new_grib1(lua_State* L)
{
	int type = luaL_checkint(L, 1);
	int unit = luaL_checkint(L, 2);
	int p1 = luaL_checkint(L, 3);
	int p2 = luaL_checkint(L, 4);
	Item<> res = timerange::GRIB1::create(type, unit, p1, p2);
	res->lua_push(L);
	return 1;
}

static int arkilua_new_grib2(lua_State* L)
{
	int type = luaL_checkint(L, 1);
	int unit = luaL_checkint(L, 2);
	int p1 = luaL_checkint(L, 3);
	int p2 = luaL_checkint(L, 4);
	Item<> res = timerange::GRIB2::create(type, unit, p1, p2);
	res->lua_push(L);
	return 1;
}

static int arkilua_new_bufr(lua_State* L)
{
	int value = luaL_checkint(L, 1);
	int unit = 254;
	if (lua_gettop(L) > 1)
		unit = luaL_checkint(L, 2);
	timerange::BUFR::create(value, unit)->lua_push(L);
	return 1;
}

void Timerange::lua_loadlib(lua_State* L)
{
	static const struct luaL_reg lib [] = {
		{ "grib1", arkilua_new_grib1 },
		{ "grib2", arkilua_new_grib2 },
		{ "bufr", arkilua_new_bufr },
		{ NULL, NULL }
	};
	luaL_openlib(L, "arki_timerange", lib, 0);
	lua_pop(L, 1);
}

namespace timerange {

static TypeCache<GRIB1> cache_grib1;
static TypeCache<GRIB2> cache_grib2;
static TypeCache<BUFR> cache_bufr;

Timerange::Style GRIB1::style() const { return Timerange::GRIB1; }

void GRIB1::encodeWithoutEnvelope(Encoder& enc) const
{
	Timerange::encodeWithoutEnvelope(enc);
	enc.addUInt(m_type, 1).addUInt(m_unit, 1).addSInt(m_p1, 1).addSInt(m_p2, 1);
}

std::ostream& GRIB1::writeNumbers(std::ostream& o) const
{
	o << setfill('0') << internal;
	switch ((t_enum_GRIB_TIMERANGE)m_type)
	{
		case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1: {
			string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)m_unit);
			o << setw(3) << (int)m_type << ", " << setw(3) << (int)m_p1 << suffix;
			break;
		}
		case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
			o << setw(3) << (int)m_type;
			break;
		case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2: {
		case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2:
		case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2:
		case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED:
		case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED:
		case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY:
		case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS:
		case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS:
		case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES:
		case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES:
			string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)m_unit);
			o << setw(3) << (int)m_type << ", " << setw(3) << (int)m_p1 << suffix << ", " << setw(3) << (int)m_p2 << suffix;
			break;
		}
		case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2: {
			string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)m_unit);
			o << setw(3) << (int)m_type << ", " << setw(5) << ((int)m_p1 << 8 | (int)m_p2) << suffix;
			break;
		}
		case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2:
		case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2: {
			string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)m_unit);
			o << setw(3) << (int)m_type << ", " << setw(3) << (int)m_p2 << suffix;
			break;
		}
		default:
			// Fallback for unknown types
			string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)m_unit);
			o << setw(3) << (int)m_type << ", " << setw(3) << (int)m_p1 << suffix << ", " << setw(3) << (int)m_p2 << suffix;
			break;
	}

	return o << setfill(' ');
}

std::ostream& GRIB1::writeToOstream(std::ostream& o) const
{
	o << formatStyle(style()) << "(";
	writeNumbers(o);
	return o << ")";
}

std::string GRIB1::exactQuery() const
{
	stringstream o;
	o << formatStyle(style()) << ", ";
	writeNumbers(o);
	return o.str();
}

const char* GRIB1::lua_type_name() const { return "arki.types.timerange.grib1"; }
bool GRIB1::lua_lookup(lua_State* L, const std::string& name) const
{
	int type;
	timerange::GRIB1::Unit unit;
	int p1, p2;
	getNormalised(type, unit, p1, p2);

	if (name == "type")
		lua_pushnumber(L, type);
	else if (name == "unit")
		switch (unit)
		{
			case timerange::GRIB1::SECOND: lua_pushstring(L, "second"); break;
			case timerange::GRIB1::MONTH: lua_pushstring(L, "month"); break;
			default: lua_pushnil(L); break;
		}
	else if (name == "p1")
		lua_pushnumber(L, p1);
	else if (name == "p2")
		lua_pushnumber(L, p2);
	else
		return Timerange::lua_lookup(L, name);
	return true;
}

int GRIB1::compare_local(const Timerange& o) const
{
	// We should be the same kind, so upcast
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Timerange, but is a ") + typeid(&o).name() + " instead");

	int atype, ap1, ap2, btype, bp1, bp2;
	Unit aunit, bunit;
	getNormalised(atype, aunit, ap1, ap2);
	v->getNormalised(btype, bunit, bp1, bp2);

	if (int res = atype - btype) return res;
	if (int res = aunit - bunit) return res;
	if (int res = ap1 - bp1) return res;
	return ap2 - bp2;
}

bool GRIB1::operator==(const Type& o) const
{
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v) return false;

	int atype, ap1, ap2, btype, bp1, bp2;
	Unit aunit, bunit;
	getNormalised(atype, aunit, ap1, ap2);
	v->getNormalised(btype, bunit, bp1, bp2);

	return atype == btype && aunit == bunit && ap1 == bp1 && ap2 == bp2;
}

Item<GRIB1> GRIB1::create(unsigned char type, unsigned char unit, signed char p1, signed char p2)
{
	GRIB1* res = new GRIB1;
	res->m_type = type;
	res->m_unit = unit;
	res->m_p1 = p1;
	res->m_p2 = p2;
	return cache_grib1.intern(res);
}

void GRIB1::getNormalised(int& otype, Unit& ounit, int& op1, int& op2) const
{
	otype = m_type;

	// Make sense of the time range unit
	int timemul = 1;
	ounit = SECOND;
	switch ((t_enum_GRIB_TIMEUNIT)m_unit)
	{
		case GRIB_TIMEUNIT_UNKNOWN:
			throw wibble::exception::Consistency("normalising TimeRange", "time unit is UNKNOWN (-1)");
		case GRIB_TIMEUNIT_MINUTE: timemul = 60; break;
		case GRIB_TIMEUNIT_HOUR: timemul = 3600; break;
		case GRIB_TIMEUNIT_DAY: timemul = 3600*24; break;
		case GRIB_TIMEUNIT_MONTH: timemul = 1; ounit = MONTH; break;
		case GRIB_TIMEUNIT_YEAR: timemul = 12; ounit = MONTH; break;
		case GRIB_TIMEUNIT_DECADE: timemul = 120; ounit = MONTH; break;
		case GRIB_TIMEUNIT_NORMAL: timemul = 12*30; ounit = MONTH; break;
		case GRIB_TIMEUNIT_CENTURY: timemul = 12*100; ounit = MONTH; break;
		case GRIB_TIMEUNIT_HOURS3: timemul = 3600*3; break;
		case GRIB_TIMEUNIT_HOURS6: timemul = 3600*6; break;
		case GRIB_TIMEUNIT_HOURS12: timemul = 3600*12; break;
		case GRIB_TIMEUNIT_SECOND: timemul = 1; break;
		default:
			throw wibble::exception::Consistency("normalising TimeRange", "time unit is unknown ("+str::fmt(m_unit)+")");
	}

	op1 = 0, op2 = 0;

	switch ((t_enum_GRIB_TIMERANGE)m_type)
	{
		case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1:
			op1 = m_p1; break;
		case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
			break;
		case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2:
			op1 = m_p1; op2 = m_p2; break;
		case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2:
			op1 = m_p1 << 8 | m_p2; break;
		case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2:
		case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2:
		case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED:
		case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED:
		case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY:
		case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS:
		case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS:
		case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES:
		case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES:
			op1 = m_p1; op2 = m_p2; break;
		case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2:
		case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2:
			op2 = m_p2; break;
		default:
			// Fallback for unknown time range types
			op1 = m_p1; op2 = m_p2; break;
	}

	op1 *= timemul;
	op2 *= timemul;
}


Timerange::Style GRIB2::style() const { return Timerange::GRIB2; }

void GRIB2::encodeWithoutEnvelope(Encoder& enc) const
{
	Timerange::encodeWithoutEnvelope(enc);
	enc.addUInt(m_type, 1).addUInt(m_unit, 1).addSInt(m_p1, 4).addSInt(m_p2, 4);
}

std::ostream& GRIB2::writeToOstream(std::ostream& o) const
{
	utils::SaveIOState sios(o);
	string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)m_unit);

	return o
	  << formatStyle(style()) << "("
	  << setfill('0') << internal
	  << setw(3) << (int)m_type << ", "
	  << setw(3) << (int)m_unit << ", "
	  << setw(10) << (int)m_p1 << suffix << ", "
	  << setw(10) << (int)m_p2 << suffix
	  << ")";
}

std::string GRIB2::exactQuery() const
{
	stringstream o;
	o << formatStyle(style()) << ","
	  << (int)m_type << ","
	  << (int)m_unit << ","
	  << (int)m_p1 << ","
	  << (int)m_p2;
	return o.str();
}

const char* GRIB2::lua_type_name() const { return "arki.types.timerange.grib2"; }

bool GRIB2::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "type")
		lua_pushnumber(L, type());
	else if (name == "unit")
		lua_pushstring(L, formatTimeUnit((t_enum_GRIB_TIMEUNIT)unit()).c_str());
	else if (name == "p1")
		lua_pushnumber(L, p1());
	else if (name == "p2")
		lua_pushnumber(L, p2());
	else
		return Timerange::lua_lookup(L, name);
	return true;
}

int GRIB2::compare_local(const Timerange& o) const
{
	// We should be the same kind, so upcast
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a GRIB2 Timerange, but is a ") + typeid(&o).name() + " instead");

	// TODO: normalise the time units if needed
	if (int res = m_type - v->m_type) return res;
	if (int res = m_unit - v->m_unit) return res;
	if (int res = m_p1 - v->m_p1) return res;
	return m_p2 - v->m_p2;
}

bool GRIB2::operator==(const Type& o) const
{
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v) return false;
	// TODO: normalise the time units if needed
	return m_type == v->m_type && m_unit == v->m_unit && m_p1 == v->m_p1 && m_p2 == v->m_p2;
}

Item<GRIB2> GRIB2::create(unsigned char type, unsigned char unit, signed long p1, signed long p2)
{
	GRIB2* res = new GRIB2;
	res->m_type = type;
	res->m_unit = unit;
	res->m_p1 = p1;
	res->m_p2 = p2;
	return cache_grib2.intern(res);
}

bool BUFR::is_seconds() const
{
	switch ((t_enum_GRIB_TIMEUNIT)m_unit)
	{
		case GRIB_TIMEUNIT_UNKNOWN:
			throw wibble::exception::Consistency("normalising TimeRange", "time unit is UNKNOWN (-1)");
		case GRIB_TIMEUNIT_MINUTE:
		case GRIB_TIMEUNIT_HOUR:
		case GRIB_TIMEUNIT_DAY:
		case GRIB_TIMEUNIT_HOURS3:
		case GRIB_TIMEUNIT_HOURS6:
		case GRIB_TIMEUNIT_HOURS12:
		case GRIB_TIMEUNIT_SECOND:
			return true;
		case GRIB_TIMEUNIT_MONTH:
		case GRIB_TIMEUNIT_YEAR:
		case GRIB_TIMEUNIT_DECADE:
		case GRIB_TIMEUNIT_NORMAL:
		case GRIB_TIMEUNIT_CENTURY:
			return false;
		default:
			throw wibble::exception::Consistency("normalising TimeRange", "time unit is unknown ("+str::fmt(m_unit)+")");
	}
}

unsigned BUFR::seconds() const
{
	if (m_value == 0) return 0;

	switch ((t_enum_GRIB_TIMEUNIT)m_unit)
	{
		case GRIB_TIMEUNIT_UNKNOWN:
			throw wibble::exception::Consistency("normalising TimeRange", "time unit is UNKNOWN (-1)");
		case GRIB_TIMEUNIT_MINUTE: return m_value * 60;
		case GRIB_TIMEUNIT_HOUR: return m_value * 3600;
		case GRIB_TIMEUNIT_DAY: return m_value * 3600*24;
		case GRIB_TIMEUNIT_HOURS3: return m_value * 3600*3;
		case GRIB_TIMEUNIT_HOURS6: return m_value * 3600*6;
		case GRIB_TIMEUNIT_HOURS12: return m_value * 3600*12;
		case GRIB_TIMEUNIT_SECOND: return m_value * 1;
		default:
			throw wibble::exception::Consistency("normalising TimeRange", "time unit ("+str::fmt(m_unit)+") does not convert to seconds");
	}
}

unsigned BUFR::months() const
{
	if (m_value == 0) return 0;

	switch ((t_enum_GRIB_TIMEUNIT)m_unit)
	{
		case GRIB_TIMEUNIT_UNKNOWN:
			throw wibble::exception::Consistency("normalising TimeRange", "time unit is UNKNOWN (-1)");
		case GRIB_TIMEUNIT_MONTH: return m_value * 1;
		case GRIB_TIMEUNIT_YEAR: return m_value * 12;
		case GRIB_TIMEUNIT_DECADE: return m_value * 120;
		case GRIB_TIMEUNIT_NORMAL: return m_value * 12*30;
		case GRIB_TIMEUNIT_CENTURY: return m_value * 12*100;
		default:
			throw wibble::exception::Consistency("normalising TimeRange", "time unit ("+str::fmt(m_unit)+") does not convert to months");
	}
}

Timerange::Style BUFR::style() const { return Timerange::BUFR; }

void BUFR::encodeWithoutEnvelope(Encoder& enc) const
{
	Timerange::encodeWithoutEnvelope(enc);
	enc.addUInt(m_unit, 1);
	enc.addVarint(m_value);
}

std::ostream& BUFR::writeToOstream(std::ostream& o) const
{
	utils::SaveIOState sios(o);
	string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)m_unit);
	o << formatStyle(style()) << "(";
	if (m_value != 0) o << m_value << suffix;
	return o << ")";
}

std::string BUFR::exactQuery() const
{
	stringstream o;
	string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)m_unit);
	o << formatStyle(style()) << "," << m_value << suffix;
	return o.str();
}

const char* BUFR::lua_type_name() const { return "arki.types.timerange.bufr"; }

bool BUFR::lua_lookup(lua_State* L, const std::string& name) const
{
	if (name == "value")
		lua_pushnumber(L, value());
	else if (name == "unit")
		lua_pushnumber(L, unit());
	else if (name == "is_seconds")
		lua_pushboolean(L, is_seconds());
	else if (name == "seconds")
		if (is_seconds())
			lua_pushnumber(L, seconds());
		else
			lua_pushnil(L);
	else if (name == "months")
		if (is_seconds())
			lua_pushnil(L);
		else
			lua_pushnumber(L, months());
	else
		return Timerange::lua_lookup(L, name);
	return true;
}

int BUFR::compare_local(const Timerange& o) const
{
	// We should be the same kind, so upcast
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v)
		throw wibble::exception::Consistency(
			"comparing metadata types",
			string("second element claims to be a BUFR Timerange, but is a ") + typeid(&o).name() + " instead");
	if (int res = (is_seconds() ? 0 : 1) - (v->is_seconds() ? 0 : 1)) return res;
	if (is_seconds())
		return seconds() - v->seconds();
	else
		return months() - v->months();
}

bool BUFR::operator==(const Type& o) const
{
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v) return false;
	if (is_seconds() != v->is_seconds()) return false;
	if (is_seconds())
		return seconds() == v->seconds();
	else
		return months() == v->months();
}

Item<BUFR> BUFR::create(unsigned value, unsigned char unit)
{
	BUFR* res = new BUFR;
	res->m_value = value;
	res->m_unit = unit;
	return cache_bufr.intern(res);
}


static void debug_interns()
{
	fprintf(stderr, "timerange GRIB1: sz %zd reused %zd\n", cache_grib1.size(), cache_grib1.reused());
	fprintf(stderr, "timerange GRIB2: sz %zd reused %zd\n", cache_grib2.size(), cache_grib2.reused());
	fprintf(stderr, "timerange BUFR: sz %zd reused %zd\n", cache_bufr.size(), cache_bufr.reused());
}

}

static MetadataType timerangeType = MetadataType::create<Timerange>(timerange::debug_interns);

}
}

#include <arki/types.tcc>

// vim:set ts=4 sw=4:
