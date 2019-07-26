#include <arki/exceptions.h>
#include <arki/types/timerange.h>
#include <arki/types/utils.h>
#include <arki/utils/iostream.h>
#include <arki/binary.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include "config.h"
#include <sstream>
#include <iomanip>
#include <cmath>
#include <cstring>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE TYPE_TIMERANGE
#define TAG "timerange"
#define SERSIZELEN 1
#define LUATAG_TIMERANGE LUATAG_TYPES ".timerange"
#define LUATAG_GRIB1 LUATAG_TIMERANGE ".grib1"
#define LUATAG_GRIB2 LUATAG_TIMERANGE ".grib2"

using namespace std;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace types {

const char* traits<Timerange>::type_tag = TAG;
const types::Code traits<Timerange>::type_code = CODE;
const size_t traits<Timerange>::type_sersize_bytes = SERSIZELEN;
const char* traits<Timerange>::type_lua_tag = LUATAG_TIMERANGE;

const char* traits<timerange::Timedef>::type_tag = TAG;
const types::Code traits<timerange::Timedef>::type_code = CODE;
const size_t traits<timerange::Timedef>::type_sersize_bytes = SERSIZELEN;
const char* traits<timerange::Timedef>::type_lua_tag = LUATAG_TIMERANGE;

// Style constants
const unsigned char Timerange::GRIB1;
const unsigned char Timerange::GRIB2;
const unsigned char Timerange::TIMEDEF;
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
  GRIB_TIMERANGE_COSMO_NUDGING                                            = 13,
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
			throw_consistency_error("formatting TimeRange unit", "time unit is UNKNOWN (-1)");
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
        {
            stringstream ss;
            ss << "cannot normalise TimeRange: time unit is unknown (" << (int)unit << ")";
            throw std::runtime_error(ss.str());
        }
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
	throw_consistency_error("parsing TimeRange unit", "unknown time unit \""+tu+"\"");
}

Timerange::Style Timerange::parseStyle(const std::string& str)
{
	if (str == "GRIB1") return GRIB1;
	if (str == "GRIB2") return GRIB2;
	if (str == "Timedef") return TIMEDEF;
	if (str == "BUFR") return BUFR;
	throw_consistency_error("parsing Timerange style", "cannot parse Timerange style '"+str+"': only GRIB1, GRIB2, Timedef and BUFR are supported");
}

std::string Timerange::formatStyle(Timerange::Style s)
{
	switch (s)
	{
		case Timerange::GRIB1: return "GRIB1";
		case Timerange::GRIB2: return "GRIB2";
		case Timerange::TIMEDEF: return "Timedef";
		case Timerange::BUFR: return "BUFR";
		default:
			std::stringstream str;
			str << "(unknown " << (int)s << ")";
			return str.str();
	}
}

unique_ptr<Timerange> Timerange::decode(BinaryDecoder& dec)
{
    Style s = (Style)dec.pop_uint(1, "timerange");
    switch (s)
    {
        case GRIB1: {
            uint8_t type = dec.pop_uint(1, "GRIB1 type"),
                unit = dec.pop_uint(1, "GRIB1 unit");
            int8_t  p1   = dec.pop_sint(1, "GRIB1 p1"),
                    p2   = dec.pop_sint(1, "GRIB1 p2");
            return createGRIB1(type, unit, p1, p2);
            }
        case GRIB2: {
            uint8_t type = dec.pop_uint(1, "GRIB2 type"),
                unit = dec.pop_uint(1, "GRIB2 unit");
            int32_t p1   = dec.pop_sint(4, "GRIB2 p1"),
                    p2   = dec.pop_sint(4, "GRIB2 p2");
            return createGRIB2(type, unit, p1, p2);
        }
        case TIMEDEF: {
            uint8_t step_unit = dec.pop_uint(1, "timedef forecast step unit");
            uint32_t step_len = 0;
            if (step_unit != 255)
                step_len = dec.pop_varint<uint32_t>("timedef forecast step length");
            uint8_t stat_type = dec.pop_uint(1, "timedef statistical processing type");
            uint8_t stat_unit = 255;
            uint32_t stat_len = 0;
            if (stat_type != 255)
            {
                stat_unit = dec.pop_uint(1, "timedef statistical processing unit");
                if (stat_unit != 255)
                    stat_len = dec.pop_varint<uint32_t>("timedef statistical processing length");
            }
            return createTimedef(step_len, (timerange::TimedefUnit)step_unit,
                                            stat_type, stat_len, (timerange::TimedefUnit)stat_unit);
        }
        case BUFR: {
            uint8_t unit   = dec.pop_uint(1, "BUFR unit");
            unsigned value = dec.pop_varint<unsigned>("BUFR value");
            return createBUFR(value, unit);
        }
		default:
			throw_consistency_error("parsing Timerange", "style is " + formatStyle(s) + " but we can only decode GRIB1, GRIB2 and BUFR");
	}
}

unique_ptr<Timerange> Timerange::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    switch (style_from_mapping(val))
    {
        case Timerange::GRIB1: return upcast<Timerange>(timerange::GRIB1::decodeMapping(val));
        case Timerange::GRIB2: return upcast<Timerange>(timerange::GRIB2::decodeMapping(val));
        case Timerange::TIMEDEF: return upcast<Timerange>(timerange::Timedef::decodeMapping(val));
        case Timerange::BUFR: return upcast<Timerange>(timerange::BUFR::decodeMapping(val));
        default:
            throw_consistency_error("parsing Timerange", "unknown Timerange style " + val.get_string());
    }
}

static void skipCommasAndSpaces(const char*& str)
{
    // Skip commas and spaces, if any
    while (*str && (::isspace(*str) || *str == ','))
        ++str;
}

static int getNumber(const char * & start, const char* what)
{
	char* endptr;

    skipCommasAndSpaces(start);

	if (!*start)
		throw_consistency_error("parsing TimeRange", string("no ") + what);

	int res = strtol(start, &endptr, 10);
	if (endptr == start)
		throw_consistency_error("parsing TimeRange",
				string("expected ") + what + ", but found \"" + start + "\"");
	start = endptr;

    skipCommasAndSpaces(start);

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

unique_ptr<Timerange> Timerange::decodeString(const std::string& val)
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
				throw_consistency_error("parsing TimeRange", "value is empty");

			// Parse the type
			int type = strtol(start, &endptr, 10);
			if (endptr == start)
				throw_consistency_error("parsing TimeRange",
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
					throw_consistency_error("parsing TimeRange",
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
						throw_consistency_error("parsing TimeRange",
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
				throw_consistency_error("parsing TimeRange",
					"found trailing characters at the end: \"" + inner.substr(start - inner.c_str()) + "\"");

			switch ((t_enum_GRIB_TIMERANGE)type)
			{
                case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1:
                    if (p1 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p1 not found, but it is required");
                    if (p2 != -1)
                        throw_consistency_error("parsing TimeRange",
                                "found an extra p2 when it was not required");
                    return createGRIB1(type, parseTimeUnit(p1tu), p1, 0);
                case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
                    if (p1 != -1)
                        throw_consistency_error("parsing TimeRange",
                                "found an extra p1 when it was not required");
                    if (p2 != -1)
                        throw_consistency_error("parsing TimeRange",
                                "found an extra p2 when it was not required");
                    return createGRIB1(type, 254, 0, 0);
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
                        throw_consistency_error("parsing TimeRange",
                                "p1 not found, but it is required");
                    if (p2 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p2 not found, but it is required");
                    if (p1tu != p2tu)
                        throw_consistency_error("parsing TimeRange",
                                "time unit for p1 (\""+p1tu+"\") is different than time unit of p2 (\""+p2tu+"\")");
                    return createGRIB1(type, parseTimeUnit(p1tu), p1, p2);
                case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2: {
                    if (p1 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p1 not found, but it is required");
                    if (p2 != -1)
                        throw_consistency_error("parsing TimeRange",
                                "found an extra p2 when it was not required");
                    p2 = p1 & 0xff;
                    p1 = p1 >> 8;
                    return createGRIB1(type, parseTimeUnit(p1tu), p1, p2);
                }
				case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2:
				case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2:
                case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2:
                    if (p1 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p1 not found, but it is required");
                    if (p2 != -1)
                        throw_consistency_error("parsing TimeRange",
                                "found an extra p2 when it was not required");
                    return createGRIB1(type, parseTimeUnit(p1tu), 0, p1);
                default:
                    // Fallback for unknown types
                    if (p1 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p1 not found, but it is required");
                    if (p2 == -1)
                        throw_consistency_error("parsing TimeRange",
                                "p2 not found, but it is required");
                    if (p1tu != p2tu)
                        throw_consistency_error("parsing TimeRange",
                                "time unit for p1 (\""+p1tu+"\") is different than time unit of p2 (\""+p2tu+"\")");
                    return createGRIB1(type, parseTimeUnit(p1tu), p1, p2);
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
            return createGRIB2(type, unit, p1, p2);
        }
        case Timerange::TIMEDEF:
            return upcast<Timerange>(timerange::Timedef::createFromYaml(inner));
        case Timerange::BUFR: {
            const char* start = inner.c_str();
            unsigned value = getNumber(start, "forecast seconds");
            unsigned unit = parseTimeUnit(start);
            return createBUFR(value, unit);
        }
        default:
        {
            stringstream ss;
            ss << "cannot parse Timerange: unknown Timerange style " << style;
            throw std::runtime_error(ss.str());
        }
    }
}

unique_ptr<timerange::Timedef> Timerange::to_timedef() const
{
    using namespace timerange;

    uint32_t step_len;
    TimedefUnit step_unit;
    uint8_t stat_type;
    uint32_t stat_len;
    TimedefUnit stat_unit;

    int ival;
    bool is_seconds;

    if (get_forecast_step(ival, is_seconds))
    {
        step_len = ival;
        step_unit = is_seconds ? timerange::UNIT_SECOND : timerange::UNIT_MONTH;
    } else {
        step_len = 0;
        step_unit = timerange::UNIT_MISSING;
    }

    if ((ival = get_proc_type()) != -1)
        stat_type = ival;
    else
        stat_type = 255;

    if (get_proc_duration(ival, is_seconds))
    {
        stat_len = ival;
        stat_unit = is_seconds ? timerange::UNIT_SECOND : timerange::UNIT_MONTH;
    } else {
        stat_len = 0;
        stat_unit = timerange::UNIT_MISSING;
    }

    return Timedef::create(step_len, step_unit, stat_type, stat_len, stat_unit);
}

static int arkilua_new_grib1(lua_State* L)
{
	int type = luaL_checkint(L, 1);
	int unit = luaL_checkint(L, 2);
	int p1 = luaL_checkint(L, 3);
	int p2 = luaL_checkint(L, 4);
    timerange::GRIB1::create(type, unit, p1, p2)->lua_push(L);
    return 1;
}

static int arkilua_new_grib2(lua_State* L)
{
	int type = luaL_checkint(L, 1);
	int unit = luaL_checkint(L, 2);
	int p1 = luaL_checkint(L, 3);
	int p2 = luaL_checkint(L, 4);
    timerange::GRIB2::create(type, unit, p1, p2)->lua_push(L);
    return 1;
}

// Parse a time unit in string or number form
static timerange::TimedefUnit arkilua_checkunit(lua_State* L, int pos)
{
    switch (lua_type(L, pos))
    {
        case LUA_TSTRING: {
            const char* str = lua_tostring(L, pos);
            timerange::TimedefUnit unit;
            if (!timerange::Timedef::timeunit_parse_suffix(str, unit))
                luaL_argcheck(L, 0, pos, "unit name not valid");
            if (*str) // Trailing garbage in unit
                luaL_argcheck(L, 0, pos, "unit name not valid");
            return unit;
        }
        case LUA_TNUMBER:
            return (timerange::TimedefUnit)lua_tointeger(L, pos);
        default:
            luaL_argcheck(L, 0, pos, "unit is not string or number");
            return timerange::UNIT_MISSING; // This should never be reached
    }
}

// Parse ("3h") or (3, "h") or (3, 1)
static int arkilua_checkunit_val(lua_State* L, int pos, timerange::TimedefUnit& unit, uint32_t& val)
{
    switch (lua_type(L, pos))
    {
        case LUA_TSTRING: {
            const char* str = lua_tostring(L, pos);
            if (!timerange::Timedef::timeunit_parse(str, unit, val))
                luaL_argcheck(L, 0, pos, "value is not a number with a time range suffix");
            if (*str) // Trailing garbage in unit
                luaL_argcheck(L, 0, pos, "value is not a number with a time range suffix");
            return 1;
        }
        case LUA_TNUMBER:
            val = lua_tointeger(L, pos);
            if (lua_gettop(L) == pos && val == 0)
                return 1;
            unit = arkilua_checkunit(L, pos + 1);
            return 2;
        default:
            luaL_argcheck(L, 0, pos, "value is not string or number");
            return 0; // This return shouldn't be reached
    }
}

static int arkilua_new_timedef(lua_State* L)
{
    using namespace timerange;

    uint32_t step_len = 0;
    TimedefUnit step_unit = timerange::UNIT_SECOND;
    int stat_type = 255;
    uint32_t stat_len = 0;
    TimedefUnit stat_unit = timerange::UNIT_MISSING;

    int pos = 1;

    // Parse forecast step
    pos += arkilua_checkunit_val(L, pos, step_unit, step_len);

    if (lua_gettop(L) >= pos)
    {
        // Parse type of statistical processing
        stat_type = luaL_checkint(L, pos);
        ++pos;

        // Parse length of statistical processing
        if (lua_gettop(L) >= pos)
            pos += arkilua_checkunit_val(L, pos, stat_unit, stat_len);
    }

    timerange::Timedef::create(step_len, step_unit, stat_type, stat_len, stat_unit)->lua_push(L);
    return 1;
}

static int arkilua_new_timedef_combined(lua_State* L)
{
    using namespace timerange;

    uint32_t step_len = 0;
    TimedefUnit step_unit = timerange::UNIT_SECOND;
    int stat_type = 255;
    uint32_t stat_len = 0;
    TimedefUnit stat_unit = timerange::UNIT_MISSING;

    int pos = 1;

    if (lua_gettop(L) != 5)
        luaL_argcheck(L, 0, lua_gettop(L), "timedef_combined requires 5 arguments");

    // Parse forecast step
    pos += arkilua_checkunit_val(L, pos, step_unit, step_len);

    // Parse type of statistical processing
    stat_type = luaL_checkint(L, pos);
    ++pos;

    // Parse length of statistical processing
    pos += arkilua_checkunit_val(L, pos, stat_unit, stat_len);

    // Normalise/uniform units to the highest common one that does not lose precision for both
    make_same_units(step_len, step_unit, stat_len, stat_unit);

    timerange::Timedef::create(step_len + stat_len, step_unit, stat_type, stat_len, stat_unit)->lua_push(L);
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
	static const struct luaL_Reg lib [] = {
		{ "grib1", arkilua_new_grib1 },
		{ "grib2", arkilua_new_grib2 },
		{ "timedef", arkilua_new_timedef },
		{ "timedef_combined", arkilua_new_timedef_combined },
		{ "bufr", arkilua_new_bufr },
		{ NULL, NULL }
	};
    utils::lua::add_global_library(L, "arki_timerange", lib);
}

unique_ptr<Timerange> Timerange::createGRIB1(unsigned char type, unsigned char unit, unsigned char p1, unsigned char p2)
{
    return upcast<Timerange>(timerange::GRIB1::create(type, unit, p1, p2));
}
unique_ptr<Timerange> Timerange::createGRIB2(unsigned char type, unsigned char unit, signed long p1, signed long p2)
{
    return upcast<Timerange>(timerange::GRIB2::create(type, unit, p1, p2));
}
unique_ptr<Timerange> Timerange::createTimedef(uint32_t step_len, timerange::TimedefUnit step_unit)
{
    return upcast<Timerange>(timerange::Timedef::create(step_len, step_unit));
}
unique_ptr<Timerange> Timerange::createTimedef(uint32_t step_len, timerange::TimedefUnit step_unit,
                                             uint8_t stat_type, uint32_t stat_len, timerange::TimedefUnit stat_unit)
{
    return upcast<Timerange>(timerange::Timedef::create(step_len, step_unit, stat_type, stat_len, stat_unit));
}
unique_ptr<Timerange> Timerange::createBUFR(unsigned value, unsigned char unit)
{
    return upcast<Timerange>(timerange::BUFR::create(value, unit));
}

namespace timerange {

bool restrict_unit(uint32_t& val, TimedefUnit& unit)
{
    switch (unit)
    {
        case UNIT_MINUTE: val *= 60; unit = UNIT_SECOND; return true;
        case UNIT_HOUR: val *= 60; unit = UNIT_MINUTE; return true;
        case UNIT_DAY: val *= 24; unit = UNIT_HOUR; return true;
        case UNIT_MONTH: return false;
        case UNIT_YEAR: val *= 12; unit = UNIT_MONTH; return true;
        case UNIT_DECADE: val *= 10; unit = UNIT_YEAR; return true;
        case UNIT_NORMAL: val *= 3; unit = UNIT_DECADE; return true;
        case UNIT_CENTURY: val *= 10; unit = UNIT_DECADE; return true;
        case UNIT_3HOURS: val *= 3; unit = UNIT_HOUR; return true;
        case UNIT_6HOURS: val *= 2; unit = UNIT_3HOURS; return true;
        case UNIT_12HOURS: val *= 2; unit = UNIT_6HOURS; return true;
        case UNIT_SECOND: return false;
        case UNIT_MISSING: return false;
        default: return false;
    }
}

bool enlarge_unit(uint32_t& val, TimedefUnit& unit)
{
    switch (unit)
    {
        case UNIT_MINUTE:
            if ((val % 60) != 0) return false;
            val /= 60; unit = UNIT_HOUR; return true;
        case UNIT_HOUR:
            if ((val % 24) != 0) return false;
            val /= 24; unit = UNIT_DAY; return true;
        case UNIT_DAY:
            return false;
        case UNIT_MONTH:
            if ((val % 12) != 0) return false;
            val /= 12; unit = UNIT_YEAR; return true;
        case UNIT_YEAR:
            if ((val % 10) != 0) return false;
            val /= 10; unit = UNIT_DECADE; return true;
        case UNIT_DECADE:
            if ((val % 100) != 0) return false;
            val /= 100; unit = UNIT_CENTURY; return true;
        case UNIT_NORMAL:
            return false;
        case UNIT_CENTURY:
            return false;
        case UNIT_3HOURS:
            if ((val % 2) != 0) return false;
            val /= 2; unit = UNIT_6HOURS; return true;
        case UNIT_6HOURS:
            if ((val % 2) != 0) return false;
            val /= 2; unit = UNIT_12HOURS; return true;
        case UNIT_12HOURS:
            if ((val % 2) != 0) return false;
            val /= 2; unit = UNIT_DAY; return true;
        case UNIT_SECOND:
            if ((val % 60) != 0) return false;
            val /= 60; unit = UNIT_MINUTE; return true;
        case UNIT_MISSING:
            return false;
        default: return false;
    }
}

bool unit_can_be_seconds(TimedefUnit unit)
{
    switch (unit)
    {
        case UNIT_SECOND:
        case UNIT_MINUTE:
        case UNIT_HOUR:
        case UNIT_3HOURS:
        case UNIT_6HOURS:
        case UNIT_12HOURS:
        case UNIT_DAY:
            return true;
        case UNIT_MONTH:
        case UNIT_YEAR:
        case UNIT_DECADE:
        case UNIT_NORMAL:
        case UNIT_CENTURY:
        case UNIT_MISSING:
        default:
            return false;
    }
}

int compare_units(TimedefUnit unit1, TimedefUnit unit2)
{
    if (unit1 == unit2) return 0;

    if (unit_can_be_seconds(unit1))
    {
        if (!unit_can_be_seconds(unit2))
        {
            std::stringstream ss;
            ss << "cannot compare two different kinds of time units (";
            ss << Timedef::timeunit_suffix(unit1);
            ss << " and ";
            ss << Timedef::timeunit_suffix(unit2);
            ss << ")";
            throw std::runtime_error(ss.str());
        }
        switch (unit1)
        {
            case UNIT_SECOND: return -1;
            case UNIT_MINUTE:
                switch (unit2)
                {
                    case UNIT_SECOND: return 1;
                    default: return -1;
                }
            case UNIT_HOUR:
                switch (unit2)
                {
                    case UNIT_MINUTE:
                    case UNIT_SECOND: return 1;
                    default: return -1;
                }
            case UNIT_3HOURS:
                switch (unit2)
                {
                    case UNIT_HOUR:
                    case UNIT_MINUTE:
                    case UNIT_SECOND: return 1;
                    default: return -1;
                }
            case UNIT_6HOURS:
                switch (unit2)
                {
                    case UNIT_3HOURS:
                    case UNIT_HOUR:
                    case UNIT_MINUTE:
                    case UNIT_SECOND: return 1;
                    default: return -1;
                }
            case UNIT_12HOURS:
                switch (unit2)
                {
                    case UNIT_6HOURS:
                    case UNIT_3HOURS:
                    case UNIT_HOUR:
                    case UNIT_MINUTE:
                    case UNIT_SECOND: return 1;
                    default: return -1;
                }
            case UNIT_DAY: return 1;
            default:
            {
                std::stringstream ss;
                ss << "unable to compare unknown unit type ";
                ss << Timedef::timeunit_suffix(unit1);
                throw std::runtime_error(ss.str());
            }
        }
    } else {
        if (unit_can_be_seconds(unit2))
        {
            std::stringstream ss;
            ss << "cannot compare two different kinds of time units (";
            ss << Timedef::timeunit_suffix(unit1);
            ss << " and ";
            ss << Timedef::timeunit_suffix(unit2);
            ss << ")";
            throw std::runtime_error(ss.str());
        }
        switch (unit1)
        {
            case UNIT_MONTH: return -1;
            case UNIT_YEAR:
                switch (unit2)
                {
                    case UNIT_MONTH: return 1;
                    default: return -1;
                }
            case UNIT_DECADE:
                switch (unit2)
                {
                    case UNIT_MONTH:
                    case UNIT_YEAR: return 1;
                    default: return -1;
                }
            case UNIT_NORMAL:
                switch (unit2)
                {
                    case UNIT_MONTH:
                    case UNIT_YEAR:
                    case UNIT_DECADE: return 1;
                    default: return -1;
                }
            case UNIT_CENTURY:
                switch (unit2)
                {
                    case UNIT_MONTH:
                    case UNIT_YEAR:
                    case UNIT_DECADE:
                    case UNIT_NORMAL: return 1;
                    default: return -1;
                }
            default:
            {
                std::stringstream ss;
                ss << "unable to compare unknown unit type ";
                ss << Timedef::timeunit_suffix(unit1);
                throw std::runtime_error(ss.str());
            }
        }
    }
}

void make_same_units(uint32_t& val1, TimedefUnit& unit1, uint32_t& val2, TimedefUnit& unit2)
{
    if (unit1 == unit2) return;

    // Try enlarging the smallest
    while (true)
    {
        int cmp = compare_units(unit1, unit2);
        if (cmp < 0)
        {
            if (!enlarge_unit(val1, unit1))
                break;
        } else if (cmp == 0) {
            return;
        } else {
            if (!enlarge_unit(val2, unit2))
                break;
        }
    }

    // Still not matching, try restricting the largest
    while (true)
    {
        int cmp = compare_units(unit1, unit2);
        if (cmp < 0)
        {
            if (!restrict_unit(val2, unit2))
                break;
        } else if (cmp == 0) {
            return;
        } else {
            if (!restrict_unit(val1, unit1))
                break;
        }
    }

    std::stringstream ss;
    ss << "Cannot convert " << Timedef::timeunit_suffix(unit1);
    ss << " and " << Timedef::timeunit_suffix(unit2);
    ss << " to the same unit";
    throw std::runtime_error(ss.str());
}


Timerange::Style GRIB1::style() const { return Timerange::GRIB1; }

void GRIB1::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Timerange::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_type, 1); enc.add_unsigned(m_unit, 1); enc.add_signed(m_p1, 1); enc.add_signed(m_p2, 1);
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

void GRIB1::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Timerange::serialiseLocal(e, f);
    e.add("ty", (int)m_type);
    e.add("un", (int)m_unit);
    e.add("p1", (int)m_p1);
    e.add("p2", (int)m_p2);
}
unique_ptr<GRIB1> GRIB1::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;
    return GRIB1::create(
            val["ty"].want_int("parsing GRIB1 timerange ty"),
            val["un"].want_int("parsing GRIB1 timerange un"),
            val["p1"].want_int("parsing GRIB1 timerange p1"),
            val["p2"].want_int("parsing GRIB1 timerange p2"));
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
    GRIB1Unit unit;
    int p1, p2;
    bool use_p1, use_p2;
    getNormalised(type, unit, p1, p2, use_p1, use_p2);

	if (name == "type")
		lua_pushnumber(L, type);
	else if (name == "unit")
		switch (unit)
		{
			case timerange::SECOND: lua_pushstring(L, "second"); break;
			case timerange::MONTH: lua_pushstring(L, "month"); break;
			default: lua_pushnil(L); break;
		}
    else if (name == "p1")
    {
        if (use_p1)
            lua_pushnumber(L, p1);
        else
            lua_pushnil(L);
    }
    else if (name == "p2")
    {
        if (use_p2)
            lua_pushnumber(L, p2);
        else
            lua_pushnil(L);
    }
	else
		return Timerange::lua_lookup(L, name);
	return true;
}

int GRIB1::compare_local(const Timerange& o) const
{
	// We should be the same kind, so upcast
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v)
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a GRIB1 Timerange, but is a ") + typeid(&o).name() + " instead");

    int atype, ap1, ap2, btype, bp1, bp2;
    GRIB1Unit aunit, bunit;
    bool dummy_use_p1, dummy_use_p2;
    getNormalised(atype, aunit, ap1, ap2, dummy_use_p1, dummy_use_p2);
    v->getNormalised(btype, bunit, bp1, bp2, dummy_use_p1, dummy_use_p2);

	if (int res = atype - btype) return res;
	if (int res = aunit - bunit) return res;
	if (int res = ap1 - bp1) return res;
	return ap2 - bp2;
}

bool GRIB1::equals(const Type& o) const
{
	const GRIB1* v = dynamic_cast<const GRIB1*>(&o);
	if (!v) return false;

	int atype, ap1, ap2, btype, bp1, bp2;
	GRIB1Unit aunit, bunit;
    bool dummy_use_p1, dummy_use_p2;
    getNormalised(atype, aunit, ap1, ap2, dummy_use_p1, dummy_use_p2);
    v->getNormalised(btype, bunit, bp1, bp2, dummy_use_p1, dummy_use_p2);

	return atype == btype && aunit == bunit && ap1 == bp1 && ap2 == bp2;
}

bool GRIB1::get_forecast_step(int& step, bool& is_seconds) const
{
    int timemul;
    is_seconds = get_timeunit_conversion(timemul);

    switch (m_type)
    {
        case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1: // = 0
        case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME: // = 1
        case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2: // = 10
  // statproc = 254
  // CALL gribtr_to_second(unit, p1_g1, p1)
  // p2 = 0
            step = m_p1 * timemul;
            return true;
        case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2: // = 2
  // ELSE IF (tri == 2) THEN ! somewhere between p1 and p2, not good for grib2 standard
  //   statproc = 205
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            step = m_p2 * timemul;
            return true;
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2: // = 3
  // ELSE IF (tri == 3) THEN ! average
  //   statproc = 0
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            step = m_p2 * timemul;
            return true;
        case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2: // = 4
  // ELSE IF (tri == 4) THEN ! accumulation
  //   statproc = 1
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            step = m_p2 * timemul;
            return true;
        case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1: // = 5
  // ELSE IF (tri == 5) THEN ! difference
  //   statproc = 4
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            step = m_p2 * timemul;
            return true;
        case GRIB_TIMERANGE_COSMO_NUDGING: // = 13
  // ELSE IF (tri == 13) THEN ! COSMO-nudging, use a temporary value then normalize
  //   statproc = 206 ! check if 206 is legal!
  //   p1 = 0 ! analysis regardless of p2_g1
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
// ! Timerange 206 (COSMO nudging) is converted to point in time if
// ! interval length is 0, or to a proper timerange if parameter is
// ! recognized as a COSMO statistically processed parameter (and in case
// ! of maximum or minimum the parameter is corrected as well); if
// ! parameter is not recognized, it is converted to instantaneous with a
// ! warning.
//            return 206;
// We cannot do that as we do not know the parameter here
            step = 0;
            return true;
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2://             = 6,
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2://              = 7,
        case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2://           = 51,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2://     = 113,
        case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2://    = 114,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2://       = 115,
        case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2://     = 116,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED://          = 117,
        case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2://    = 118,
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED://            = 119,
        case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2://                 = 123,
        case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2://            = 124,
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY://       = 125,
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS://                  = 128,
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS://             = 129,
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES://                       = 130,
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES://                  = 131
  // call l4f_log(L4F_ERROR,'timerange_g1_to_g2: GRIB1 timerange '//TRIM(to_char(tri)) &
  //  //' cannot be converted to GRIB2.')
  // CALL raise_error()
            return false;

// if (statproc == 254 .and. p2 /= 0 ) then
//   call l4f_log(L4F_WARN,"inconsistence in timerange:254,"//trim(to_char(p1))//","//trim(to_char(p2)))
// end if
    }
    return false;
}

int GRIB1::get_proc_type() const
{
    switch (m_type)
    {
        case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1:
        case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
        case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2:
  // statproc = 254
  // CALL gribtr_to_second(unit, p1_g1, p1)
  // p2 = 0
            return 254;
        case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
  // ELSE IF (tri == 2) THEN ! somewhere between p1 and p2, not good for grib2 standard
  //   statproc = 205
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            return 205;
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
  // ELSE IF (tri == 3) THEN ! average
  //   statproc = 0
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            return 0;
        case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
  // ELSE IF (tri == 4) THEN ! accumulation
  //   statproc = 1
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            return 1;
        case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
  // ELSE IF (tri == 5) THEN ! difference
  //   statproc = 4
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            return 4;
        case GRIB_TIMERANGE_COSMO_NUDGING:
  // ELSE IF (tri == 13) THEN ! COSMO-nudging, use a temporary value then normalize
  //   statproc = 206 ! check if 206 is legal!
  //   p1 = 0 ! analysis regardless of p2_g1
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
// ! Timerange 206 (COSMO nudging) is converted to point in time if
// ! interval length is 0, or to a proper timerange if parameter is
// ! recognized as a COSMO statistically processed parameter (and in case
// ! of maximum or minimum the parameter is corrected as well); if
// ! parameter is not recognized, it is converted to instantaneous with a
// ! warning.
//            return 206;
// We cannot do that as we do not know the parameter here
            return 254;
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2://             = 6,
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2://              = 7,
        case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2://           = 51,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2://     = 113,
        case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2://    = 114,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2://       = 115,
        case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2://     = 116,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED://          = 117,
        case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2://    = 118,
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED://            = 119,
        case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2://                 = 123,
        case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2://            = 124,
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY://       = 125,
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS://                  = 128,
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS://             = 129,
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES://                       = 130,
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES://                  = 131
  // call l4f_log(L4F_ERROR,'timerange_g1_to_g2: GRIB1 timerange '//TRIM(to_char(tri)) &
  //  //' cannot be converted to GRIB2.')
  // CALL raise_error()
            return -1;

// if (statproc == 254 .and. p2 /= 0 ) then
//   call l4f_log(L4F_WARN,"inconsistence in timerange:254,"//trim(to_char(p1))//","//trim(to_char(p2)))
// end if
    }
    return -1;
}

bool GRIB1::get_proc_duration(int& duration, bool& is_seconds) const
{
    int timemul;
    is_seconds = get_timeunit_conversion(timemul);

    switch (m_type)
    {
        case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1:
        case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
        case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2:
  // statproc = 254
  // CALL gribtr_to_second(unit, p1_g1, p1)
  // p2 = 0
            duration = 0;
            return true;
        case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
  // ELSE IF (tri == 2) THEN ! somewhere between p1 and p2, not good for grib2 standard
  //   statproc = 205
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            duration = (m_p2 - m_p1) * timemul;
            return true;
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
  // ELSE IF (tri == 3) THEN ! average
  //   statproc = 0
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            duration = (m_p2 - m_p1) * timemul;
            return true;
        case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
  // ELSE IF (tri == 4) THEN ! accumulation
  //   statproc = 1
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            duration = (m_p2 - m_p1) * timemul;
            return true;
        case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
  // ELSE IF (tri == 5) THEN ! difference
  //   statproc = 4
  //   CALL gribtr_to_second(unit, p2_g1, p1)
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
            duration = (m_p2 - m_p1) * timemul;
            return true;
        case GRIB_TIMERANGE_COSMO_NUDGING:
  // ELSE IF (tri == 13) THEN ! COSMO-nudging, use a temporary value then normalize
  //   statproc = 206 ! check if 206 is legal!
  //   p1 = 0 ! analysis regardless of p2_g1
  //   CALL gribtr_to_second(unit, p2_g1-p1_g1, p2)
// ! Timerange 206 (COSMO nudging) is converted to point in time if
// ! interval length is 0, or to a proper timerange if parameter is
// ! recognized as a COSMO statistically processed parameter (and in case
// ! of maximum or minimum the parameter is corrected as well); if
// ! parameter is not recognized, it is converted to instantaneous with a
// ! warning.
//            return 206;
// We cannot do that as we do not know the parameter here
            duration = 0;
            return true;
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2://             = 6,
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2://              = 7,
        case GRIB_TIMERANGE_CLIMATOLOGICAL_MEAN_OVER_MULTIPLE_YEARS_FOR_P2://           = 51,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_REFTIME_PERIOD_P2://     = 113,
        case GRIB_TIMERANGE_ACCUMULATED_OVER_FORECAST_PERIOD_P1_REFTIME_PERIOD_P2://    = 114,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_OF_PERIOD_P1_AT_INTERVALS_P2://       = 115,
        case GRIB_TIMERANGE_ACCUMULATION_OVER_FORECAST_PERIOD_P1_AT_INTERVALS_P2://     = 116,
        case GRIB_TIMERANGE_AVERAGE_OVER_FORECAST_FIRST_P1_OTHER_P2_REDUCED://          = 117,
        case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2://    = 118,
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_FIRST_P1_OTHER_P2_REDUCED://            = 119,
        case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2://                 = 123,
        case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2://            = 124,
        case GRIB_TIMERANGE_STDDEV_OF_FORECASTS_RESPECT_TO_AVERAGE_OF_TENDENCY://       = 125,
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_ACCUMULATIONS://                  = 128,
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_ACCUMULATIONS://             = 129,
        case GRIB_TIMERANGE_AVERAGE_OF_DAILY_FORECAST_AVERAGES://                       = 130,
        case GRIB_TIMERANGE_AVERAGE_OF_SUCCESSIVE_FORECAST_AVERAGES://                  = 131
  // call l4f_log(L4F_ERROR,'timerange_g1_to_g2: GRIB1 timerange '//TRIM(to_char(tri)) &
  //  //' cannot be converted to GRIB2.')
  // CALL raise_error()
            return false;

// if (statproc == 254 .and. p2 /= 0 ) then
//   call l4f_log(L4F_WARN,"inconsistence in timerange:254,"//trim(to_char(p1))//","//trim(to_char(p2)))
// end if
    }
    return false;
}

GRIB1* GRIB1::clone() const
{
    GRIB1* res = new GRIB1;
    res->m_type = m_type;
    res->m_unit = m_unit;
    res->m_p1 = m_p1;
    res->m_p2 = m_p2;
    return res;
}

unique_ptr<GRIB1> GRIB1::create(unsigned char type, unsigned char unit, unsigned char p1, unsigned char p2)
{
    GRIB1* res = new GRIB1;
    res->m_type = type;
    res->m_unit = unit;
    res->m_p1 = p1;
    res->m_p2 = p2;
    return unique_ptr<GRIB1>(res);
}

bool GRIB1::get_timeunit_conversion(int& timemul) const
{
    bool is_seconds = true;
    timemul = 1;
    switch ((t_enum_GRIB_TIMEUNIT)m_unit)
    {
        case GRIB_TIMEUNIT_UNKNOWN:
            throw_consistency_error("normalising TimeRange", "time unit is UNKNOWN (-1)");
        case GRIB_TIMEUNIT_MINUTE: timemul = 60; break;
        case GRIB_TIMEUNIT_HOUR: timemul = 3600; break;
        case GRIB_TIMEUNIT_DAY: timemul = 3600*24; break;
        case GRIB_TIMEUNIT_MONTH: timemul = 1; is_seconds = false; break;
        case GRIB_TIMEUNIT_YEAR: timemul = 12; is_seconds = false; break;
        case GRIB_TIMEUNIT_DECADE: timemul = 120; is_seconds = false; break;
        case GRIB_TIMEUNIT_NORMAL: timemul = 12*30; is_seconds = false; break;
        case GRIB_TIMEUNIT_CENTURY: timemul = 12*100; is_seconds = false; break;
        case GRIB_TIMEUNIT_HOURS3: timemul = 3600*3; break;
        case GRIB_TIMEUNIT_HOURS6: timemul = 3600*6; break;
        case GRIB_TIMEUNIT_HOURS12: timemul = 3600*12; break;
        case GRIB_TIMEUNIT_SECOND: timemul = 1; break;
        default:
        {
            stringstream ss;
            ss << "cannot normalise TimeRange: time unit is unknown (" << m_unit << ")";
            throw std::runtime_error(ss.str());
        }
    }
    return is_seconds;
}

void GRIB1::getNormalised(int& otype, GRIB1Unit& ounit, int& op1, int& op2, bool& use_op1, bool& use_op2) const
{
    otype = m_type;

    // Make sense of the time range unit
    int timemul;
    if (get_timeunit_conversion(timemul))
        ounit = SECOND;
    else
        ounit = MONTH;

    // Convert p1 and p2
    op1 = 0, op2 = 0;
    use_op1 = use_op2 = false;

    switch ((t_enum_GRIB_TIMERANGE)m_type)
    {
        case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1:
            op1 = m_p1;
            use_op1 = true;
            break;
		case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
			break;
		case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
		case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
		case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2:
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2:
            op1 = m_p1; op2 = m_p2;
            use_op1 = use_op2 = true;
            break;
        case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2:
            op1 = m_p1 << 8 | m_p2;
            use_op1 = true;
            use_op2 = false;
            break;
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
            op1 = m_p1;
            op2 = m_p2;
            use_op1 = use_op2 = true;
            break;
		case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2:
		case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2:
        case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2:
            op2 = m_p2;
            use_op2 = true;
            break;
        default:
            // Fallback for unknown time range types
            op1 = m_p1;
            op2 = m_p2;
            use_op1 = use_op2 = true;
            break;
    }

	op1 *= timemul;
	op2 *= timemul;
}

void GRIB1::arg_significance(unsigned type, bool& use_p1, bool& use_p2)
{
    use_p1 = use_p2 = false;

    switch ((t_enum_GRIB_TIMERANGE)type)
    {
        case GRIB_TIMERANGE_FORECAST_AT_REFTIME_PLUS_P1:
            use_p1 = true;
            break;
        case GRIB_TIMERANGE_ANALYSIS_AT_REFTIME:
            break;
        case GRIB_TIMERANGE_VALID_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
        case GRIB_TIMERANGE_ACCUMULATED_INTERVAL_REFTIME_PLUS_P1_REFTIME_PLUS_P2:
        case GRIB_TIMERANGE_DIFFERENCE_REFTIME_PLUS_P2_REFTIME_PLUS_P1:
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_MINUS_P2:
        case GRIB_TIMERANGE_AVERAGE_IN_REFTIME_MINUS_P1_REFTIME_PLUS_P2:
            use_p1 = use_p2 = true;
            break;
        case GRIB_TIMERANGE_VALID_AT_REFTIME_PLUS_P1P2:
            use_p1 = true;
            use_p2 = true;
            break;
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
            use_p1 = use_p2 = true;
            break;
        case GRIB_TIMERANGE_VARIANCE_OF_ANALYSES_WITH_REFERENCE_TIME_INTERVALS_P2:
        case GRIB_TIMERANGE_AVERAGE_OVER_ANALYSES_AT_INTERVALS_OF_P2:
        case GRIB_TIMERANGE_ACCUMULATION_OVER_ANALYSES_AT_INTERVALS_OF_P2:
            use_p2 = true;
            break;
        default:
            // Fallback for unknown time range types
            use_p1 = use_p2 = true;
            break;
    }
}

Timerange::Style GRIB2::style() const { return Timerange::GRIB2; }

void GRIB2::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Timerange::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_type, 1); enc.add_unsigned(m_unit, 1); enc.add_signed(m_p1, 4); enc.add_signed(m_p2, 4);
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

void GRIB2::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Timerange::serialiseLocal(e, f);
    e.add("ty", (int)m_type);
    e.add("un", (int)m_unit);
    e.add("p1", (int)m_p1);
    e.add("p2", (int)m_p2);
}
unique_ptr<GRIB2> GRIB2::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;
    return GRIB2::create(
            val["ty"].want_int("parsing GRIB2 timerange ty"),
            val["un"].want_int("parsing GRIB2 timerange un"),
            val["p1"].want_int("parsing GRIB2 timerange p1"),
            val["p2"].want_int("parsing GRIB2 timerange p2"));
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
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a GRIB2 Timerange, but is a ") + typeid(&o).name() + " instead");

	// TODO: normalise the time units if needed
	if (int res = m_type - v->m_type) return res;
	if (int res = m_unit - v->m_unit) return res;
	if (int res = m_p1 - v->m_p1) return res;
	return m_p2 - v->m_p2;
}

bool GRIB2::equals(const Type& o) const
{
	const GRIB2* v = dynamic_cast<const GRIB2*>(&o);
	if (!v) return false;
	// TODO: normalise the time units if needed
	return m_type == v->m_type && m_unit == v->m_unit && m_p1 == v->m_p1 && m_p2 == v->m_p2;
}

bool GRIB2::get_forecast_step(int& step, bool& is_seconds) const
{
    return false;
}

int GRIB2::get_proc_type() const
{
    return -1;
}

bool GRIB2::get_proc_duration(int& duration, bool& is_seconds) const
{
    return false;
}

GRIB2* GRIB2::clone() const
{
    GRIB2* res = new GRIB2;
    res->m_type = m_type;
    res->m_unit = m_unit;
    res->m_p1 = m_p1;
    res->m_p2 = m_p2;
    return res;
}

unique_ptr<GRIB2> GRIB2::create(unsigned char type, unsigned char unit, signed long p1, signed long p2)
{
    GRIB2* res = new GRIB2;
    res->m_type = type;
    res->m_unit = unit;
    res->m_p1 = p1;
    res->m_p2 = p2;
    return unique_ptr<GRIB2>(res);
}

Timerange::Style Timedef::style() const { return Timerange::TIMEDEF; }

void Timedef::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Timerange::encodeWithoutEnvelope(enc);

    enc.add_unsigned((unsigned)m_step_unit, 1);
    if (m_step_unit != 255)
        enc.add_varint(m_step_len);

    enc.add_unsigned(m_stat_type, 1);
    if (m_stat_type != 255)
    {
        enc.add_unsigned((unsigned)m_stat_unit, 1);
        if (m_stat_unit != 255)
            enc.add_varint(m_stat_len);
    }
}

std::ostream& Timedef::writeToOstream(std::ostream& o) const
{
    utils::SaveIOState sios(o);

    o << formatStyle(style()) << "("
      << setfill('0') << internal;

    timeunit_output(m_step_unit, m_step_len, o);

    if (m_stat_type != 255)
    {
        o << ", " << (unsigned)m_stat_type;
        if (m_stat_unit != UNIT_MISSING)
        {
            o << ", ";
            timeunit_output(m_stat_unit, m_stat_len, o);
        }
    }
    o << ")";

    return o;
}

unique_ptr<Timedef> Timedef::createFromYaml(const std::string& encoded)
{
    const char* str = encoded.c_str();

    TimedefUnit step_unit;
    uint32_t step_len;
    if (!timeunit_parse(str, step_unit, step_len))
        throw_consistency_error("parsing Timerange", "cannot parse time range step");

    int stat_type;
    TimedefUnit stat_unit = UNIT_MISSING;
    uint32_t stat_len = 0;

    if (!*str)
        stat_type = 255;
    else
    {
        stat_type = getNumber(str, "type of statistical processing");
        if (*str && !timeunit_parse(str, stat_unit, stat_len))
            throw_consistency_error("parsing Timerange", "cannot parse length of statistical processing");
    }

    return timerange::Timedef::create(step_len, step_unit, stat_type, stat_len, stat_unit);
}


void Timedef::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Timerange::serialiseLocal(e, f);
    e.add("sl", (int)m_step_len);
    e.add("su", (int)m_step_unit);
    if (m_stat_type != 255)
    {
        e.add("pt", (int)m_stat_type);
        if (m_stat_unit != UNIT_MISSING)
        {
            e.add("pl", (int)m_stat_len);
            e.add("pu", (int)m_stat_unit);
        }
    }
}
unique_ptr<Timedef> Timedef::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;

    uint32_t step_len = val["sl"].want_int("parsing Timedef forecast step length");
    TimedefUnit step_unit = (TimedefUnit)val["su"].want_int("parsing Timedef forecast step units");

    int stat_type = 255;
    uint32_t stat_len = 0;
    TimedefUnit stat_unit = timerange::UNIT_MISSING;

    const Node& pt = val["pt"];
    if (pt.is_int())
    {
        stat_type = pt.get_int();

        const Node& pu = val["pu"];
        if (pu.is_int())
        {
            stat_unit = (TimedefUnit)pu.get_int();
            stat_len = val["pl"].want_int("parsing Timedef length of interval of statistical processing");
        }
    }

    return timerange::Timedef::create(step_len, step_unit, stat_type, stat_len, stat_unit);
}

std::string Timedef::exactQuery() const
{
    stringstream o;
    o << formatStyle(style()) << ",";
    if (m_step_unit == 255)
        o << "-,";
    else
        o << m_step_len << timeunit_suffix(m_step_unit) << ",";
    if (m_stat_type == 255)
        o << "-";
    else
    {
        o << (unsigned)m_stat_type << ",";
        if (m_stat_unit == 255)
            o << "-";
        else
            o << m_stat_len << timeunit_suffix(m_stat_unit);
    }
    return o.str();
}

const char* Timedef::lua_type_name() const { return "arki.types.timerange.grib2"; }

bool Timedef::lua_lookup(lua_State* L, const std::string& name) const
{
    if (name == "step_unit")
        lua_pushnumber(L, m_step_unit);
    else if (name == "step_len")
        lua_pushnumber(L, m_step_len);
    else if (name == "stat_type")
        lua_pushnumber(L, m_stat_type);
    else if (name == "stat_unit")
        lua_pushnumber(L, m_stat_unit);
    else if (name == "stat_len")
        lua_pushnumber(L, m_stat_len);
    else
        return Timerange::lua_lookup(L, name);
    return true;
}

int Timedef::compare_local(const Timerange& o) const
{
    // We should be the same kind, so upcast
    const Timedef* v = dynamic_cast<const Timedef*>(&o);
    if (!v)
        throw_consistency_error(
                "comparing metadata types",
                string("second element claims to be a Timedef Timerange, but is a ") + typeid(&o).name() + " instead");

    // Normalise step time units and compare steps
    if (m_step_unit == 255 && v->m_step_unit != 255) return -1;
    if (m_step_unit != 255 && v->m_step_unit == 255) return 1;
    if (m_step_unit != 255)
    {
        int timemul1;
        bool is1 = timeunit_conversion(m_step_unit, timemul1);
        int timemul2;
        bool is2 = timeunit_conversion(v->m_step_unit, timemul2);
        if (is1 && !is2) return -1;
        if (is2 && !is1) return 1;
        if (int res = m_step_len * timemul1 - v->m_step_len * timemul2) return res;
    }

    // Compare type of statistical processing
    if (int res = m_stat_type - v->m_stat_type) return res;

    // Normalise stat units and compare stat processing length
    if (m_stat_unit == 255 && v->m_stat_unit != 255) return -1;
    if (m_stat_unit != 255 && v->m_stat_unit == 255) return 1;
    if (m_stat_unit != 255)
    {
        int timemul1;
        bool is1 = timeunit_conversion(m_stat_unit, timemul1);
        int timemul2;
        bool is2 = timeunit_conversion(v->m_stat_unit, timemul2);
        if (is1 && !is2) return -1;
        if (is2 && !is1) return 1;
        if (int res = m_stat_len * timemul1 - v->m_stat_len * timemul2) return res;
    }

    return 0;
}

bool Timedef::equals(const Type& o) const
{
    const Timedef* v = dynamic_cast<const Timedef*>(&o);
    if (!v) return false;

    // Normalise step time units and compare steps
    if (m_step_unit == UNIT_MISSING && v->m_step_unit != UNIT_MISSING) return false;
    if (m_step_unit != UNIT_MISSING && v->m_step_unit == UNIT_MISSING) return false;
    if (m_step_unit != UNIT_MISSING)
    {
        int timemul1;
        bool is1 = timeunit_conversion(m_step_unit, timemul1);
        int timemul2;
        bool is2 = timeunit_conversion(v->m_step_unit, timemul2);
        if (is1 != is2) return false;
        if (m_step_len * timemul1 != v->m_step_len * timemul2) return false;
    }

    // Compare type of statistical processing
    if (m_stat_type != v->m_stat_type) return false;

    // Normalise stat units and compare stat processing length
    if (m_stat_unit == UNIT_MISSING && v->m_stat_unit != UNIT_MISSING) return false;
    if (m_stat_unit != UNIT_MISSING && v->m_stat_unit == UNIT_MISSING) return false;
    if (m_stat_unit != UNIT_MISSING)
    {
        int timemul1;
        bool is1 = timeunit_conversion(m_stat_unit, timemul1);
        int timemul2;
        bool is2 = timeunit_conversion(v->m_stat_unit, timemul2);
        if (is1 != is2) return false;
        if (m_stat_len * timemul1 != v->m_stat_len * timemul2) return false;
    }

    return true;
}

unique_ptr<reftime::Position> Timedef::validity_time_to_emission_time(const reftime::Position& src) const
{
    // Compute our forecast step in seconds
    int secs;
    bool is_secs;
    get_forecast_step(secs, is_secs);
    if (!is_secs)
        throw_consistency_error("converting validity time to emission time", "timedef has a step that cannot be converted to seconds");

    // Compute the new time
    Time new_time(src.time);
    new_time.se -= secs;
    new_time.normalise();

    // Return it
    return unique_ptr<reftime::Position>(new reftime::Position(new_time));
}

bool Timedef::get_forecast_step(int& step, bool& is_seconds) const
{
    if (m_step_unit == UNIT_MISSING) return false;

    int timemul;
    is_seconds = timeunit_conversion(m_step_unit, timemul);
    step = m_step_len * timemul;
    return true;
}

int Timedef::get_proc_type() const
{
    if (m_stat_type == 255)
        return -1;
    return m_stat_type;
}

bool Timedef::get_proc_duration(int& duration, bool& is_seconds) const
{
    if (m_stat_type == 255 || m_stat_unit == UNIT_MISSING) return false;

    int timemul;
    is_seconds = timeunit_conversion(m_stat_unit, timemul);
    duration = m_stat_len * timemul;
    return true;
}

Timedef* Timedef::clone() const
{
    Timedef* res = new Timedef;
    res->m_step_len = m_step_len;
    res->m_step_unit = m_step_unit;
    res->m_stat_type = m_stat_type;
    res->m_stat_len = m_stat_len;
    res->m_stat_unit = m_stat_unit;
    return res;
}

unique_ptr<Timedef> Timedef::create(uint32_t step_len, TimedefUnit step_unit)
{
    Timedef* res = new Timedef;
    res->m_step_len = step_len;
    res->m_step_unit = step_unit == UNIT_MISSING ? UNIT_MISSING : step_len == 0 ? UNIT_SECOND : step_unit;
    res->m_stat_type = 255;
    res->m_stat_len = 0;
    res->m_stat_unit = UNIT_MISSING;
    return unique_ptr<Timedef>(res);
}

unique_ptr<Timedef> Timedef::create(uint32_t step_len, TimedefUnit step_unit,
                          uint8_t stat_type, uint32_t stat_len, TimedefUnit stat_unit)
{
    Timedef* res = new Timedef;
    res->m_step_len = step_len;
    res->m_step_unit = step_unit == UNIT_MISSING ? UNIT_MISSING : step_len == 0 ? UNIT_SECOND : step_unit;
    res->m_stat_type = stat_type;
    res->m_stat_len = stat_len;
    res->m_stat_unit = stat_unit == UNIT_MISSING ? UNIT_MISSING : stat_len == 0 ? UNIT_SECOND : stat_unit;
    return unique_ptr<Timedef>(res);
}

bool Timedef::timeunit_conversion(TimedefUnit unit, int& timemul)
{
    bool is_seconds = true;
    timemul = 1;
    switch (unit)
    {
        case UNIT_MINUTE: timemul = 60; break;
        case UNIT_HOUR: timemul = 3600; break;
        case UNIT_DAY: timemul = 3600*24; break;
        case UNIT_MONTH: timemul = 1; is_seconds = false; break;
        case UNIT_YEAR: timemul = 12; is_seconds = false; break;
        case UNIT_DECADE: timemul = 120; is_seconds = false; break;
        case UNIT_NORMAL: timemul = 12*30; is_seconds = false; break;
        case UNIT_CENTURY: timemul = 12*100; is_seconds = false; break;
        case UNIT_3HOURS: timemul = 3600*3; break;
        case UNIT_6HOURS: timemul = 3600*6; break;
        case UNIT_12HOURS: timemul = 3600*12; break;
        case UNIT_SECOND: timemul = 1; break;
        case UNIT_MISSING:
            throw_consistency_error("normalising time", "time unit is missing (255)");
        default:
        {
            stringstream ss;
            ss << "cannot normalise time: time unit is unknown (" << (int)unit << ")";
            throw std::runtime_error(ss.str());
        }
    }
    return is_seconds;
}

void Timedef::timeunit_output(TimedefUnit unit, uint32_t val, std::ostream& out)
{
    out << val << timeunit_suffix(unit);
}

bool Timedef::timeunit_parse_suffix(const char*& str, TimedefUnit& unit)
{
    if (!*str) return false;

    switch (*str)
    {
        case 'm':
            ++str;
            switch (*str)
            {
                case 'o': ++str; unit = UNIT_MONTH; break;
                default: unit = UNIT_MINUTE; break;
            }
            return true;
        case 'h':
            ++str;
            switch (*str)
            {
                case '3': ++str; unit = UNIT_3HOURS; break;
                case '6': ++str; unit = UNIT_6HOURS; break;
                case '1':
                    if (*(str+1) && *(str+1) == '2')
                    {
                        str += 2;
                        unit = UNIT_12HOURS;
                        break;
                    }
                    // Fallover to the next case
                default: unit = UNIT_HOUR; break;
            }
            return true;
        case 'd':
            ++str;
            switch (*str)
            {
                case 'e': ++str; unit = UNIT_DECADE; break;
                default: unit = UNIT_DAY; break;
            }
            return true;
        case 'y': ++str; unit = UNIT_YEAR; return true;
        case 'n':
            if (*(str+1) && *(str+1) == 'o')
            {
                str += 2;
                unit = UNIT_NORMAL;
                return true;
            }
            return false;
        case 'c':
            if (*(str+1) && *(str+1) == 'e')
            {
                str += 2;
                unit = UNIT_CENTURY;
                return true;
            }
            return false;
        case 's': ++str; unit = UNIT_SECOND; return true;
    }
    return false;
}

bool Timedef::timeunit_parse(const char*& str, TimedefUnit& unit, uint32_t& val)
{
    if (!*str)
        return false;

    skipCommasAndSpaces(str);

    if (str[0] == '-')
    {
        unit = UNIT_MISSING;
        val = 0;

        skipCommasAndSpaces(str);
        return true;
    }

    char* endptr;
    val = strtoul(str, &endptr, 10);
    if (endptr == str)
        return false;

    str = endptr;

    if (!timeunit_parse_suffix(str, unit))
    {
        if (val == 0)
        {
            unit = UNIT_SECOND;
            return true;
        }
        return false;
    }

    skipCommasAndSpaces(str);
    return true;
}

const char* Timedef::timeunit_suffix(TimedefUnit unit)
{
    switch (unit)
    {
        case UNIT_MINUTE: return "m";
        case UNIT_HOUR: return "h";
        case UNIT_DAY: return "d";
        case UNIT_MONTH: return "mo";
        case UNIT_YEAR: return "y";
        case UNIT_DECADE: return "de";
        case UNIT_NORMAL: return "no";
        case UNIT_CENTURY: return "ce";
        case UNIT_3HOURS: return "h3";
        case UNIT_6HOURS: return "h6";
        case UNIT_12HOURS: return "h12";
        case UNIT_SECOND: return "s";
        case UNIT_MISSING:
            throw_consistency_error("finding time unit suffix", "time unit is missing (255)");
        default:
        {
            stringstream ss;
            ss << "cannot find find time unit suffix: time unit is unknown (" << (int)unit << ")";
            throw std::runtime_error(ss.str());
        }
    }
}


bool BUFR::is_seconds() const
{
	switch ((t_enum_GRIB_TIMEUNIT)m_unit)
	{
		case GRIB_TIMEUNIT_UNKNOWN:
			throw_consistency_error("normalising TimeRange", "time unit is UNKNOWN (-1)");
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
        {
            stringstream ss;
            ss << "cannot normalise TimeRange: time unit is unknown (" << m_unit << ")";
            throw std::runtime_error(ss.str());
        }
    }
}

unsigned BUFR::seconds() const
{
	if (m_value == 0) return 0;

	switch ((t_enum_GRIB_TIMEUNIT)m_unit)
	{
		case GRIB_TIMEUNIT_UNKNOWN:
			throw_consistency_error("normalising TimeRange", "time unit is UNKNOWN (-1)");
		case GRIB_TIMEUNIT_MINUTE: return m_value * 60;
		case GRIB_TIMEUNIT_HOUR: return m_value * 3600;
		case GRIB_TIMEUNIT_DAY: return m_value * 3600*24;
		case GRIB_TIMEUNIT_HOURS3: return m_value * 3600*3;
		case GRIB_TIMEUNIT_HOURS6: return m_value * 3600*6;
		case GRIB_TIMEUNIT_HOURS12: return m_value * 3600*12;
		case GRIB_TIMEUNIT_SECOND: return m_value * 1;
		default:
        {
            stringstream ss;
            ss << "cannot normalise TimeRange: time unit (" << m_unit << ") does not convert to seconds";
            throw std::runtime_error(ss.str());
        }
    }
}

unsigned BUFR::months() const
{
	if (m_value == 0) return 0;

	switch ((t_enum_GRIB_TIMEUNIT)m_unit)
	{
		case GRIB_TIMEUNIT_UNKNOWN:
			throw_consistency_error("normalising TimeRange", "time unit is UNKNOWN (-1)");
		case GRIB_TIMEUNIT_MONTH: return m_value * 1;
		case GRIB_TIMEUNIT_YEAR: return m_value * 12;
		case GRIB_TIMEUNIT_DECADE: return m_value * 120;
		case GRIB_TIMEUNIT_NORMAL: return m_value * 12*30;
		case GRIB_TIMEUNIT_CENTURY: return m_value * 12*100;
		default:
        {
            stringstream ss;
            ss << "cannot normalise TimeRange: time unit (" << m_unit << ") does not convert to months";
            throw std::runtime_error(ss.str());
        }
    }
}

Timerange::Style BUFR::style() const { return Timerange::BUFR; }

void BUFR::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    Timerange::encodeWithoutEnvelope(enc);
    enc.add_unsigned(m_unit, 1);
    enc.add_varint(m_value);
}

std::ostream& BUFR::writeToOstream(std::ostream& o) const
{
	utils::SaveIOState sios(o);
	string suffix = formatTimeUnit((t_enum_GRIB_TIMEUNIT)m_unit);
	o << formatStyle(style()) << "(";
	if (m_value != 0) o << m_value << suffix;
	return o << ")";
}

void BUFR::serialiseLocal(Emitter& e, const Formatter* f) const
{
    Timerange::serialiseLocal(e, f);
    e.add("un", (int)m_unit);
    e.add("va", (int)m_value);
}
unique_ptr<BUFR> BUFR::decodeMapping(const emitter::memory::Mapping& val)
{
    using namespace emitter::memory;
    return BUFR::create(
            val["va"].want_int("parsing BUFR timerange value"),
            val["un"].want_int("parsing BUFR timerange un"));
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
		throw_consistency_error(
			"comparing metadata types",
			string("second element claims to be a BUFR Timerange, but is a ") + typeid(&o).name() + " instead");
	if (int res = (is_seconds() ? 0 : 1) - (v->is_seconds() ? 0 : 1)) return res;
	if (is_seconds())
		return seconds() - v->seconds();
	else
		return months() - v->months();
}

bool BUFR::equals(const Type& o) const
{
	const BUFR* v = dynamic_cast<const BUFR*>(&o);
	if (!v) return false;
	if (is_seconds() != v->is_seconds()) return false;
	if (is_seconds())
		return seconds() == v->seconds();
	else
		return months() == v->months();
}

bool BUFR::get_forecast_step(int& step, bool& is_seconds) const
{
    if ((is_seconds = this->is_seconds()))
        step = seconds();
    else
        step = seconds();
    return true;
}

int BUFR::get_proc_type() const
{
    return -1;
}

bool BUFR::get_proc_duration(int& duration, bool& is_seconds) const
{
    return false;
}

BUFR* BUFR::clone() const
{
    BUFR* res = new BUFR;
    res->m_value = m_value;
    res->m_unit = m_unit;
    return res;
}
unique_ptr<BUFR> BUFR::create(unsigned value, unsigned char unit)
{
    BUFR* res = new BUFR;
    res->m_value = value;
    res->m_unit = unit;
    return unique_ptr<BUFR>(res);
}

}

void Timerange::init()
{
    MetadataType::register_type<Timerange>();
}

}
}

#include <arki/types.tcc>
