#include "values.h"
#include "arki/libconfig.h"
#include "arki/exceptions.h"
#include "arki/core/binary.h"
#include "arki/utils/string.h"
#include "arki/structured/emitter.h"
#include "arki/structured/memory.h"
#include <memory>
#include <cstdlib>
#include <cctype>
#include <cstdio>

#ifdef HAVE_LUA
extern "C" {
#include <lauxlib.h>
#include <lualib.h>
}
#endif

using namespace std;
using namespace arki::utils;

#if 0
static void dump(const char* name, const std::string& str)
{
	fprintf(stderr, "%s ", name);
	for (string::const_iterator i = str.begin(); i != str.end(); ++i)
	{
		fprintf(stderr, "%02x ", (int)(unsigned char)*i);
	}
	fprintf(stderr, "\n");
}
static void dump(const char* name, const unsigned char* str, int len)
{
	fprintf(stderr, "%s ", name);
	for (int i = 0; i < len; ++i)
	{
		fprintf(stderr, "%02x ", str[i]);
	}
	fprintf(stderr, "\n");
}
#endif

static bool parsesAsNumber(const std::string& str, int& parsed)
{
	if (str.empty())
		return false;
	const char* s = str.c_str();
	char* e;
	long int ival = strtol(s, &e, 0);
	for ( ; e-s < (signed)str.size() && *e && isspace(*e); ++e)
		;
	if (e-s != (signed)str.size())
		return false;
	parsed = ival;
	return true;
}

static bool needsQuoting(const std::string& str)
{
	// Empty strings don't need quoting
	if (str.empty())
		return false;
	
	// String starting with spaces or double quotes, need quoting
	if (isspace(str[0]) || str[0] == '"')
		return true;
	
	// String ending with spaces or double quotes, need quoting
	if (isspace(str[str.size() - 1]) || str[str.size() - 1] == '"')
		return true;

	// Strings containing nulls need quoting
	if (str.find('\0', 0) != string::npos)
		return true;

	// Otherwise, no quoting is neeed as decoding should be unambiguous
	return false;
}

static inline size_t skipSpaces(const std::string& str, size_t cur)
{
	while (cur < str.size() && isspace(str[cur]))
		++cur;
	return cur;
}


namespace arki {
namespace types {
namespace values {

// Main encoding type constants (fit 2 bits)

// 6bit signed
static const int ENC_SINT6 = 0;
// Number.  Type is given in the next 6 bits.
static const int ENC_NUMBER = 1;
// String whose length can be encoded in 6 bits unsigned (i.e. max 64 characters)
static const int ENC_NAME = 2;
// Extension flag.  Type is given in the next 6 bits.
static const int ENC_EXTENDED = 3;

// Number encoding constants (next 2 bits after the main encoding type in the
// case of ENC_NUMBER)

static const int ENC_NUM_INTEGER = 0;	// Sign in the next bit.  Number of bytes in the next 3 bits.
static const int ENC_NUM_FLOAT = 1;		// Floating point, type in the next 4 bits.
static const int ENC_NUM_UNUSED = 2;	// Unused.  Leave to 0.
static const int ENC_NUM_EXTENDED = 3;	// Extension flag.  Type is given in the next 4 bits.


/**
 * Implementation of Value methods common to all types
 */
template<typename TYPE>
class Common : public Value
{
protected:
	TYPE m_val;

public:
	Common(const TYPE& val) : m_val(val) {}

    bool operator==(const Value& v) const override
    {
		const Common<TYPE>* vi = dynamic_cast<const Common<TYPE>*>(&v);
		if (vi == 0)
			return false;
		return m_val == vi->m_val;
    }
    bool operator<(const Value& v) const override
    {
		const Common<TYPE>* vi = dynamic_cast<const Common<TYPE>*>(&v);
		if (vi == 0)
			return false;
		return m_val < vi->m_val;
    }
    std::string toString() const override
    {
        stringstream ss;
        ss << m_val;
        return ss.str();
    }
};

struct Integer : public Common<int>
{
	Integer(const int& val) : Common<int>(val) {}

    int compare(const Value& v) const override
    {
		if (const Integer* v1 = dynamic_cast< const Integer* >(&v))
			return m_val - v1->m_val;
		else
			return sortKey() - v.sortKey();
	}

    int sortKey() const override { return 1; }

    int toInt() const { return m_val; }

    void encode(core::BinaryEncoder& enc) const override
    {
        if (m_val >= -32 && m_val < 31)
        {
            // If it's a small one, encode in the remaining 6 bits
            uint8_t encoded = { ENC_SINT6 << 6 };
            if (m_val < 0)
            {
                encoded |= ((~(-m_val) + 1) & 0x3f);
            } else {
                encoded |= (m_val & 0x3f);
            }
            enc.add_raw(&encoded, 1u);
        }
        else 
        {
            // Else, encode as an integer Number

            // Type
            uint8_t type = (ENC_NUMBER << 6) | (ENC_NUM_INTEGER << 4);
            // Value to encode
            unsigned int val;
			if (m_val < 0)
			{
				// Sign bit
				type |= 0x8;
				val = -m_val;
			}
			else
				val = m_val;
			// Number of bytes
			unsigned nbytes;
			// TODO: add bits for 64 bits here if it's ever needed
			if (val & 0xff000000)
				nbytes = 4;
			else if (val & 0x00ff0000)
				nbytes = 3;
			else if (val & 0x0000ff00)
				nbytes = 2;
			else if (val & 0x000000ff)
				nbytes = 1;
            else
                throw std::runtime_error("cannot encode integer number: value " + to_string(val) + " is too large to be encoded");

            type |= (nbytes-1);
            enc.add_raw(&type, 1u);
            enc.add_unsigned(val, nbytes);
        }
    }

    static Integer* parse(const std::string& str);

    void serialise(structured::Emitter& e) const override
    {
        e.add_int(m_val);
    }

    Value* clone() const override { return new Integer(m_val); }
};

Integer* Integer::parse(const std::string& str)
{
	return new Integer(atoi(str.c_str()));
}


struct String : public Common<std::string>
{
	String(const std::string& val) : Common<std::string>(val) {}

    int sortKey() const override { return 2; }

    int compare(const Value& v) const override
    {
		if (const String* v1 = dynamic_cast< const String* >(&v))
		{
			if (m_val < v1->m_val) return -1;
			if (m_val > v1->m_val) return 1;
			return 0;
		}
		else
			return sortKey() - v.sortKey();
    }

    void encode(core::BinaryEncoder& enc) const override
    {
        if (m_val.size() < 64)
        {
            uint8_t type = ENC_NAME << 6;
            type |= m_val.size() & 0x3f;
            enc.add_raw(&type, 1u);
            enc.add_raw(m_val);
        }
        else
            // TODO: if needed, here we implement another string encoding type
            throw_consistency_error("encoding short string", "string '"+m_val+"' is too long: the maximum length is 63 characters, but the string is " + to_string(m_val.size()) + " characters long");
    }

    std::string toString() const override
    {
		int idummy;

		if (parsesAsNumber(m_val, idummy) || needsQuoting(m_val))
		{
			// If it is surrounded by double quotes or it parses as a number, we need to escape it
			return "\"" + str::encode_cstring(m_val) + "\"";
		} else {
			// Else, we can use the value as it is
			return m_val;
		}
	}

    void serialise(structured::Emitter& e) const override
    {
        e.add_string(m_val);
    }

    Value* clone() const override { return new String(m_val); }
};

Value* Value::decode(core::BinaryDecoder& dec)
{
    uint8_t lead = dec.pop_byte("valuebag value type");
    switch ((lead >> 6) & 0x3)
    {
        case ENC_SINT6:
            if (lead & 0x20)
                return new Integer(-((~(lead-1)) & 0x3f));
            else
                return new Integer(lead & 0x3f);
        case ENC_NUMBER: {
            switch ((lead >> 4) & 0x3)
            {
                case ENC_NUM_INTEGER: {
                    // Sign in the next bit.  Number of bytes in the next 3 bits.
                    unsigned nbytes = (lead & 0x7) + 1;
                    unsigned val = dec.pop_uint(nbytes, "integer number value");
                    return new Integer((lead & 0x8) ? -val : val);
                }
                case ENC_NUM_FLOAT:
                    throw std::runtime_error("cannot decode value: the number value to decode is a floating point number, but decoding floating point numbers is not currently implemented");
                case ENC_NUM_UNUSED:
                    throw std::runtime_error("cannot decode value: the number value to decode has an unknown type");
                case ENC_NUM_EXTENDED:
                    throw std::runtime_error("cannot decode value: the number value to decode has an extended type, but no extended type is currently implemented");
                default:
                    throw std::runtime_error("cannot decode value: control flow should never reach here (" __FILE__ ":" + to_string(__LINE__) + "), but the compiler cannot easily know it.  This is here to silence a compiler warning.");
            }
        }
        case ENC_NAME: {
            unsigned size = lead & 0x3f;
            return new String(dec.pop_string(size, "valuebag string value"));
        }
        case ENC_EXTENDED:
            throw std::runtime_error("cannot decode value: the encoded value has an extended type, but no extended type is currently implemented");
        default:
            throw std::runtime_error("cannot decode value: control flow should never reach here (" __FILE__ ":" + to_string(__LINE__) + "), but the compiler cannot easily know it.  This is here to silence a compiler warning.");
    }
}

Value* Value::parse(const std::string& str)
{
	size_t dummy;
	return Value::parse(str, dummy);
}

Value* Value::parse(const std::string& str, size_t& lenParsed)
{
    size_t begin = skipSpaces(str, 0);

    // Handle the empty string
    if (begin == str.size())
    {
        lenParsed = begin;
        return new String(string());
    }

	// Handle the quoted string
	if (str[begin] == '"')
	{
		// Skip the first double quote
		++begin;

		// Unescape the string
		size_t parsed;
		string res = str::decode_cstring(str.substr(begin), parsed);

        lenParsed = skipSpaces(str, begin + parsed);
        return new String(res);
    }

	// No quoted string, so we can terminate the token at the next space, ',' or ';'
	size_t end = begin;
	while (end != str.size() && !isspace(str[end]) && str[end] != ',' && str[end] != ';')
		++end;
	string res = str.substr(begin, end-begin);
	lenParsed = skipSpaces(str, end);

    // If it can be parsed as a number, with maybe leading and trailing
    // spaces, return the number
    int val;
    if (parsesAsNumber(res, val))
        return new Integer(val);

    // Else return the string
    return new String(res);
}

Value* Value::create_integer(int val) { return new Integer(val); }
Value* Value::create_string(const std::string& val) { return new String(val); }

}


ValueBag::ValueBag() {}

ValueBag::~ValueBag()
{
	// Deallocate all the Value pointers
	for (iterator i = begin(); i != end(); ++i)
		if (i->second)
			delete i->second;
}

ValueBag::ValueBag(const ValueBag& vb)
{
	iterator inspos = begin();
	for (ValueBag::const_iterator i = vb.begin(); i != vb.end(); ++i)
		inspos = insert(inspos, make_pair(i->first, i->second->clone()));
}

ValueBag& ValueBag::operator=(const ValueBag& vb)
{
	// Handle the case a=a
	if (this == &vb)
		return *this;

	clear();
	
	// Fill up again with the new values
	iterator inspos = begin();
	for (ValueBag::const_iterator i = vb.begin(); i != vb.end(); ++i)
		inspos = insert(inspos, make_pair(i->first, i->second->clone()));
	return *this;
}

bool ValueBag::operator<(const ValueBag& vb) const
{
	const_iterator a = begin();
	const_iterator b = vb.begin();
	for ( ; a != end() && b != vb.end(); ++a, ++b)
	{
		if (a->first < b->first)
			return true;
		if (a->first != b->first)
			return false;
		if (*a->second < *b->second)
			return true;
		if (*a->second != *b->second)
			return false;
	}
	if (a == end() && b == vb.end())
		return false;
	if (a == end())
		return true;
	return false;
}

int ValueBag::compare(const ValueBag& vb) const
{
	const_iterator a = begin();
	const_iterator b = vb.begin();
	for ( ; a != end() && b != vb.end(); ++a, ++b)
	{
		if (a->first < b->first)
			return -1;
		if (a->first > b->first)
			return 1;
		if (int res = a->second->compare(*b->second)) return res;
	}
	if (a == end() && b == vb.end())
		return 0;
	if (a == end())
		return -1;
	return 1;
}

bool ValueBag::contains(const ValueBag& vb) const
{
	// Both a and b are sorted, so we can iterate them linearly together

	ValueBag::const_iterator a = begin();
	ValueBag::const_iterator b = vb.begin();

	while (a != end())
	{
		// Nothing else wanted anymore
		if (b == vb.end())
			return true;
		if (a->first < b->first)
			// This value is not in the match expression
			++a;
		else if (b->first < a->first)
			// This value is wanted but we don't have it
			return false;
		else if (*a->second != *b->second)
			// Same key, check if the value is the same
			return false;
		else
		{
			// If also the value is the same, move on to the next item
			++a;
			++b;
		}
	}
	// We got to the end of a.  If there are still things in b, we don't
	// match.  If we are also to the end of b, then we matched everything
	return b == vb.end();
}

bool ValueBag::operator==(const ValueBag& vb) const
{
	const_iterator a = begin();
	const_iterator b = vb.begin();
	for ( ; a != end() && b != vb.end(); ++a, ++b)
		if (a->first != b->first || *a->second != *b->second)
			return false;
	return a == end() && b == vb.end();
}

void ValueBag::clear()
{
	// Deallocate all the Value pointers
	for (iterator i = begin(); i != end(); ++i)
		if (i->second)
			delete i->second;

    // Empty the map
    map<string, values::Value*>::clear();
}

const values::Value* ValueBag::get(const std::string& key) const
{
    const_iterator i = find(key);
    if (i == end())
        return 0;
    return i->second;
}

void ValueBag::set(const std::string& key, values::Value* val)
{
	iterator i = find(key);
	if (i == end())
		insert(make_pair(key, val));
	else
	{
		if (i->second)
			delete i->second;
		i->second = val;
	}
}

void ValueBag::update(const ValueBag& vb)
{
	for (ValueBag::const_iterator i = vb.begin();
			i != vb.end(); ++i)
		set(i->first, i->second->clone());
}

void ValueBag::encode(core::BinaryEncoder& enc) const
{
    for (const_iterator i = begin(); i != end(); ++i)
    {
        // Key length
        enc.add_unsigned(i->first.size(), 1);
        // Key
        enc.add_raw(i->first);
        // Value
        i->second->encode(enc);
    }
}

std::string ValueBag::toString() const
{
	string res;
	for (const_iterator i = begin(); i != end(); ++i)
	{
		if (i != begin())
			res += ", ";
		res += i->first;
		res += '=';
		res += i->second->toString();
	}
	return res;
}

void ValueBag::serialise(structured::Emitter& e) const
{
    e.start_mapping();
    for (const_iterator i = begin(); i != end(); ++i)
    {
        e.add(i->first);
        i->second->serialise(e);
    }
    e.end_mapping();
}

/**
 * Decode from compact binary representation
 */
ValueBag ValueBag::decode(core::BinaryDecoder& dec)
{
    ValueBag res;
    while (dec)
    {
        // Key length
        unsigned key_len = dec.pop_uint(1, "valuebag key length");

        // Key
        string key = dec.pop_string(key_len, "valuebag key");

        // Value
        res.set(key, values::Value::decode(dec));
    }
    return res;
}

/**
 * Parse from a string representation
 */
ValueBag ValueBag::parse(const std::string& str)
{
	// Parsa key\s*=\s*val
	// Parsa \s*,\s*
	// Loop
	ValueBag res;
	size_t begin = 0;
	while (begin < str.size())
	{
		// Take until the next '='
		size_t cur = str.find('=', begin);
		// If there are no more '=', check that we are at the end
		if (cur == string::npos)
		{
			cur = skipSpaces(str, begin);
			if (cur != str.size())
				throw_consistency_error("parsing key=value list", "found invalid extra characters \""+str.substr(begin)+"\" at the end of the list");
			break;
		}
			
		// Read the key
		string key = str::strip(str.substr(begin, cur-begin));

		// Skip the '=' sign
		++cur;

		// Skip spaces after the '='
		cur = skipSpaces(str, cur);

        // Parse the value
        size_t lenParsed;
        unique_ptr<values::Value> val(values::Value::parse(str.substr(cur), lenParsed));

        // Set the value
        if (val.get())
            res.set(key, val.release());
        else
            throw_consistency_error("parsing key=value list", "cannot parse value at \""+str.substr(cur)+"\"");

		// Move on to the next one
		begin = cur + lenParsed;

		// Skip separators
		while (begin != str.size() && (isspace(str[begin]) || str[begin] == ','))
			++begin;
	}

	return res;
}

ValueBag ValueBag::parse(const structured::Reader& reader)
{
    ValueBag res;
    reader.items("values", [&](const std::string& key, const structured::Reader& val) {
        switch (val.type())
        {
            case structured::NodeType::NONE:
                break;
            case structured::NodeType::INT:
                res.set(key, values::Value::create_integer(val.as_int("int value")));
                break;
            case structured::NodeType::STRING:
                res.set(key, values::Value::create_string(val.as_string("string value")));
                break;
            default:
                throw std::runtime_error("cannot decode value " + key + ": value is neither integer nor string");
        }
    });
    return res;
}

#ifdef HAVE_LUA
void ValueBag::lua_push(lua_State* L) const
{
    lua_newtable(L);
    for (const_iterator i = begin(); i != end(); ++i)
    {
        string name = i->first;
        lua_pushlstring(L, name.data(), name.size());
        if (const values::Integer* vs = dynamic_cast<const values::Integer*>(i->second))
        {
            lua_pushnumber(L, vs->toInt());
        } else if (const values::String* vs = dynamic_cast<const values::String*>(i->second)) {
            string val = vs->toString();
            lua_pushlstring(L, val.data(), val.size());
        } else {
            string val = i->second->toString();
            lua_pushlstring(L, val.data(), val.size());
        }
        // Set name = val in the table
        lua_settable(L, -3);
    }
    // Leave the table on the stack: we pushed it
}

void ValueBag::load_lua_table(lua_State* L, int idx)
{
	// Make the table index absolute
	if (idx < 0) idx = lua_gettop(L) + idx + 1;

	// Iterate the table
	lua_pushnil(L);
	while (lua_next(L, idx))
	{
        // Get key
        string key;
        switch (lua_type(L, -2))
        {
            case LUA_TNUMBER: key = to_string((int)lua_tonumber(L, -2)); break;
            case LUA_TSTRING: key = lua_tostring(L, -2); break;
            default:
            {
                char buf[256];
                snprintf(buf, 256, "cannot read Lua table: key has type %s but only ints and strings are supported",
                        lua_typename(L, lua_type(L, -2)));
                throw std::runtime_error(buf);
            }
        }
        // Get value
        switch (lua_type(L, -1))
        {
            case LUA_TNUMBER:
                set(key, values::Value::create_integer(lua_tonumber(L, -1)));
                break;
            case LUA_TSTRING:
                set(key, values::Value::create_string(lua_tostring(L, -1)));
                break;
            default:
            {
                char buf[256];
                snprintf(buf, 256, "cannot read Lua table: value has type %s but only ints and strings are supported",
                            lua_typename(L, lua_type(L, -1)));
                throw std::runtime_error(buf);
            }
		}
		lua_pop(L, 1);
	}
	lua_pop(L, 1);
}
#endif

#if 0

template<typename Base>
struct DataKeyVal : Base
{
	virtual bool match(const std::map<std::string, int>& wanted) const
	{
		// Both a and b are sorted, so we can iterate them linerly together

		values_t::const_iterator a = values.begin();
		std::map<std::string, int>::const_iterator b = wanted.begin();

		while (a != values.end())
		{
			// Nothing else wanted anymore
			if (b == wanted.end())
				return true;
			if (a->first < b->first)
				// This value is not in the match expression
				++a;
			else if (b->first < a->first)
				// This value is wanted but we don't have it
				return false;
			else if (a->second != b->second)
				// Same key, check if the value is the same
				return false;
			else
			{
				// If also the value is the same, move on to the next item
				++a;
				++b;
			}
		}
		// We got to the end of a.  If there are still things in b, we don't
		// match.  If we are also to the end of b, then we matched everything
		return b == wanted.end();
	}
};
#endif

}
}
