#include <arki/wibble/exception.h>
#include <arki/types/value.h>
#include <arki/types/utils.h>
#include <arki/binary.h>
#include <arki/utils/string.h>
#include <arki/emitter.h>
#include <arki/emitter/memory.h>
#include "config.h"
#include <iomanip>
#include <sstream>

#ifdef HAVE_LUA
#include <arki/utils/lua.h>
#endif

#define CODE TYPE_VALUE
#define TAG "value"
#define SERSIZELEN 0   // Not supported in version 1

using namespace std;
using namespace arki::utils;

namespace arki {
namespace types {

const char* traits<Value>::type_tag = TAG;
const types::Code traits<Value>::type_code = CODE;
const size_t traits<Value>::type_sersize_bytes = SERSIZELEN;
const char* traits<Value>::type_lua_tag = LUATAG_TYPES ".run";

bool Value::equals(const Type& o) const
{
    const Value* v = dynamic_cast<const Value*>(&o);
    if (!v) return false;
    return buffer == v->buffer;
}

int Value::compare(const Type& o) const
{
    using namespace wibble;

    int res = CoreType<Value>::compare(o);
    if (res != 0) return res;

    // We should be the same kind, so upcast
    const Value* v = dynamic_cast<const Value*>(&o);
    if (!v)
    {
        stringstream ss;
        ss << "cannot compare metadata type: second element claims to be `value', but is `" << typeid(&o).name() << "' instead";
        throw std::runtime_error(ss.str());
    }

    // Just compare buffers
    if (buffer < v->buffer) return -1;
    if (buffer == v->buffer) return 0;
    return 1;
}

void Value::encodeWithoutEnvelope(BinaryEncoder& enc) const
{
    enc.add_raw(buffer);
}

std::ostream& Value::writeToOstream(std::ostream& o) const
{
    return o << str::encode_cstring(buffer);
}

void Value::serialiseLocal(Emitter& e, const Formatter* f) const
{
    e.add("va", buffer);
}

unique_ptr<Value> Value::decode(BinaryDecoder& dec)
{
    return Value::create(dec.pop_string(dec.size, "'value' metadata type"));
}

unique_ptr<Value> Value::decodeString(const std::string& val)
{
    size_t len;
    return Value::create(str::decode_cstring(val, len));
}

unique_ptr<Value> Value::decodeMapping(const emitter::memory::Mapping& val)
{
    return Value::create(val["va"].want_string("parsing item value encoded in metadata"));
}

Value* Value::clone() const
{
    Value* val = new Value;
    val->buffer = buffer;
    return val;
}

unique_ptr<Value> Value::create(const std::string& buf)
{
    Value* val = new Value;
    val->buffer = buf;
    return unique_ptr<Value>(val);
}

void Value::lua_loadlib(lua_State* L)
{
#if 0
	static const struct luaL_Reg lib [] = {
		{ "minute", arkilua_new_minute },
		{ NULL, NULL }
	};
	luaL_openlib(L, "arki_run", lib, 0);
	lua_pop(L, 1);
#endif
}

void Value::init()
{
    MetadataType::register_type<Value>();
}

}
}
#include <arki/types.tcc>
// vim:set ts=4 sw=4:
