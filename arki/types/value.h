#ifndef ARKI_TYPES_VALUE_H
#define ARKI_TYPES_VALUE_H

#include <arki/types/core.h>
#include <string>

struct lua_State;

namespace arki {
namespace types {

struct Value;

template<>
struct traits<Value>
{
	static const char* type_tag;
	static const types::Code type_code;
	static const size_t type_sersize_bytes;
	static const char* type_lua_tag;
};

/**
 * The value of very short data encoded as part of the metadata
 *
 * This is currently used to encode the non-metadata part of VM2 data so that
 * it can be extracted from metadata or dataset indices and completed using the
 * rest of metadata values, avoiding disk lookips
 */
struct Value : public types::CoreType<Value>
{
    std::string buffer;

    bool equals(const Type& o) const override;
    int compare(const Type& o) const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    /// CODEC functions
    static std::unique_ptr<Value> decode(core::BinaryDecoder& dec);
    static std::unique_ptr<Value> decodeString(const std::string& val);
    static std::unique_ptr<Value> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    Value* clone() const override;
    static std::unique_ptr<Value> create(const std::string& buf);

    static void lua_loadlib(lua_State* L);

    // Register this type tree with the type system
    static void init();
};

}
}
#endif
