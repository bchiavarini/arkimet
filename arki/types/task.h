#ifndef ARKI_TYPES_TASK_H
#define ARKI_TYPES_TASK_H

#include <arki/types/core.h>

namespace arki {
namespace types {

template<> struct traits<Task>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef unsigned char Style;
};

/**
 * A Task annotation
 */
struct Task : public CoreType<Task>
{
	std::string task;

	Task(const std::string& value) : task(value) {}

    int compare(const Type& o) const override;
    bool equals(const Type& o) const override;

    /// CODEC functions
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    static std::unique_ptr<Task> decode(core::BinaryDecoder& dec);
    static std::unique_ptr<Task> decodeString(const std::string& val);
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;

    Task* clone() const override;

    /// Create a task
    static std::unique_ptr<Task> create(const std::string& value);
    static std::unique_ptr<Task> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    // Register this type tree with the type system
    static void init();
};

}
}
#endif



















