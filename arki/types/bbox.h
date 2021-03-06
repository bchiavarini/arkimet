#ifndef ARKI_TYPES_BBOX_H
#define ARKI_TYPES_BBOX_H

/**
 * WARNING
 * This metadata type is discontinued, and it exists only to preserve
 * compatibility with existing saved data
 */


#include <arki/types/styled.h>

namespace arki {
namespace types {

namespace bbox {

/// Style values
enum class Style: unsigned char {
    INVALID = 1,
    POINT = 2,
    BOX = 3,
    HULL = 4,
};

}

struct BBox;

template<>
struct traits<BBox>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef bbox::Style Style;
};

/**
 * The bbox of some data.
 *
 * It can contain information like centre, process, subcentre, subprocess and
 * other similar data.
 */
struct BBox : public types::StyledType<BBox>
{
	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /// CODEC functions
    static std::unique_ptr<BBox> decode(core::BinaryDecoder& dec);
    static std::unique_ptr<BBox> decodeString(const std::string& val);
    static std::unique_ptr<BBox> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    // Register this type tree with the type system
    static void init();

    static std::unique_ptr<BBox> createInvalid();
};

namespace bbox {

struct INVALID : public BBox
{
    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;

    int compare_local(const BBox& o) const override;
    bool equals(const Type& o) const override;

    INVALID* clone() const override;
    static std::unique_ptr<INVALID> create();
};

}

}
}

// vim:set ts=4 sw=4:
#endif
