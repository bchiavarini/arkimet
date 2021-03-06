#ifndef ARKI_TYPES_TIMERANGE_H
#define ARKI_TYPES_TIMERANGE_H

#include <arki/types.h>
#include <arki/types/reftime.h>
#include <stdint.h>

namespace arki {
namespace types {
namespace timerange {

/// Style values
enum class Style: unsigned char {
    GRIB1 = 1,
    GRIB2 = 2,
    BUFR = 3,
    TIMEDEF = 4,
};

enum GRIB1Unit {
    SECOND = 0,
    MONTH = 1
};

enum TimedefUnit {
    UNIT_MINUTE  = 0,
    UNIT_HOUR    = 1,
    UNIT_DAY     = 2,
    UNIT_MONTH   = 3,
    UNIT_YEAR    = 4,
    UNIT_DECADE  = 5,
    UNIT_NORMAL  = 6,
    UNIT_CENTURY = 7,
    UNIT_3HOURS  = 10,
    UNIT_6HOURS  = 11,
    UNIT_12HOURS = 12,
    UNIT_SECOND  = 13,
    UNIT_MISSING = 255
};

}

template<>
struct traits<Timerange>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef timerange::Style Style;
};

template<>
struct traits<timerange::Timedef>
{
    static const char* type_tag;
    static const types::Code type_code;
    static const size_t type_sersize_bytes;

    typedef timerange::Style Style;
};

/**
 * The time span information of the data
 *
 * It can contain information such as accumulation time, or validity of
 * forecast.
 */
struct Timerange : public types::StyledType<Timerange>
{
	/// Convert a string into a style
	static Style parseStyle(const std::string& str);
	/// Convert a style into its string representation
	static std::string formatStyle(Style s);

    /**
     * Compute the forecast step
     *
     * @retval step The forecast step
     * @retval is_seconds if true, the forecast step is in seconds, if false it
     *         is in months
     * @return true if the forecast step could be computed, else false
     */
    virtual bool get_forecast_step(int& step, bool& is_seconds) const = 0;

    /**
     * Return the type of statistical processing (or -1 if not available)
     */
    virtual int get_proc_type() const = 0;

    /**
     * Compute the duration of statistical processing
     *
     * @retval duration The computed duration
     * @retval is_seconds if true, the duration is in seconds, if false it is
     *         in months
     * @return true if the duration could be computed, else false
     */
    virtual bool get_proc_duration(int& duration, bool& is_seconds) const = 0;

    /// Create a Timedef equivalent of this time range
    virtual std::unique_ptr<timerange::Timedef> to_timedef() const;

    /// CODEC functions
    static std::unique_ptr<Timerange> decode(core::BinaryDecoder& dec);
    static std::unique_ptr<Timerange> decodeString(const std::string& val);
    static std::unique_ptr<Timerange> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    // Register this type tree with the type system
    static void init();

    Timerange* clone() const override = 0;

    static std::unique_ptr<Timerange> createGRIB1(unsigned char type, unsigned char unit, unsigned char p1, unsigned char p2);
    static std::unique_ptr<Timerange> createGRIB2(unsigned char type, unsigned char unit, signed long p1, signed long p2);
    static std::unique_ptr<Timerange> createTimedef(uint32_t step_len, timerange::TimedefUnit step_unit=timerange::UNIT_SECOND);
    static std::unique_ptr<Timerange> createTimedef(uint32_t step_len, timerange::TimedefUnit step_unit,
                                                  uint8_t stat_type, uint32_t stat_len, timerange::TimedefUnit stat_unit=timerange::UNIT_SECOND);
    static std::unique_ptr<Timerange> createBUFR(unsigned value = 0, unsigned char unit = 254);
};

namespace timerange {

inline std::ostream& operator<<(std::ostream& o, Style s) { return o << Timerange::formatStyle(s); }


struct GRIB1 : public Timerange
{
protected:
	std::ostream& writeNumbers(std::ostream& o) const;
    /**
     * Get time unit conversion
     *
     * @retval timemul
     *   Factor to multiply to a value in the current units to obtain months or
     *   seconds
     * @returns
     *   true if multiplying by timemul gives seconds, false if it gives months
     */
    bool get_timeunit_conversion(int& timemul) const;

	unsigned char m_type, m_unit;
	unsigned char m_p1, m_p2;

public:
	unsigned type() const { return m_type; }
	unsigned unit() const { return m_unit; }
	unsigned p1() const { return m_p1; }
	unsigned p2() const { return m_p2; }

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    bool get_forecast_step(int& step, bool& is_seconds) const override;
    int get_proc_type() const override;
    bool get_proc_duration(int& duration, bool& is_seconds) const override;

    int compare_local(const Timerange& o) const override;
    bool equals(const Type& o) const override;

    void getNormalised(int& type, GRIB1Unit& unit, int& p1, int& p2, bool& use_op1, bool& use_op2) const;

    GRIB1* clone() const override;
    static std::unique_ptr<GRIB1> create(unsigned char type, unsigned char unit, unsigned char p1, unsigned char p2);
    static std::unique_ptr<GRIB1> decode_structure(const structured::Keys& keys, const structured::Reader& val);
    static void arg_significance(unsigned type, bool& use_p1, bool& use_p2);
};

class GRIB2 : public Timerange
{
protected:
	unsigned char m_type, m_unit;
	signed long m_p1, m_p2;

public:
	unsigned type() const { return m_type; }
	unsigned unit() const { return m_unit; }
	signed p1() const { return m_p1; }
	signed p2() const { return m_p2; }

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    bool get_forecast_step(int& step, bool& is_seconds) const override;
    int get_proc_type() const override;
    bool get_proc_duration(int& duration, bool& is_seconds) const override;

    int compare_local(const Timerange& o) const override;
    bool equals(const Type& o) const override;

    GRIB2* clone() const override;
    static std::unique_ptr<GRIB2> create(unsigned char type, unsigned char unit, signed long p1, signed long p2);
    static std::unique_ptr<GRIB2> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};

class Timedef : public Timerange
{
public:
protected:
    /// Units for forecast step, or UNIT_MISSING if forecast step is missing
    TimedefUnit m_step_unit;
    uint32_t m_step_len;

    /// Type of statistical processing, or 255 if missing
    uint8_t m_stat_type;

    /// Units for length of statistical processing, or UNIT_MINUTE if length of
    /// statistical processing is missing
    TimedefUnit m_stat_unit;
    uint32_t m_stat_len;

public:
    TimedefUnit step_unit() const { return m_step_unit; }
    unsigned step_len() const { return m_step_len; }
    uint8_t stat_type() const { return m_stat_type; }
    TimedefUnit stat_unit() const { return m_stat_unit; }
    unsigned stat_len() const { return m_stat_len; }

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    bool get_forecast_step(int& step, bool& is_seconds) const override;
    int get_proc_type() const override;
    bool get_proc_duration(int& duration, bool& is_seconds) const override;

    int compare_local(const Timerange& o) const override;
    bool equals(const Type& o) const override;

    /**
     * Given a reftime representing validity time, compute and return its
     * emission time, shifting it by what is represented by this timedef
     */
    std::unique_ptr<reftime::Position> validity_time_to_emission_time(const reftime::Position& src) const;

    Timedef* clone() const override;
    static std::unique_ptr<Timedef> create(uint32_t step_len, TimedefUnit step_unit=UNIT_SECOND);
    static std::unique_ptr<Timedef> create(uint32_t step_len, TimedefUnit step_unit,
                              uint8_t stat_type, uint32_t stat_len, TimedefUnit stat_unit=UNIT_SECOND);
    static std::unique_ptr<Timedef> createFromYaml(const std::string& str);
    static std::unique_ptr<Timedef> decode_structure(const structured::Keys& keys, const structured::Reader& val);

    /**
     * Unit conversion for code table 4.4 GRIB2 indicator of unit of time range
     *
     * @param unit value to use
     * @retval timemul
     *   Factor to multiply to a value in the current units to obtain months or
     *   seconds
     * @returns
     *   true if multiplying by timemul gives seconds, false if it gives months
     */
    static bool timeunit_conversion(TimedefUnit unit, int& timemul);

    /**
     * Return the suffix for the given time unit
     *
     * Can return NULL if unit is 255 or an invalid/unsupported value
     */
    static const char* timeunit_suffix(TimedefUnit unit);

    static void timeunit_output(TimedefUnit unit, uint32_t val, std::ostream& out);

    static bool timeunit_parse_suffix(const char*& str, TimedefUnit& unit);
    static bool timeunit_parse(const char*& str, TimedefUnit& unit, uint32_t& val);
};

class BUFR : public Timerange
{
protected:
	unsigned char m_unit;
	unsigned m_value;

public:
	unsigned unit() const { return m_unit; }
	unsigned value() const { return m_value; }

	bool is_seconds() const;
	unsigned seconds() const;
	unsigned months() const;

    Style style() const override;
    void encodeWithoutEnvelope(core::BinaryEncoder& enc) const override;
    std::ostream& writeToOstream(std::ostream& o) const override;
    void serialise_local(structured::Emitter& e, const structured::Keys& keys, const Formatter* f=0) const override;
    std::string exactQuery() const override;

    bool get_forecast_step(int& step, bool& is_seconds) const override;
    int get_proc_type() const override;
    bool get_proc_duration(int& duration, bool& is_seconds) const override;

    int compare_local(const Timerange& o) const override;
    bool equals(const Type& o) const override;

    BUFR* clone() const override;
    static std::unique_ptr<BUFR> create(unsigned value = 0, unsigned char unit = 254);
    static std::unique_ptr<BUFR> decode_structure(const structured::Keys& keys, const structured::Reader& val);
};

}
}
}
#endif
