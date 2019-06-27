#ifndef ARKI_CONFIGFILE_H
#define ARKI_CONFIGFILE_H

#include <arki/core/fwd.h>
#include <map>
#include <string>
#include <stdexcept>

namespace arki {

namespace configfile {

class ParseError : public std::runtime_error
{
protected:
    std::string m_name;
    int m_line;
    std::string m_error;

    static std::string describe(const std::string& filename, int line, const std::string& error);

public:
    ParseError(const std::string& filename, int line, const std::string& error)
        : std::runtime_error(describe(filename, line, error)), m_name(filename), m_line(line), m_error(error) {}
    ~ParseError() throw () {}
};

class Parser;

}

/**
 * A tree of configuration information
 */
class ConfigFile
{
protected:
	/// The configuration values in this node
	std::map<std::string, std::string> m_values;

	/**
	 * Subnodes of this node
	 */
	std::map<std::string, ConfigFile*> sections;

public:
    ConfigFile();
    ConfigFile(const ConfigFile& cfg);
    ConfigFile(const std::string& cfg, const std::string& pathname="memory buffer");
    ~ConfigFile();

    ConfigFile& operator=(const ConfigFile& cfg);

	//typedef std::map<std::string, std::string>::iterator iterator;
	typedef std::map<std::string, std::string>::const_iterator const_iterator;
	typedef std::map<std::string, ConfigFile*>::iterator section_iterator;
	typedef std::map<std::string, ConfigFile*>::const_iterator const_section_iterator;

    const_iterator begin() const { return m_values.begin(); }
    const_iterator end() const { return m_values.end(); }
    section_iterator section_begin() { return sections.begin(); }
    section_iterator section_end() { return sections.end(); }
    const_section_iterator section_begin() const { return sections.begin(); }
    const_section_iterator section_end() const { return sections.end(); }

    /// Remove all contents of this ConfigFile
    void clear();

    /// Return the number of sections
    size_t sections_size() const { return sections.size(); }

	/// Number of values at this level (does not include values inside subsections)
	size_t valueSize() const { return m_values.size(); }

	/// Get the values map
	const std::map<std::string, std::string>& values() const { return m_values; }

	/**
	 * Copy all the values from c into this ConfigFile, merging with existing
	 * values and sections.
	 */
	void merge(const ConfigFile& c);

	/**
	 * Copy all the values from c into the given section of this ConfigFile,
	 * creating the section if it does not exist and merging with existing
	 * values and subsections.
	 */
	void mergeInto(const std::string& sectionName, const ConfigFile& c);

	/**
	 * Retrieve the value for a key.
	 *
	 * The empty string is returned if the value is empty.
	 */
	std::string value(const std::string& key) const;

    /// Set a value
    void setValue(const std::string& key, const std::string& value);

    /// Set a value converting any type to a string
    void setValue(const std::string& key, int value);

	/**
	 * Retrieve a section from this config file.
	 *
	 * 0 is returned if there is no such section.
	 */
	ConfigFile* section(const std::string& key) const;

	/**
	 * Retrieve a section from this config file, creating it if it is
	 * missing.
	 */
	ConfigFile* obtainSection(const std::string& key);

    /// Delete a section, if present
    void delete_section(const std::string& key);

    /**
     * Parse configuration from the given input file.
     *
     * The sections that are found are added to this ConfigFile.
     *
     * @param in
     *   The input lines to parse.
     * @param fileName
     *   The file name to use to generate useful parse error messages.
     */
    void parse(core::NamedFileDescriptor& in);

    /**
     * Parse configuration from the given input stream.
     *
     * The sections that are found are added to this ConfigFile.
     *
     * @param in
     *   The input lines to parse.
     * @param fileName
     *   The file name to use to generate useful parse error messages.
     */
    void parse(core::LineReader& in, const std::string& pathname);

    /**
     * Parse configuration from the given string.
     *
     * The values and sections that are found are added to this ConfigFile.
     */
    void parse(const std::string& in, const std::string& pathname="memory buffer");

    /**
     * Output this configuration to the given output stream
     */
    void output(std::ostream& out, const std::string& fileName, const std::string& secName=std::string()) const;

    /// Serialize to a string
    std::string serialize() const;

	/**
	 * Convenient function to parse a boolean
	 */
	static bool boolValue(const std::string& str, bool def = false);

    friend class configfile::Parser;
};

}
#endif
