#ifndef ARKI_METADATA_H
#define ARKI_METADATA_H

/*
 * metadata - Handle arkimet metadata
 *
 * Copyright (C) 2007--2014  ARPA-SIM <urpsim@smr.arpa.emr.it>
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


#include <arki/itemset.h>
#include <arki/types.h>
#include <arki/types/note.h>
#include <arki/types/source.h>
#include <wibble/sys/buffer.h>
#include <string>

struct lua_State;

namespace arki {

namespace types {
namespace source {
struct Blob;
}
}

namespace metadata {
class Eater;

struct ReadContext
{
    std::string basedir;
    std::string pathname;

    ReadContext();
    ReadContext(const std::string& pathname);
    ReadContext(const std::string& pathname, const std::string& basedir);
};

}

class Formatter;

/**
 * Metadata information about a message
 */
struct Metadata : public ItemSet
{
protected:
    std::string m_notes;
    types::Source* m_source;

public:
    Metadata();
    Metadata(const Metadata&);
    ~Metadata();
    Metadata& operator=(const Metadata&);

    /// Check if a source has been set
    bool has_source() const { return m_source; }
    /// Return the source if present, else raise an exception
    const types::Source& source() const;
    /// Return the Blob source if it exists, else 0
    const types::source::Blob* has_source_blob() const;
    /// Return the Blob source if possible, else raise an exception
    const types::source::Blob& sourceBlob() const;
    /// Set a new source, replacing the old one if present
    void set_source(std::auto_ptr<types::Source> s);
    /// Unsets the source
    void unset_source();

    std::vector<types::Note> notes() const;
    const std::string& notes_encoded() const;
    void set_notes(const std::vector<types::Note>& notes);
    void set_notes_encoded(const std::string& notes);
    void add_note(const types::Note& note);
    void add_note(const std::string& note);

	/**
	 * Check that two Metadata contain the same information
	 */
	bool operator==(const Metadata& m) const;

	/**
	 * Check that two Metadata contain different information
	 */
	bool operator!=(const Metadata& m) const { return !operator==(m); }

	int compare(const Metadata& m) const;
	bool operator<(const Metadata& o) const { return compare(o) < 0; }
	bool operator<=(const Metadata& o) const { return compare(o) <= 0; }
	bool operator>(const Metadata& o) const { return compare(o) > 0; }
	bool operator>=(const Metadata& o) const { return compare(o) >= 0; }

    /// Clear all the contents of this Metadata
    void clear();

	/**
	 * Read a metadata document from the given memory buffer
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 *
	 * If readInline is true, in case the data is transmitted inline, it reads
	 * the data as well: this is what you expect.
	 *
	 * If it's false, then the reader needs to check from the Metadata source
	 * if it is inline, and in that case proceed to read the inline data.
	 *
	 * @returns false when the end of the buffer is reached
	 */
	bool read(const unsigned char*& buf, size_t& len, const metadata::ReadContext& filename);

	/**
	 * Decode the metadata, without the outer bundle headers, from the given buffer.
	 */
	void read(const unsigned char* buf, size_t len, unsigned version, const metadata::ReadContext& filename);

	/**
	 * Decode the metadata, without the outer bundle headers, from the given buffer.
	 */
	void read(const wibble::sys::Buffer& buf, unsigned version, const metadata::ReadContext& filename);

    /// Decode from structured data
	void read(const emitter::memory::Mapping& val);

	/**
	 * Read a metadata document from the given input stream.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 *
	 * If readInline is true, in case the data is transmitted inline, it reads
	 * the data as well: this is what you expect.
	 *
	 * If it's false, then the reader needs to check from the Metadata source
	 * if it is inline, and in that case proceed to read the inline data.
	 *
	 * @returns false when end-of-file is reached
	 */
	bool read(std::istream& in, const std::string& filename, bool readInline = true);

	/**
	 * Read the inline data from the given stream
	 */
	void readInlineData(std::istream& in, const std::string& filename);

	/**
	 * Read a metadata document encoded in Yaml from the given input stream.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 *
	 * @returns false when end-of-file is reached
	 */
	bool readYaml(std::istream& in, const std::string& filename);

	/**
	 * Write the metadata to the given output stream.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 */
	void write(std::ostream& out, const std::string& filename) const;

	/**
	 * Write the metadata to the given output stream.
	 *
	 * The filename string is used to generate nicer parse error messages when
	 * throwing exceptions, and can be anything.
	 */
	void write(int outfd, const std::string& filename) const;

	/**
	 * Write the metadata as YAML text to the given output stream.
	 */
	void writeYaml(std::ostream& out, const Formatter* formatter = 0) const;

    /// Serialise using an emitter
    void serialise(Emitter& e, const Formatter* f=0) const;

    /**
     * Encode to a string
     */
    std::string encodeBinary() const;


	/**
	 * Read the raw blob data described by this metadata.
	 */
	wibble::sys::Buffer getData() const;

    /**
     * Read the raw blob data described by this metadata, forcing
     * reconstructing it from the Value metadata.
     * Returns a 0-length buffer when not possible
     */
    wibble::sys::Buffer getDataFromValue() const;

    /**
     * If the source is not inline, but the data are cached in memory, drop
     * them.
     *
     * Data for non-inline metadata can be cached in memory, for example,
     * by a getData() call or a setCachedData() call.
     */
    void dropCachedData() const;

	/**
	 * Set cached data for non-inline sources, so that getData() won't have
	 * to read it again.
	 */
	void setCachedData(const wibble::sys::Buffer& buf);

	/**
	 * Set the inline data for the metadata
	 */
	void setInlineData(const std::string& format, wibble::sys::Buffer buf);

	/**
	 * Read the data and inline them in the metadata
	 */
	void makeInline();

	/**
	 * Return the size of the data, if known
	 */
	size_t dataSize() const;

    /// Read all metadata from a file into the given consumer
    static void readFile(const std::string& fname, metadata::Eater& mdc);

    /**
     * Read all metadata from a file into the given consumer
     */
    static void readFile(const metadata::ReadContext& fname, metadata::Eater& mdc);

    /**
     * Read all metadata from a file into the given consumer
     */
    static void readFile(std::istream& in, const metadata::ReadContext& file, metadata::Eater& mdc);

    /**
     * Read a metadata group into the given consumer
     */
    static void readGroup(const wibble::sys::Buffer& buf, unsigned version, const metadata::ReadContext& file, metadata::Eater& mdc);

	// LUA functions
	/// Push to the LUA stack a userdata to access this Metadata
	void lua_push(lua_State* L);

    /**
     * Push a userdata to access this Metadata, and hand over its ownership to
     * Lua's garbage collector
     */
    static void lua_push(lua_State* L, std::auto_ptr<Metadata> md);

	/**
	 * Check that the element at \a idx is a Metadata userdata
	 *
	 * @return the Metadata element, or 0 if the check failed
	 */
	static Metadata* lua_check(lua_State* L, int idx);

	/**
	 * Load metadata functions into a lua VM
	 */
	static void lua_openlib(lua_State* L);
};

}

// vim:set ts=4 sw=4:
#endif
