#ifndef ARKI_SCAN_ANY_H
#define ARKI_SCAN_ANY_H

/*
 * scan/any - Scan files autodetecting the format
 *
 * Copyright (C) 2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <string>
#include <ctime>
#include <sys/types.h>

namespace arki {
class Metadata;

namespace metadata {
class Consumer;
}

namespace scan {

/**
 * Scan the given file, sending its metadata to a consumer.
 *
 * If `filename`.metadata exists and its timestamp is the same of the file or
 * newer, it will be used instead of the file.
 *
 * @return true if the file has been scanned, false if the file is in a format
 * that is not supported or recognised.
 */
bool scan(const std::string& file, metadata::Consumer& c);

/**
 * Scan the given file without format autodetection, sending its metadata to a
 * consumer.
 *
 * If `filename`.metadata exists and its timestamp is the same of the file or
 * newer, it will be used instead of the file.
 *
 * @return true if the file has been scanned, false if the file is in a format
 * that is not supported or recognised.
 */
bool scan(const std::string& file, metadata::Consumer& c, const std::string& format);

/**
 * Return true if the file looks like a file with data that can be scanned.
 *
 * The current implementation only looks at the file extension. Future
 * implementations may also have a quick look at the file contents.
 */
bool canScan(const std::string& file);

/**
 * Return true if the file exists, either uncompressed or compressed
 */
bool exists(const std::string& file);

/**
 * Return true if the file exists, and is compressed
 */
bool isCompressed(const std::string& file);

/**
 * Return the file mtime, whether it is compressed or not
 */
time_t timestamp(const std::string& file);

/**
 * Compress the given file
 */
void compress(const std::string& file, size_t groupsize = 512);

/**
 * Validate data
 */
struct Validator
{
	virtual ~Validator() {}

	// Validate data found in a file
	virtual void validate(int fd, off64_t offset, size_t size, const std::string& fname) const = 0;

	// Validate a memory buffer
	virtual void validate(const void* buf, size_t size) const = 0;

	/**
	 * Get the validator for a given file name
	 *
	 * @returns
	 *   a pointer to a static object, which should not be deallocated.
	 */
	static const Validator& by_filename(const std::string& filename);
};

/**
 * Return the update sequence number for this data
 *
 * The data associated to the metadata is read and scanned if needed.
 *
 * @retval
 *   The update sequence number found. This is left untouched if the function
 *   returns false.
 * @returns
 *   true if the update sequence number could be found, else false
 *
 */
bool update_sequence_number(const Metadata& md, int& usn);

}
}

// vim:set ts=4 sw=4:
#endif
