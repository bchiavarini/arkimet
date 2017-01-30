#ifndef ARKI_READER_H
#define ARKI_READER_H

/// Generic interface to read data files
#include <arki/libconfig.h>
#include <string>
#include <unordered_map>
#include <memory>
#include <cstddef>
#include <sys/types.h>

namespace arki {
namespace reader {

struct Reader
{
public:
	virtual ~Reader() {}

	virtual void read(off_t ofs, size_t size, void* buf) = 0;
	virtual bool is(const std::string& fname) = 0;
};

class Registry
{
protected:
    std::unordered_map<std::string, std::weak_ptr<Reader>> cache;

    std::shared_ptr<Reader> instantiate(const std::string& abspath);

public:
    /**
     * Returns a reader for the file with the given absolute path
     */
    std::shared_ptr<Reader> reader(const std::string& abspath);

    /**
     * Remove expired entries from the cache
     */
    void cleanup();
};

/**
 * Read data from files, keeping the last file open.
 *
 * This optimizes for sequentially reading data from files.
 */
class DataReader
{
protected:
    reader::Reader* last;

public:
	DataReader();
	~DataReader();

	void read(const std::string& fname, off_t ofs, size_t size, void* buf);
	void flush();
};

}
}
#endif
