#ifndef ARKI_RUNTIME_DISPATCHER_H
#define ARKI_RUNTIME_DISPATCHER_H

#include <arki/configfile.h>
#include <arki/dataset/memory.h>

namespace arki {
struct Metadata;
struct Dispatcher;

namespace runtime {
struct DatasetProcessor;

/// Dispatch metadata
struct MetadataDispatch
{
    const ConfigFile& cfg;
    Dispatcher* dispatcher = nullptr;
    dataset::Memory results;
    runtime::DatasetProcessor& next;
    bool ignore_duplicates = false;
    bool reportStatus = false;

    // Used for timings. Read with gettimeofday at the beginning of a task,
    // and summarySoFar will report the elapsed time
    struct timeval startTime;

    // Incremented when a metadata is imported in the destination dataset.
    // Feel free to reset it to 0 anytime.
    int countSuccessful = 0;

    // Incremented when a metadata is imported in the error dataset because it
    // had already been imported.  Feel free to reset it to 0 anytime.
    int countDuplicates = 0;

    // Incremented when a metadata is imported in the error dataset.  Feel free
    // to reset it to 0 anytime.
    int countInErrorDataset = 0;

    // Incremented when a metadata is not imported at all.  Feel free to reset
    // it to 0 anytime.
    int countNotImported = 0;

    /// Directory where we store copyok files
    std::string dir_copyok;

    /// Directory where we store copyko files
    std::string dir_copyko;

    /// File to which we send data that was successfully imported
    std::unique_ptr<arki::File> copyok;

    /// File to which we send data that was not successfully imported
    std::unique_ptr<arki::File> copyko;


    MetadataDispatch(const ConfigFile& cfg, runtime::DatasetProcessor& next, bool test=false);
    ~MetadataDispatch();

    /**
     * Dispatch the data from one source
     *
     * @returns true if all went well, false if any problem happend.
     * It can still throw in case of big trouble.
     */
    bool process(dataset::Reader& ds, const std::string& name);

    // Flush all imports done so far
    void flush();

    // Format a summary of the import statistics so far
    std::string summarySoFar() const;

    // Set startTime to the current time
    void setStartTime();

protected:
    bool dispatch(std::unique_ptr<Metadata>&& md);

    void do_copyok(Metadata& md);
    void do_copyko(Metadata& md);
};

}
}
#endif
