#include "dispatcher.h"
#include "arki/dispatcher.h"
#include "arki/utils/string.h"
#include "arki/runtime/processor.h"
#include "arki/validator.h"
#include "arki/nag.h"
#include "config.h"
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>

#if __xlC__
// From glibc
#define timersub(a, b, result)                                                \
  do {                                                                        \
    (result)->tv_sec = (a)->tv_sec - (b)->tv_sec;                             \
    (result)->tv_usec = (a)->tv_usec - (b)->tv_usec;                          \
    if ((result)->tv_usec < 0) {                                              \
      --(result)->tv_sec;                                                     \
      (result)->tv_usec += 1000000;                                           \
    }                                                                         \
  } while (0)
#endif

using namespace std;
using namespace arki::utils;

namespace arki {
namespace runtime {

MetadataDispatch::MetadataDispatch()
{
    timerclear(&startTime);
}

MetadataDispatch::~MetadataDispatch()
{
    if (dispatcher)
        delete dispatcher;
}

void MetadataDispatch::add_validator(const std::string& name)
{
    const ValidatorRepository& vals = ValidatorRepository::get();
    ValidatorRepository::const_iterator i = vals.find(name);
    if (i == vals.end())
        throw commandline::BadOption("unknown validator '" + name + "'. You can get a list using --validate=list.");
    validators.push_back(i->second);
}

void MetadataDispatch::init()
{
    if (test)
        dispatcher = new TestDispatcher(cfg, cerr);
    else
        dispatcher = new RealDispatcher(cfg);

    for (const auto& v: validators)
        dispatcher->add_validator(*v);
}

bool MetadataDispatch::process(dataset::Reader& ds, const std::string& name)
{
    setStartTime();
    results.clear();

    if (!dir_copyok.empty())
        copyok.reset(new arki::File(str::joinpath(dir_copyok, str::basename(name)), O_WRONLY | O_APPEND | O_CREAT));
    else
        copyok.release();

    if (!dir_copyko.empty())
        copyko.reset(new arki::File(str::joinpath(dir_copyko, str::basename(name)), O_WRONLY | O_APPEND | O_CREAT));
    else
        copyko.release();

    try {
        ds.query_data(Matcher(), [&](unique_ptr<Metadata> md) { return this->dispatch(move(md)); });
    } catch (std::exception& e) {
        // FIXME: this is a quick experiment: a better message can
        // print some of the stats to document partial imports
        //cerr << i->second->value("path") << ": import FAILED: " << e.what() << endl;
        nag::warning("import FAILED: %s", e.what());
        // Still process what we've got so far
        if (next) next->process(results, name);
        throw;
    }

    // Process the resulting annotated metadata as a dataset
    if (next) next->process(results, name);

    if (reportStatus)
    {
        cerr << name << ": " << summarySoFar() << endl;
        cerr.flush();
    }

    bool success = !(countNotImported || countInErrorDataset);
    if (ignore_duplicates)
        success = success && (countSuccessful || countDuplicates);
    else
        success = success && (countSuccessful && !countDuplicates);

    flush();

    countSuccessful = 0;
    countNotImported = 0;
    countDuplicates = 0;
    countInErrorDataset = 0;

    return success;
}

bool MetadataDispatch::dispatch(unique_ptr<Metadata>&& md)
{
    // Dispatch to matching dataset
    switch (dispatcher->dispatch(*md))
    {
        case Dispatcher::DISP_OK:
            do_copyok(*md);
            ++countSuccessful;
            break;
        case Dispatcher::DISP_DUPLICATE_ERROR:
            do_copyko(*md);
            ++countDuplicates;
            break;
        case Dispatcher::DISP_ERROR:
            do_copyko(*md);
            ++countInErrorDataset;
            break;
        case Dispatcher::DISP_NOTWRITTEN:
            do_copyko(*md);
            // If dispatching failed, add a big note about it.
            md->add_note("WARNING: The data has not been imported in ANY dataset");
            ++countNotImported;
            break;
    }
    results.acquire(move(md));
    return dispatcher->canContinue();
}

void MetadataDispatch::do_copyok(Metadata& md)
{
    if (copyok && copyok->is_open())
        copyok->write_all_or_throw(md.getData());
}

void MetadataDispatch::do_copyko(Metadata& md)
{
    if (copyko && copyko->is_open())
        copyko->write_all_or_throw(md.getData());
}

void MetadataDispatch::flush()
{
    if (dispatcher) dispatcher->flush();
}

string MetadataDispatch::summarySoFar() const
{
    string timeinfo;
    if (timerisset(&startTime))
    {
        struct timeval now;
        struct timeval diff;
        gettimeofday(&now, NULL);
        timersub(&now, &startTime, &diff);
        char buf[32];
        snprintf(buf, 32, " in %d.%06d seconds", (int)diff.tv_sec, (int)diff.tv_usec);
        timeinfo = buf;
    }
    if (!countSuccessful && !countNotImported && !countDuplicates && !countInErrorDataset)
        return "no data processed" + timeinfo;

    if (!countNotImported && !countDuplicates && !countInErrorDataset)
    {
        stringstream ss;
        ss << "everything ok: " << countSuccessful << " message";
        if (countSuccessful != 1)
            ss << "s";
        ss << " imported" + timeinfo;
        return ss.str();
    }

    stringstream res;

    if (countNotImported)
        res << "serious problems: ";
    else
        res << "some problems: ";

    res << countSuccessful << " ok, "
        << countDuplicates << " duplicates, "
        << countInErrorDataset << " in error dataset";

    if (countNotImported)
        res << ", " << countNotImported << " NOT imported";

    res << timeinfo;

    return res.str();
}

void MetadataDispatch::setStartTime()
{
    gettimeofday(&startTime, NULL);
}



}
}
