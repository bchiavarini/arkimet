#ifndef ARKI_MATCHER_RUN
#define ARKI_MATCHER_RUN

#include <arki/matcher.h>
#include <arki/matcher/utils.h>
#include <arki/types/run.h>

namespace arki {
namespace matcher {

/**
 * Match Runs
 */
struct MatchRun : public Implementation
{
    std::string name() const override;

    static std::unique_ptr<MatchRun> parse(const std::string& pattern);
    static void init();
};

struct MatchRunMinute : public MatchRun
{
    // This is -1 when it should be ignored in the match
    int minute;

    MatchRunMinute(const std::string& pattern);
    bool matchItem(const types::Type& o) const override;
    std::string toString() const override;
};

}
}

// vim:set ts=4 sw=4:
#endif
