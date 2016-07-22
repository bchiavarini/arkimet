#include "step.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/types/reftime.h"
#include "arki/utils/pcounter.h"
#include "arki/utils/string.h"
#include <cstdint>
#include <inttypes.h>
#include <sstream>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
using arki::core::Time;

namespace arki {
namespace dataset {

struct BaseStep : public Step
{
    bool pathMatches(const std::string& path, const matcher::OR& m) const override
    {
        Time min;
        Time max;
        if (!path_timespan(path, min, max))
            return false;
        auto rt = Reftime::createPeriod(min, max);
        return m.matchItem(*rt);
    }
};

struct Yearly : public BaseStep
{
    static const char* name() { return "yearly"; }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int dummy;
        int ye;
        if (sscanf(path.c_str(), "%02d/%04d", &dummy, &ye) != 2)
            return false;

        start_time.set_lowerbound(ye);
        end_time.set_upperbound(ye);
        return true;
    }

    std::string operator()(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[9];
        snprintf(buf, 9, "%02d/%04d", tt.ye/100, tt.ye);
        return buf;
    }

    std::string path_in_shard(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[9];
        snprintf(buf, 9, "%04d", tt.ye);
        return buf;
    }
};

struct Monthly : public BaseStep
{
    static const char* name() { return "monthly"; }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo;
        if (sscanf(path.c_str(), "%04d/%02d", &ye, &mo) == 0)
            return false;

        start_time.set_lowerbound(ye, mo);
        end_time.set_upperbound(ye, mo);
        return true;
    }

    std::string operator()(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%04d/%02d", tt.ye, tt.mo);
        return buf;
    }

    std::string path_in_shard(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%02d", tt.mo);
        return buf;
    }
};

struct Biweekly : public BaseStep
{
    static const char* name() { return "biweekly"; }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo = -1, biweek = -1;
        if (sscanf(path.c_str(), "%04d/%02d-%d", &ye, &mo, &biweek) == 0)
            return false;

        int min_da = -1;
        int max_da = -1;
        switch (biweek)
        {
            case 1: min_da = 1; max_da = 14; break;
            case 2: min_da = 15; max_da = -1; break;
            default: break;
        }
        start_time.set_lowerbound(ye, mo, min_da);
        end_time.set_upperbound(ye, mo, max_da);
        return true;
    }

    std::string operator()(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%04d/%02d-", tt.ye, tt.mo);
        stringstream res;
        res << buf;
        res << (tt.da > 15 ? 2 : 1);
        return res.str();
    }

    std::string path_in_shard(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%02d-", tt.mo);
        stringstream res;
        res << buf;
        res << (tt.da > 15 ? 2 : 1);
        return res.str();
    }
};

struct Weekly : public BaseStep
{
    static const char* name() { return "weekly"; }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo = -1, week = -1;
        if (sscanf(path.c_str(), "%04d/%02d-%d", &ye, &mo, &week) == 0)
            return false;
        int min_da = -1;
        int max_da = -1;
        if (week != -1)
        {
            min_da = (week - 1) * 7 + 1;
            max_da = min_da + 6;
        }
        start_time.set_lowerbound(ye, mo, min_da);
        end_time.set_upperbound(ye, mo, max_da);
        return true;
    }

    std::string operator()(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%04d/%02d-", tt.ye, tt.mo);
        stringstream res;
        res << buf;
        res << (((tt.da - 1) / 7) + 1);
        return res.str();
    }

    std::string path_in_shard(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%02d-", tt.mo);
        stringstream res;
        res << buf;
        res << (((tt.da - 1) / 7) + 1);
        return res.str();
    }
};

struct Daily : public BaseStep
{
    static const char* name() { return "daily"; }

    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo, da;
        if (sscanf(path.c_str(), "%04d/%02d-%02d", &ye, &mo, &da) == 0)
            return false;
        start_time.set_lowerbound(ye, mo, da);
        end_time.set_upperbound(ye, mo, da);
        return true;
    }

    std::string operator()(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[15];
        snprintf(buf, 15, "%04d/%02d-%02d", tt.ye, tt.mo, tt.da);
        return buf;
    }

    std::string path_in_shard(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[15];
        snprintf(buf, 15, "%02d-%02d", tt.mo, tt.da);
        return buf;
    }
};

std::shared_ptr<Step> Step::create(const std::string& type)
{
    if (type == Daily::name())
        return shared_ptr<Step>(new Daily);
    else if (type == Weekly::name())
        return shared_ptr<Step>(new Weekly);
    else if (type == Biweekly::name())
        return shared_ptr<Step>(new Biweekly);
    else if (type == Monthly::name())
        return shared_ptr<Step>(new Monthly);
    else if (type == Yearly::name())
        return shared_ptr<Step>(new Yearly);
    else
        throw std::runtime_error("step '" + type + "' is not supported.  Valid values are daily, weekly, biweekly, monthly, and yearly.");
}

std::vector<std::string> Step::list()
{
    vector<string> res {
        Daily::name(),
        Weekly::name(),
        Biweekly::name(),
        Monthly::name(),
        Yearly::name(),
    };
    return res;
}


struct ShardYearly : public ShardedStep
{
    static const char* name() { return "yearly"; }

    /*
    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int dummy;
        int ye;
        if (sscanf(path.c_str(), "%02d/%04d", &dummy, &ye) != 2)
            return false;

        start_time.set_lowerbound(ye);
        end_time.set_upperbound(ye);
        return true;
    }
    */

    std::string shard_path(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[9];
        snprintf(buf, 9, "%04d", tt.ye);
        return buf;
    }

    /*
    std::string path_in_shard(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[9];
        snprintf(buf, 9, "%04d", tt.ye);
        return buf;
    }
    */
};

struct ShardMonthly : public ShardedStep
{
    static const char* name() { return "monthly"; }

    /*
    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo;
        if (sscanf(path.c_str(), "%04d/%02d", &ye, &mo) == 0)
            return false;

        start_time.set_lowerbound(ye, mo);
        end_time.set_upperbound(ye, mo);
        return true;
    }
    */

    std::string shard_path(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%04d-%02d", tt.ye, tt.mo);
        return buf;
    }

    /*
    std::string path_in_shard(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%02d", tt.mo);
        return buf;
    }
    */
};

struct ShardWeekly : public ShardedStep
{
    static const char* name() { return "weekly"; }

    /*
    bool path_timespan(const std::string& path, Time& start_time, Time& end_time) const override
    {
        int ye, mo = -1, week = -1;
        if (sscanf(path.c_str(), "%04d/%02d-%d", &ye, &mo, &week) == 0)
            return false;
        int min_da = -1;
        int max_da = -1;
        if (week != -1)
        {
            min_da = (week - 1) * 7 + 1;
            max_da = min_da + 6;
        }
        start_time.set_lowerbound(ye, mo, min_da);
        end_time.set_upperbound(ye, mo, max_da);
        return true;
    }
    */

    std::string shard_path(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%04d-%02d-%1d", tt.ye, tt.mo, (((tt.da - 1) / 7) + 1));
        return buf;
    }

    /*
    std::string path_in_shard(const Metadata& md) const override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%02d-", tt.mo);
        stringstream res;
        res << buf;
        res << (((tt.da - 1) / 7) + 1);
        return res.str();
    }
    */
};


std::shared_ptr<ShardedStep> ShardedStep::create(const std::string& shard_type, const std::string& type)
{
    if (type == ShardWeekly::name())
        return shared_ptr<ShardedStep>(new ShardWeekly);
    else if (type == ShardMonthly::name())
        return shared_ptr<ShardedStep>(new ShardMonthly);
    else if (type == ShardYearly::name())
        return shared_ptr<ShardedStep>(new ShardYearly);
    else
        throw std::runtime_error("shard step '" + type + "' is not supported.  Valid values are weekly, monthly, and yearly.");
}

}
}
