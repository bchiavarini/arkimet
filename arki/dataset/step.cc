#include "step.h"
#include "arki/configfile.h"
#include "arki/metadata.h"
#include "arki/matcher.h"
#include "arki/types/reftime.h"
#include "arki/types/time.h"
#include "arki/utils/pcounter.h"
#include "arki/utils/string.h"
#include "arki/wibble/grcal/grcal.h"
#include <cstdint>
#include <inttypes.h>
#include <sstream>
#include <cstdio>

using namespace std;
using namespace arki;
using namespace arki::types;
using namespace arki::utils;
namespace gd = wibble::grcal::date;

namespace arki {
namespace dataset {

struct BaseStep : public Step
{
    bool pathMatches(const std::string& path, const matcher::OR& m) const override
    {
        types::Time min;
        types::Time max;
        if (!path_timespan(path, min, max))
            return false;
        auto rt = Reftime::createPeriod(min, max);
        return m.matchItem(*rt);
    }
};

struct Yearly : public BaseStep
{
    static const char* name() { return "yearly"; }

    bool path_timespan(const std::string& path, types::Time& start_time, types::Time& end_time) const override
    {
        int dummy;
        int base[6] = { -1, -1, -1, -1, -1, -1 };
        if (sscanf(path.c_str(), "%02d/%04d", &dummy, &base[0]) != 2)
            return false;

        gd::lowerbound(base, start_time.vals);
        gd::upperbound(base, end_time.vals);
        return true;
    }

    std::string operator()(const Metadata& md) override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[9];
        snprintf(buf, 9, "%02d/%04d", tt.vals[0]/100, tt.vals[0]);
        return buf;
    }
};

struct Monthly : public BaseStep
{
    static const char* name() { return "monthly"; }

    bool path_timespan(const std::string& path, types::Time& start_time, types::Time& end_time) const override
    {
        int base[6] = { -1, -1, -1, -1, -1, -1 };
        if (sscanf(path.c_str(), "%04d/%02d", &base[0], &base[1]) == 0)
            return false;

        gd::lowerbound(base, start_time.vals);
        gd::upperbound(base, end_time.vals);
        return true;
    }

    std::string operator()(const Metadata& md) override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%04d/%02d", tt.vals[0], tt.vals[1]);
        return buf;
    }
};

struct Biweekly : public BaseStep
{
    static const char* name() { return "biweekly"; }

    bool path_timespan(const std::string& path, types::Time& start_time, types::Time& end_time) const override
    {
        int year, month = -1, biweek = -1;
        int min[6] = { -1, -1, -1, -1, -1, -1 };
        int max[6] = { -1, -1, -1, -1, -1, -1 };
        if (sscanf(path.c_str(), "%04d/%02d-%d", &year, &month, &biweek) == 0)
            return false;
        min[0] = max[0] = year;
        min[1] = max[1] = month;
		switch (biweek)
		{
			case 1: min[2] = 1; max[2] = 14; break;
			case 2: min[2] = 15; max[2] = -1; break;
			default: break;
		}
        gd::lowerbound(min);
        gd::upperbound(max);
        start_time.set(min);
        end_time.set(max);
        return true;
    }

    std::string operator()(const Metadata& md) override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%04d/%02d-", tt.vals[0], tt.vals[1]);
        stringstream res;
        res << buf;
        res << (tt.vals[2] > 15 ? 2 : 1);
        return res.str();
    }
};

struct Weekly : public BaseStep
{
    static const char* name() { return "weekly"; }

    bool path_timespan(const std::string& path, types::Time& start_time, types::Time& end_time) const override
    {
		int year, month = -1, week = -1;
		int min[6] = { -1, -1, -1, -1, -1, -1 };
		int max[6] = { -1, -1, -1, -1, -1, -1 };
        if (sscanf(path.c_str(), "%04d/%02d-%d", &year, &month, &week) == 0)
            return false;
        min[0] = max[0] = year;
        min[1] = max[1] = month;
		if (week != -1)
		{
			min[2] = (week - 1) * 7 + 1;
			max[2] = min[2] + 6;
		}
        gd::lowerbound(min);
        gd::upperbound(max);
        start_time.set(min);
        end_time.set(max);
        return true;
    }

    std::string operator()(const Metadata& md) override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[10];
        snprintf(buf, 10, "%04d/%02d-", tt.vals[0], tt.vals[1]);
        stringstream res;
        res << buf;
        res << (((tt.vals[2]-1) / 7) + 1);
        return res.str();
    }
};

struct Daily : public BaseStep
{
    static const char* name() { return "daily"; }

    bool path_timespan(const std::string& path, types::Time& start_time, types::Time& end_time) const override
    {
        int base[6] = { -1, -1, -1, -1, -1, -1 };
        if (sscanf(path.c_str(), "%04d/%02d-%02d", &base[0], &base[1], &base[2]) == 0)
            return false;
        gd::lowerbound(base, start_time.vals);
        gd::upperbound(base, end_time.vals);
        return true;
    }

    std::string operator()(const Metadata& md) override
    {
        const Time& tt = md.get<reftime::Position>()->time;
        char buf[15];
        snprintf(buf, 15, "%04d/%02d-%02d", tt.vals[0], tt.vals[1], tt.vals[2]);
        return buf;
    }
};

struct SingleFile : public BaseStep
{
	/* produce un nome di percorso di file nel formato <YYYYY>/<MM>/<DD>/<hh>/<progressivo> */
	/* in base al metadato "reftime". */
	/* l'uso del progressivo permette di evitare collisioni nel caso di reftime uguali */
	/* il progressivo e' calcolato a livello di dataset e non di singola directory */
	/* (questo permette eventualmente di tenere traccia dell'ordine di archiviazione) */

	static const char* name() { return "singlefile"; }

	arki::utils::PersistentCounter<uint64_t> m_counter;

    SingleFile(const ConfigFile& cfg)
        :m_counter()
    {
		std::string path = cfg.value("path");
		if (path.empty()) path = ".";
		path += "/targetfile.singlefile.dat";
		m_counter.bind(path);
	}

    virtual ~SingleFile() {}

    bool path_timespan(const std::string& path, types::Time& start_time, types::Time& end_time) const override
    {
        int base[6] = { -1, -1, -1, -1, -1, -1 };
        uint64_t counter;
        if (sscanf(path.c_str(), "%04d/%02d/%02d/%02d/%" SCNu64, &base[0], &base[1], &base[2], &base[3], &counter) == 0)
            return false;
        gd::lowerbound(base, start_time.vals);
        gd::upperbound(base, end_time.vals);
        return true;
    }

    std::string operator()(const Metadata& md) override
    {
        const Time& tt = md.get<reftime::Position>()->time;
		uint64_t num = m_counter.inc();
		char buf[50];
        snprintf(buf, 50, "%04d/%02d/%02d/%02d/%" SCNu64, tt.vals[0], tt.vals[1], tt.vals[2], tt.vals[3], num);
        return buf;
    }

};

Step* Step::create(const ConfigFile& cfg)
{
    string step = str::lower(cfg.value("step"));
    if (step.empty()) return nullptr;

	if (step == Daily::name())
		return new Daily;
	else if (step == Weekly::name())
		return new Weekly;
	else if (step == Biweekly::name())
		return new Biweekly;
	else if (step == Monthly::name())
		return new Monthly;
	else if (step == Yearly::name())
		return new Yearly;
	else if (step == SingleFile::name())
		return new SingleFile(cfg);
    else
        throw std::runtime_error("step '"+step+"' is not supported.  Valid values are daily, weekly, biweekly, monthly and yearly.");
}

std::vector<std::string> Step::list()
{
    vector<string> res {
        Daily::name(),
        Weekly::name(),
        Biweekly::name(),
        Monthly::name(),
        Yearly::name(),
        SingleFile::name(),
    };
    return res;
}

}
}