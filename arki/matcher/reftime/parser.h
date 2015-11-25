#ifndef ARKI_MATCHER_REFTIME_PARSER_H
#define ARKI_MATCHER_REFTIME_PARSER_H

#include <arki/types.h>
#include <arki/types/time.h>
#include <wibble/exception.h>
#include <arki/utils/string.h>
#include <wibble/grcal/grcal.h>
#include <string>
#include <ctime>
#include <vector>
#include <set>
#include <sstream>
#include <cstdio>

using namespace std;
using namespace wibble::grcal;

namespace arki {
namespace matcher {
namespace reftime {

struct DTMatch
{
	virtual ~DTMatch() {}
	virtual bool match(const int* tt) const = 0;
	virtual bool match(const int* begin, const int* end) const = 0;
	virtual std::string sql(const std::string& column) const = 0;
	virtual std::string toString() const = 0;
    /**
     * Time (in seconds since midnight) of this expression, used as a reference
     * when building derived times. For example, an expression of
     * "reftime:>yesterday every 12h" may become ">=yyyy-mm-dd 23:59:59" but
     * the step should still match 00:00 and 12:00, and not 23:59 and 11:59
     */
    virtual int timebase() const = 0;
	virtual bool isLead() const { return true; }
    /**
     * Restrict a datetime range, returning the new range endpoints in begin
     * and end.
     *
     * A NULL unique_ptr means an open end.
     *
     * Returns true if the result is a valid interval, false if this match does
     * not match the given interval at all.
     */
    virtual bool restrict_date_range(std::unique_ptr<types::Time>& begin, std::unique_ptr<types::Time>& end) const {}

    static DTMatch* createLE(const int* tt);
    static DTMatch* createLT(const int* tt);
    static DTMatch* createGE(const int* tt);
    static DTMatch* createGT(const int* tt);
    static DTMatch* createEQ(const int* tt);

    static DTMatch* createTimeLE(const int* tt);
    static DTMatch* createTimeLT(const int* tt);
    static DTMatch* createTimeGE(const int* tt);
    static DTMatch* createTimeGT(const int* tt);
    static DTMatch* createTimeEQ(const int* tt);
};

struct Parser
{
	time_t tnow;
	vector<string> errors;
	string unexpected;

	vector<DTMatch*> res;

	Parser()
	{
		tnow = time(NULL);
	}
	~Parser()
	{
		for (vector<DTMatch*>::iterator i = res.begin(); i != res.end(); ++i)
			delete *i;
	}

	void add(DTMatch* t)
	{
		//fprintf(stderr, "ADD %s\n", t->toString().c_str());
		res.push_back(t);
	}

    void add_step(int val, int idx, DTMatch* base=0);

	void mknow(int* vals)
	{
		struct tm now;
	 	gmtime_r(&tnow, &now);
		date::fromtm(now, vals, 6);
	}

	void mktoday(int* vals)
	{
		struct tm now;
	 	gmtime_r(&tnow, &now);
		date::fromtm(now, vals, 3);
	}

	void mkyesterday(int* vals)
	{
		time_t tv = tnow - 3600*24;
		struct tm v;
	 	gmtime_r(&tv, &v);
		date::fromtm(v, vals, 3);
	}

	void mktomorrow(int* vals)
	{
		time_t tv = tnow + 3600*24;
		struct tm v;
	 	gmtime_r(&tv, &v);
		date::fromtm(v, vals, 3);
	}

	void parse(const std::string& str);
};

}
}
}

// vim:set ts=4 sw=4:
#endif
