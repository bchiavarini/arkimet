%{
#include "config.h"
#include "parser.h"
#include "arki/core/fuzzytime.h"
#include <string>
#include <stdexcept>
#include <ctime>
#include <sstream>

using namespace std;
using namespace arki::matcher::reftime;

struct LexInterval {
    /// Value of the interval
    int val;
    /// Unit of the interval (0: year... 5: second)
    int idx;

    void add_to(int* vals) const
    {
        vals[idx] += val;
    }
};

#include "reftime-parse.hh"
#include "reftime-lex.h"

//#define YYDEBUG 1

// #define DEBUG_PARSER

#ifdef DEBUG_PARSER
#include <stdio.h>

void pdate(const char* msg, const arki::core::FuzzyTime& date)
{
    fprintf(stderr, "%s: %s\n", msg, date.to_string().c_str());
}

void pdate(const char* msg, const int* date)
{
    fprintf(stderr, "%s: %04d-%02d-%02d %02d:%02d:%02d\n", msg, date[0], date[1], date[2], date[3], date[4], date[5]);
}
#else
#define pdate(msg, date) do {} while(0)
#endif

static void arki_reftimeerror(yyscan_t yyscanner, arki::matcher::reftime::Parser& state, const char* s);

static inline void interval_copy(int* dst, const int* src)
{
    for (unsigned i = 0; i < 6; ++i)
       dst[i] = src[i];
}

static inline void init_interval(int* dst, const LexInterval& interval)
{
    for (int i = 0; i < 6; ++i)
        if (i == interval.idx)
            dst[i] = interval.val;
        else
            dst[i] = 0;
}

// Compute where the 0s begin
static unsigned interval_depth(const int* val)
{
    unsigned depth = 0;
    for (unsigned i = 0; i < 6; ++i)
        if (val[i])
            depth = i + 1;
    return depth;
}

// Compute where the -1s begin
static unsigned interval_depth(const arki::core::FuzzyTime& t)
{
    unsigned depth = 0;
    if (t.ye != -1) depth = 1;
    if (t.mo != -1) depth = 2;
    if (t.da != -1) depth = 3;
    if (t.ho != -1) depth = 4;
    if (t.mi != -1) depth = 5;
    if (t.se != -1) depth = 6;
    return depth;
}

static void interval_add(arki::core::FuzzyTime& dst, const int* val, bool subtract=false)
{
    pdate("add", val);
    pdate(" add to", dst);

    // Compute what's the last valid item between dst and val
    unsigned depth = max(interval_depth(val), interval_depth(dst));

    // Lowerbound dst, adding val values to it
    arki::core::Time lb = dst.lowerbound();
    if (subtract)
    {
        lb.ye -= val[0];
        lb.mo -= val[1];
        lb.da -= val[2];
        lb.ho -= val[3];
        lb.mi -= val[4];
        lb.se -= val[5];
    } else {
        lb.ye += val[0];
        lb.mo += val[1];
        lb.da += val[2];
        lb.ho += val[3];
        lb.mi += val[4];
        lb.se += val[5];
    }
    pdate(" added", dst);

    // Normalise
    lb.normalise();
    dst = lb;
    pdate(" normalised", dst);

    // Restore all the values not defined in dst and val to -1
    switch (depth)
    {
        case 0: dst.ye = -1;
        case 1: dst.mo = -1;
        case 2: dst.da = -1;
        case 3: dst.ho = -1;
        case 4: dst.mi = -1;
        case 5: dst.se = -1;
    }
    pdate(" readded missing", dst);
}

void interval_sub(arki::core::FuzzyTime& dst, const int* val)
{
    interval_add(dst, val, true);
}

// Set dst to 'val' before 'now'
arki::core::FuzzyTime* interval_ago(const int* val, time_t now)
{
    struct tm v;
    gmtime_r(&now, &v);
    std::unique_ptr<arki::core::FuzzyTime> res(new arki::core::FuzzyTime(v));
    interval_sub(*res, val);

    // Compute how many nonzero items we have
    unsigned depth = interval_depth(val);

    // Set all the elements after the position of the last nonzero item to -1
    switch (depth)
    {
        case 0: res->ye = -1;
        case 1: res->mo = -1;
        case 2: res->da = -1;
        case 3: res->ho = -1;
        case 4: res->mi = -1;
        case 5: res->se = -1;
    }

    return res.release();
}

static void mergetime(arki::core::FuzzyTime& dt, const int* time)
{
    dt.mo = dt.mo != -1 ? dt.mo : 1;
    dt.da = dt.da != -1 ? dt.da : 1;
    dt.ho = time[0];
    dt.mi = time[1];
    dt.se = time[2];
}

%}

%union {
	arki::core::FuzzyTime* dtspec;
	int tspec[3];
	struct LexInterval lexInterval;
	int interval[6];
	struct DTMatch* dtmatch;
	char error;
}

%defines
%pure-parser
%name-prefix "arki_reftime"
%lex-param { yyscan_t yyscanner }
%parse-param { void* yyscanner }
%parse-param { arki::matcher::reftime::Parser& state }
//%error-verbose

%start Input
%token <dtspec> DATE
%token <tspec> TIME
%token <lexInterval> INTERVAL STEP
%token <error> UNEXPECTED
%token NOW TODAY YESTERDAY TOMORROW AGO BEFORE AFTER AND PLUS MINUS MIDDAY NOON MIDNIGHT
%token GE GT LE LT EQ COMMA
%type <dtspec> Absolute
%type <dtspec> Date
%type <tspec> Daytime
%type <interval> Interval
%type <void> Input
%type <dtmatch> DateExpr TimeExpr

%%

Input	: DateExpr			{ state.add($1); }
        | DateExpr STEP		{ state.add($1); state.add_step($2.val, $2.idx, $1); }
        | TimeExpr			{ state.add($1); }
        | TimeExpr STEP		{ state.add($1); state.add_step($2.val, $2.idx, $1); }
        | STEP				{ state.add_step($1.val, $1.idx); }
		| Input COMMA Input {}
		| error Unexpected  {
								string msg = "before '";
								msg += state.unexpected;
								msg += "'";
								arki_reftimeerror(yyscanner, state, msg.c_str());
								yyclearin;
								YYERROR;
							}
        ;

// Accumulate unexpected characters to generate nicer error messages
Unexpected: UNEXPECTED		{ state.unexpected = string() + $1; }
		 | Unexpected UNEXPECTED
		 					{ state.unexpected += $2; }
		 ;

DateExpr : GE Absolute		{ $$ = DTMatch::createGE($2); }
         | GT Absolute		{ $$ = DTMatch::createGT($2); }
		 | LE Absolute		{ $$ = DTMatch::createLE($2); }
		 | LT Absolute		{ $$ = DTMatch::createLT($2); }
		 | EQ Absolute		{ $$ = DTMatch::createEQ($2); }
		 ;

TimeExpr : GE Daytime		{ $$ = DTMatch::createTimeGE($2); }
         | GT Daytime		{ $$ = DTMatch::createTimeGT($2); }
		 | LE Daytime		{ $$ = DTMatch::createTimeLE($2); }
		 | LT Daytime		{ $$ = DTMatch::createTimeLT($2); }
		 | EQ Daytime		{ $$ = DTMatch::createTimeEQ($2); }
		 ;

Interval : INTERVAL                     { init_interval($$, $1); }
         | Interval INTERVAL            { interval_copy($$, $1); $2.add_to($$); }
         | Interval AND INTERVAL        { interval_copy($$, $1); $3.add_to($$); }
         ;

Absolute : NOW                          { $$ = state.mknow(); }
         | Date                         { $$ = $1; }
         | Date Daytime                 { $$ = $1; mergetime(*$$, $2); }
         | Interval AGO                 { $$ = interval_ago($1, state.tnow); }
         | Interval BEFORE Absolute     { $$ = $3; interval_sub(*$$, $1); }
         | Interval AFTER Absolute      { $$ = $3; interval_add(*$$, $1); }
         | Absolute PLUS Interval       { $$ = $1; interval_add(*$$, $3); }
         | Absolute MINUS Interval      { $$ = $1; interval_sub(*$$, $3); }
         ;

Date     : TODAY                        { $$ = state.mktoday(); }
         | YESTERDAY                    { $$ = state.mkyesterday(); }
         | TOMORROW                     { $$ = state.mktomorrow(); }
         | DATE                         { }
         ;

Daytime : MIDDAY						{ $$[0] = 12; $$[1] = 0; $$[2] = 0; }
        | NOON							{ $$[0] = 12; $$[1] = 0; $$[2] = 0; }
        | MIDNIGHT						{ $$[0] = 0; $$[1] = 0; $$[2] = 0; }
        | TIME							{ }
        ;

%%

void arki_reftimeerror(yyscan_t yyscanner, arki::matcher::reftime::Parser& state, const char* s)
{
	state.errors.push_back(s);
	//tagcoll::TagexprParser::instance()->addError(s);
	//fprintf(stderr, "%s\n", s);
}

namespace arki {
namespace matcher {
namespace reftime {

void Parser::parse(const std::string& str)
{
	for (vector<DTMatch*>::iterator i = res.begin(); i != res.end(); ++i)
		delete *i;
	res.clear();

	yyscan_t scanner;
	arki_reftimelex_init(&scanner);

	YY_BUFFER_STATE buf = arki_reftime_scan_string(str.c_str(), scanner);

	int res = yyparse(scanner, *this);
	arki_reftime_delete_buffer(buf, scanner);
	arki_reftimelex_destroy(scanner);

	switch (res)
	{
		case 0:
			// Parse successful
			break;
		case 1: {
            // Syntax error
            std::stringstream ss;
            ss << "cannot parse '" << str << "': ";
            for (vector<string>::const_iterator i = errors.begin();
                    i != errors.end(); ++i)
            {
                if (i != errors.begin())
                    ss << "; ";
                ss << *i;
            }
            throw std::invalid_argument(ss.str());
        }
        case 2:
            // Out of memory
            throw std::runtime_error("parser out of memory");
        default: {
            // Should never happen
            stringstream ss;
            ss << "cannot parse '" << str << "': Bison parser function returned unexpected value " << res;
            throw std::runtime_error(ss.str());
        }
    }
}

}
}
}
