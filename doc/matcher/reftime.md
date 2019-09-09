--- Matching reference times

local sample = arki.metadata.new()

--[[

Syntax: reftime:<operator><date>[,<operator><date>...]

A reftime matcher is a sequence of expressions separated by a comma. All the
expressions are ANDed together.

Valid operators are: >=, >, <=, <, ==, =

For example: "reftime:=2010-05-03 12:00:00" matches midday on the 3rd of May.

A date can be specified partially, and it will be considered as a time
interval. For example, "reftime:=2007-01" matches from midnight of the 1st of
January 2007 to 23:59:59 of the 31st of January 2007, inclusive.

It is also possible to specify the time, without the date: ">=12", ">=12:00"
and ">=12:00:00" all "mean between midday and midnight of any day". ">12" means
"from 13:00:00 until midnight". "=12" means "from 12:00:00 to 12:59:59".

Finally, it is possible to specify a time step: ">=2007-06-05 04:00:30%12h"
means "anything from 04:00:30 June 5, 2007, but only at precisely 04:00:30 and
18:00:30".

The time step can be given in hours, minutes, seconds or any of their
combinations. For example: "12h", "30m", "12h30m15s".

Dates and times can also be specified relative to the curren time:

 now: current date and time
 today: current date

There are also aliases for commonly used bits:
]]

ensure_matchers_equal("reftime:=yesterday", "reftime:=today - 1 day")
ensure_matchers_equal("reftime:=tomorrow",  "reftime:=today + 1 day")
ensure_matchers_equal("reftime:=midday",    "reftime:=12:00:00")
ensure_matchers_equal("reftime:=noon",      "reftime:=12:00:00")
ensure_matchers_equal("reftime:=midnight",  "reftime:=00:00:00")

-- Dates can be altered using +, -, 'ago', 'before', 'after'. For example:

ensure_matchers_equal("reftime:=2 days before yesterday",   "reftime:=today - 3 days")
ensure_matchers_equal("reftime:=1 hour and 30 minutes after tomorrow midnight",     "reftime:=tomorrow 01:30:00")
ensure_matchers_equal("reftime:=1 hour after today midday", "reftime:=today 13:00:00")
ensure_matchers_equal("reftime:=processione san luca 2010", "reftime:=34 days after easter 2010")

-- You can use arki-dump --query 'reftime:...' to check how a query gets parsed.

sample:set(arki_reftime.position(arki_time.sql("2010-09-08 07:06:05")))

-- Year intervals
ensure_matches(sample, "reftime:>=2010")
ensure_matches(sample, "reftime:<=2010")
ensure_matches(sample, "reftime:==2010")
ensure_matches(sample, "reftime:>2009")
ensure_matches(sample, "reftime:<2011")
ensure_not_matches(sample, "reftime:<2010")
ensure_matches(sample, "reftime:>2009,<2011")
ensure_not_matches(sample, "reftime:>2009,<2010")

-- Month intervals
ensure_matches(sample, "reftime:>=2010-09")
ensure_matches(sample, "reftime:<=2010-09")
ensure_matches(sample, "reftime:==2010-09")
ensure_matches(sample, "reftime:>2010-08")
ensure_matches(sample, "reftime:<2010-10")
ensure_not_matches(sample, "reftime:<2010-09")
ensure_matches(sample, "reftime:>2010-08,<2010-10")
ensure_not_matches(sample, "reftime:>2010-08,<2010-09")

-- Day intervals
ensure_matches(sample, "reftime:>=2010-09-08")
ensure_matches(sample, "reftime:<=2010-09-08")
ensure_matches(sample, "reftime:==2010-09-08")
ensure_matches(sample, "reftime:>2010-09-07")
ensure_matches(sample, "reftime:<2010-09-09")
ensure_not_matches(sample, "reftime:<2010-09-08")
ensure_matches(sample, "reftime:>2010-09-07,<2010-09-09")
ensure_not_matches(sample, "reftime:>2010-09-07,<2010-09-08")

-- Hour intervals
ensure_matches(sample, "reftime:>=2010-09-08 07")
ensure_matches(sample, "reftime:<=2010-09-08 07")
ensure_matches(sample, "reftime:==2010-09-08 07")
ensure_matches(sample, "reftime:>2010-09-08 06")
ensure_matches(sample, "reftime:<2010-09-08 08")
ensure_not_matches(sample, "reftime:<2010-09-08 07")
ensure_matches(sample, "reftime:>2010-09-08 06,<2010-09-08 08")
ensure_not_matches(sample, "reftime:>2010-09-08 06,<2010-09-08 07")

-- Minute intervals
ensure_matches(sample, "reftime:>=2010-09-08 07:06")
ensure_matches(sample, "reftime:<=2010-09-08 07:06")
ensure_matches(sample, "reftime:==2010-09-08 07:06")
ensure_matches(sample, "reftime:>2010-09-08 07:05")
ensure_matches(sample, "reftime:<2010-09-08 07:07")
ensure_not_matches(sample, "reftime:<2010-09-08 07:06")
ensure_matches(sample, "reftime:>2010-09-08 07:05,<2010-09-08 07:07")
ensure_not_matches(sample, "reftime:>2010-09-08 07:05,<2010-09-08 07:06")

-- Precise timestamps
ensure_matches(sample, "reftime:>=2010-09-08 07:06:05")
ensure_matches(sample, "reftime:<=2010-09-08 07:06:05")
ensure_matches(sample, "reftime:==2010-09-08 07:06:05")
ensure_matches(sample, "reftime:>2010-09-08 07:06:04")
ensure_matches(sample, "reftime:<2010-09-08 07:06:06")
ensure_not_matches(sample, "reftime:<2010-09-08 07:06:05")
ensure_matches(sample, "reftime:>2010-09-08 07:06:04,<2010-09-08 07:06:06")
ensure_not_matches(sample, "reftime:>2010-09-08 07:05:04,<2010-09-08 07:06:05")

-- Hour expressions
ensure_matches(sample, "reftime:=07")
ensure_matches(sample, "reftime:>06")
ensure_matches(sample, "reftime:<08")
ensure_not_matches(sample, "reftime:>07") -- Same as >=08
ensure_matches(sample, "reftime:=07:06")
ensure_matches(sample, "reftime:>07:05")
ensure_matches(sample, "reftime:<08:00")
ensure_not_matches(sample, "reftime:>07:06") -- Same as >=07:07
ensure_matches(sample, "reftime:=07:06:05")
ensure_matches(sample, "reftime:>07:06:04")
ensure_matches(sample, "reftime:<07:06:06")
ensure_not_matches(sample, "reftime:>07:06:05")
ensure_not_matches(sample, "reftime:<07:06:05")

-- Time steps
ensure_matches(sample, "reftime:>=2010-09-08 %5s")
ensure_matches(sample, "reftime:%5s")
ensure_not_matches(sample, "reftime:%2s")