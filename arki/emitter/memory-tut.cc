/*
 * Copyright (C) 2010  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/tests/test-utils.h>
#include <arki/emitter/memory.h>

namespace tut {
using namespace std;
using namespace arki;
using namespace arki::emitter;

struct arki_emitter_memory_shar {
    arki_emitter_memory_shar()
    {
    }
};
TESTGRP(arki_emitter_memory);

// null value
template<> template<>
void to::test<1>()
{
    Memory m;
    m.add_null();

    ensure_equals(m.root().tag(), "null");
    ensure(m.root().is_null());
}

// bool value
template<> template<>
void to::test<2>()
{
    {
        Memory m;
        m.add(true);
        ensure_equals(m.root().tag(), "bool");
        ensure(m.root().is_bool());
        ensure_equals(m.root().get_bool(), true);
    }
    {
        Memory m;
        m.add(false);
        ensure_equals(m.root().tag(), "bool");
        ensure(m.root().is_bool());
        ensure_equals(m.root().get_bool(), false);
    }
}

// int value
template<> template<>
void to::test<3>()
{
    Memory m;
    m.add(42);
    ensure_equals(m.root().tag(), "int");
    ensure(m.root().is_int());
    ensure_equals(m.root().get_int(), 42);
}

// double value
template<> template<>
void to::test<4>()
{
    {
        Memory m;
        m.add(1.0);
        ensure_equals(m.root().tag(), "double");
        ensure(m.root().is_double());
        ensure_equals(m.root().get_double(), 1.0);
    }

    {
        Memory m;
        m.add(0.1);
        ensure_equals(m.root().tag(), "double");
        ensure(m.root().is_double());
        ensure_equals(m.root().get_double(), 0.1);
    }
}

// string value
template<> template<>
void to::test<5>()
{
    {
        Memory m;
        m.add_string("");
        ensure_equals(m.root().tag(), "string");
        ensure(m.root().is_string());
        ensure_equals(m.root().get_string(), "");
    }

    {
        Memory m;
        m.add("antani");
        ensure_equals(m.root().tag(), "string");
        ensure(m.root().is_string());
        ensure_equals(m.root().get_string(), "antani");
    }
}

// list value
template<> template<>
void to::test<6>()
{
    Memory m;
    m.start_list();
      m.add("antani");
      m.start_list();
        m.add("blinda");
        m.add("supercazzola");
      m.end_list();
      m.add("tapioca");
    m.end_list();

    ensure_equals(m.root().tag(), "list");
    ensure(m.root().is_list());
    const memory::List& l = m.root().get_list();

    ensure_equals(l.empty(), false);
    ensure_equals(l.size(), 3u);
    ensure_equals(l[0].is_string(), true);
    ensure_equals(l[0].get_string(), "antani");
    ensure_equals(l[1].is_list(), true);
    ensure_equals(l[1].get_list().empty(), false);
    ensure_equals(l[1].get_list().size(), 2u);
    ensure_equals(l[1].get_list()[0].get_string(), "blinda");
    ensure_equals(l[1].get_list()[1].get_string(), "supercazzola");
    ensure_equals(l[2].get_string(), "tapioca");
}

// mapping value
template<> template<>
void to::test<7>()
{
    Memory m;
    m.start_mapping();
      m.add("antani");
      m.start_list();
        m.add("blinda");
        m.add("supercazzola");
      m.end_list();
      m.add("tapioca");
      m.start_mapping();
        m.add("blinda");
        m.add("supercazzola");
      m.end_mapping();
    m.end_mapping();

    ensure_equals(m.root().tag(), "mapping");
    ensure(m.root().is_mapping());
    const memory::Mapping& i = m.root().get_mapping();

    ensure_equals(i.empty(), false);
    ensure_equals(i.size(), 2u);
    ensure_equals(i["antani"].is_list(), true);
    ensure_equals(i["antani"].get_list()[0].get_string(), "blinda");
    ensure_equals(i["antani"].get_list()[1].get_string(), "supercazzola");
    ensure_equals(i["tapioca"].is_mapping(), true);
    ensure_equals(i["tapioca"].get_mapping().size(), 1u);
    ensure_equals(i["tapioca"].get_mapping()["blinda"].get_string(), "supercazzola");
}

}

// vim:set ts=4 sw=4: