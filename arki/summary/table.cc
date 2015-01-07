/*
 * summary/intern - Intern table to map types to unique pointers
 *
 * Copyright (C) 2015  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include "table.h"
#include "intern.h"
#include <arki/metadata.h>
#include <arki/matcher.h>
#include <arki/types/utils.h>
#include <arki/emitter/memory.h>
#include <arki/summary.h>
#include <wibble/string.h>
#include <algorithm>

using namespace std;
using namespace arki::types;

namespace arki {
namespace summary {

// Metadata Scan Order
const types::Code Table::mso[] = {
        types::TYPE_ORIGIN,
        types::TYPE_PRODUCT,
        types::TYPE_LEVEL,
        types::TYPE_TIMERANGE,
        types::TYPE_AREA,
        types::TYPE_PRODDEF,
        types::TYPE_BBOX,
        types::TYPE_RUN,
        types::TYPE_QUANTITY,
        types::TYPE_TASK

};
const size_t Table::msoSize = sizeof(mso) / sizeof(Code);
int* Table::msoSerLen = 0;

// Reverse mapping
static int* itemMsoMap = 0;
static size_t itemMsoMapSize = 0;

bool Row::operator<(const Row& row) const
{
    for (unsigned i = 0; i < mso_size; ++i)
    {
        if (items[i] < row.items[i]) return true;
        if (items[i] > row.items[i]) return false;
    }
    return false;
}

bool Row::matches(const Matcher& matcher) const
{
    if (!matcher.m_impl) return false;

    const matcher::AND& mand = *matcher.m_impl;
    for (unsigned i = 0; i < mso_size; ++i)
    {
        matcher::AND::const_iterator j = mand.find(Table::mso[i]);
        if (j == mand.end()) continue;
        if (!items[i]) return false;
        if (!j->second->matchItem(*items[i])) return false;
    }
    matcher::AND::const_iterator rm = mand.find(types::TYPE_REFTIME);
    if (rm != mand.end() && !rm->second->matchItem(*stats.reftimeMerger.makeReftime()))
        return false;
    return true;
}

void Row::dump(std::ostream& out, unsigned indent) const
{
    string head(indent, ' ');

    size_t max_tag = 0;
    for (unsigned i = 0; i < mso_size; ++i)
        max_tag = max(max_tag, types::tag(Table::mso[i]).size());

    for (unsigned i = 0; i < mso_size; ++i)
    {
        string tag = types::tag(Table::mso[i]);
        out << head;
        for (unsigned j = 0; j < max_tag - tag.size(); ++j)
            out << ' ';
        out << tag << " ";
        if (items[i])
            out << *items[i] << endl;
        else
            out << "--" << endl;
    }
}

Table::Table()
    : interns(new TypeIntern[Table::msoSize]), rows(0), row_count(0), row_capacity(0)
{
    buildMsoSerLen();
    buildItemMsoMap();
}

Table::~Table()
{
    delete[] interns;
    free(rows);
}

bool Table::equals(const Table& table) const
{
    if (row_count != table.row_count) return false;
    for (unsigned ri = 0; ri < row_count; ++ri)
        for (unsigned ci = 0; ci < msoSize; ++ci)
            if (!Type::nullable_equals(rows[ri].items[ci], table.rows[ri].items[ci]))
                return false;
    return true;
}

const types::Type* Table::intern(unsigned pos, std::auto_ptr<types::Type> item)
{
    return interns[pos].intern(item);
}

void Table::merge(const Metadata& md)
{
    merge(md, Stats(md));
}

void Table::merge(const Metadata& md, const Stats& st)
{
    Row new_row(st);
    for (size_t i = 0; i < msoSize; ++i)
    {
        const Type* item = md.get(mso[i]);
        if (item)
            new_row.items[i] = interns[i].intern(*item);
        else
            new_row.items[i] = 0;
    }

    merge(new_row);
}

void Table::merge(const std::vector<const Type*>& md, const Stats& st)
{
    Row new_row(st);
    for (size_t i = 0; i < msoSize; ++i)
    {
        if (i < md.size() && md[i])
            new_row.items[i] = interns[i].intern(*md[i]);
        else
            new_row.items[i] = 0;
    }

    merge(new_row);
}

void Table::merge(const std::vector<const types::Type*>& md, const Stats& st, const vector<unsigned>& positions)
{
    Row new_row(st);
    new_row.set_to_zero();
    for (vector<unsigned>::const_iterator i = positions.begin(); i != positions.end(); ++i)
    {
        if (*i < md.size() && md[*i])
            new_row.items[*i] = interns[*i].intern(*md[*i]);
        else
            new_row.items[*i] = 0;
    }

    merge(new_row);
}

void Table::merge(const emitter::memory::Mapping& m)
{
    using namespace emitter::memory;

    auto_ptr<summary::Stats> stats = summary::Stats::decodeMapping(
            m["summarystats"].want_mapping("parsing summary item stats"));
    Row new_row(*stats);
    new_row.set_to_zero();

    for (size_t pos = 0; pos < msoSize; ++pos)
    {
        types::Code code = summary::Visitor::codeForPos(pos);
        const Node& n = m[types::tag(code)];
        if (n.is_mapping())
            new_row.items[pos] = interns[pos].intern(types::decodeMapping(code, n.get_mapping()));
    }

    merge(new_row);
}

bool Table::merge_yaml(std::istream& in, const std::string& filename)
{
    using namespace wibble::str;

    Row new_row;
    new_row.set_to_zero();
    YamlStream yamlStream;
    for (YamlStream::const_iterator i = yamlStream.begin(in);
            i != yamlStream.end(); ++i)
    {
        types::Code type = types::parseCodeName(i->first);
        switch (type)
        {
            case types::TYPE_SUMMARYITEM:
                {
                    stringstream in(i->second, ios_base::in);
                    YamlStream yamlStream;
                    for (YamlStream::const_iterator i = yamlStream.begin(in);
                            i != yamlStream.end(); ++i)
                    {
                        types::Code type = types::parseCodeName(i->first);
                        int pos = summary::Visitor::posForCode(type);
                        if (pos < 0)
                            throw wibble::exception::Consistency("parsing summary item", "found element of unsupported type " + types::formatCode(type));
                        new_row.items[pos] = interns[pos].intern(types::decodeString(type, i->second));
                    }
                }
                break;
            case types::TYPE_SUMMARYSTATS:
            {
                new_row.stats = *Stats::decodeString(i->second);
                merge(new_row);
                new_row.set_to_zero();
                break;
            }
            default:
                throw wibble::exception::Consistency("parsing file " + filename,
                    "cannot handle element " + fmt(type));
        }
    }
    return !in.eof();
}

void Table::ensure_we_can_add_one()
{
    if (row_count + 1 >= row_capacity)
    {
        unsigned new_capacity = row_capacity == 0 ? 16 : row_capacity * 2;
        Row* new_rows = (Row*)realloc(rows, new_capacity * sizeof(Row));
        if (!new_rows)
            throw wibble::exception::System("cannot allocate memory for summary table");
        rows = new_rows;
        row_capacity = new_capacity;
    }
}

void Table::merge(const Row& row)
{
//    cerr << "MERGE " << this << " cur_size: " << row_count << " [" << rows << ", " << (rows + row_count) << ")" << " stats count " << row.stats.count << endl;
//    row.dump(cerr);
    // Find the insertion point
    //
    // This works well even in case rows == 0, since it works in the [0, 0)
    // range, returning 0 and later matching the append case
    Row* pos = lower_bound(rows, rows + row_count, row);

//    cerr << " INSERTION POINT " << (pos - rows) << endl;

    if (pos == rows + row_count)
    {
//        cerr << " APPEND" << endl;
        // Append
        ensure_we_can_add_one();
        // Use placement new instead of assignment, otherwise the vtable of
        // stats will not be initialized
        // FIXME: simplify Stats not to be a type?
        new(rows + row_count++) Row(row);
    } else if (*pos == row) {
//        cerr << " MERGE" << endl;
        // Just merge stats
        pos->stats.merge(row.stats);
    } else {
//        cerr << " INSERT" << endl;
        // Insert
        unsigned idx = pos - rows;
        // Use the array position since we may reallocate, invalidating the
        // previous pointer
        ensure_we_can_add_one();
        memmove(rows + idx + 1, rows + idx, row_count - idx);
        new(rows + idx) Row(row);
        ++row_count;
    }
    stats.merge(row.stats);
}

void Table::dump(std::ostream& out) const
{
}

bool Table::visitItem(size_t msoidx, ItemVisitor& visitor) const
{
    const TypeIntern& intern = interns[msoidx];
    for (TypeIntern::const_iterator i = intern.begin(); i != intern.end(); ++i)
        if (!visitor(**i))
            return false;
    return true;
}

bool Table::visit(Visitor& visitor) const
{
    vector<const Type*> visitmd;
    visitmd.resize(msoSize);

    for (unsigned ri = 0; ri < row_count; ++ri)
    {
        // Set this node's metadata in visitmd
        // FIXME: change the visitor API to just get a const Type* const* and
        //        assume it's msoSize long
        for (size_t i = 0; i < msoSize; ++i)
            visitmd[i] = rows[ri].items[i];

        if (!visitor(visitmd, rows[ri].stats))
            return false;
    }

    return true;
}

bool Table::visitFiltered(const Matcher& matcher, Visitor& visitor) const
{
    vector<const Type*> visitmd;
    visitmd.resize(msoSize);

    for (unsigned ri = 0; ri < row_count; ++ri)
    {
        if (!rows[ri].matches(matcher)) continue;

        // FIXME: change the visitor API to just get a const Type* const* and
        //        assume it's msoSize long
        for (size_t i = 0; i < msoSize; ++i)
            visitmd[i] = rows[ri].items[i];

        if (!visitor(visitmd, rows[ri].stats))
            return false;
    }

    return true;
}

void Table::buildMsoSerLen()
{
    if (msoSerLen) return;
    msoSerLen = new int[msoSize];
    for (size_t i = 0; i < msoSize; ++i)
    {
        const types::MetadataType* mdt = types::MetadataType::get(mso[i]);
        msoSerLen[i] = mdt ? mdt->serialisationSizeLen : 0;
    }
}
void Table::buildItemMsoMap()
{
    if (itemMsoMap) return;

    for (size_t i = 0; i < msoSize; ++i)
        if ((size_t)mso[i] > itemMsoMapSize)
            itemMsoMapSize = (size_t)mso[i];
    itemMsoMapSize += 1;

    itemMsoMap = new int[itemMsoMapSize];
    for (size_t i = 0; i < itemMsoMapSize; ++i)
        itemMsoMap[i] = -1;
    for (size_t i = 0; i < msoSize; ++i)
        itemMsoMap[(size_t)mso[i]] = i;
}

types::Code Visitor::codeForPos(size_t pos)
{
    return Table::mso[pos];
}

int Visitor::posForCode(types::Code code)
{
    if ((size_t)code >= itemMsoMapSize) return -1;
    return itemMsoMap[(size_t)code];
}

}
}
