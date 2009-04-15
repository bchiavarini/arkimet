#ifndef ARKI_DATASET_ONDISK2_H
#define ARKI_DATASET_ONDISK2_H

/*
 * dataset/ondisk2 - Local on disk dataset
 *
 * Copyright (C) 2007,2008,2009  ARPA-SIM <urpsim@smr.arpa.emr.it>
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

#include <arki/dataset/ondisk2/reader.h>
#include <arki/dataset/ondisk2/writer.h>

/**
 * Generic dataset interface.
 *
 * Archived data is stored in datasets.
 *
 * A dataset is a group of data with similar properties, like the output of a
 * given forecast model, or images from a given satellite.
 *
 * Every dataset has a dataset name.
 *
 * Every dataset has a dataset index to speed up retrieval of subsets of
 * informations from it.
 *
 * The dataset index needs to be simple and fast, but does not need to support
 * a complex range of queries.
 *
 * The data is stored in the dataset without alteration from its original
 * form.
 *
 * It must be possible to completely regenerate the dataset index by
 * rescanning all the data stored in the dataset.
 *
 * A consequence of the last two points is that a dataset can always be fully
 * regenerated by just reimporting data previously extracted from it.
 *
 * A dataset must be fully independent from other datasets, and not must not hold
 * any information about them.
 *
 * A dataset should maintain a summary of its contents, that lists the
 * various properties of the data it contains.
 *
 * Since the dataset groups data with similar properties, the summary is intended
 * to change rarely.
 *
 * An archive index can therefore be built, indexing all dataset summaries,
 * to allow complex data searches across datasets.
 *
 * The dataset contains: (needs review)
 * \l files with the data, and an extension depending on the data type
 * \l flagfiles for failed updates, associated to the data files, with the extra extension .appending
 * \l flagfiles marking datafiles that need repack, associated to the data
 *    files, with the extra extension .needs-repack
 * \l files with the summary, one per data file, with the extra extension .summary
 * \l files with the summary, one per directory, called 'summary'
 * \l sqlite index, only on the toplevel directory, called 'index.sqlite'
 *
 * When the .appending flagfile is present: (needs review)
 * \l no modifications are allowed to start
 * \l the summary file will not be generated
 * \l the flagfile can be removed only after rescanning the data file and
 *    rebuilding the metadata
 * \l the dataset and the summary can still be used for querying, as in the
 *    worse case newly appended metadata are missed by the summary or the summary
 *    mentions deleted metadata, and everything is fixed as soon as the
 *    datafile is rescanned.
 *
 * To append to the dataset: (needs review)
 * \l if .appending flagfile exists, abort; flagfile can be removed only after
 *    rescanning the data file and rebuilding the metadata
 * \l create .appending flagfile
 * \l append of the data
 * \l append of the metadata
 * \l append to the index "journal"
 * \l (repeat more than once, for performance)
 * \l flush and remove .appending flagfile
 * \l update summary, propagating in parent dirs (delayed)
 * \l update index from the "journal" (delayed)
 *
 * To remove from the dataset: (needs review)
 * \l if .appending flagfile exists, abort; flagfile can be removed only after
 *    rescanning the data file and rebuilding the metadata
 * \l create .appending flagfile
 * \l touch datafile.needs-repack
 * \l mark as deleted in the metadata
 * \l append to the index "journal"
 * \l (repeat more than once, for performance)
 * \l flush and remove .appending flagfile
 * \l update summary, propagating in parent dirs (delayed)
 * \l update index from the "journal" (delayed)
 *
 * To replace in the dataset: (needs review)
 * \l if .appending flagfile exists, abort; flagfile can be removed only after
 *    rescanning the data file and rebuilding the metadata
 * \l create .appending flagfile
 * \l append of the data
 * \l append of the metadata
 * \l touch datafile.needs-repack
 * \l mark as deleted in the metadata
 * \l append to the index "journal"
 * \l (repeat more than once, for performance)
 * \l flush and remove .appending flagfile
 * \l update summary, propagating in parent dirs (delayed)
 * \l update index from the "journal" (delayed)
 *
 * To pack a dataset (needs review):
 * \l go through all metadata file next to .needs-repack files looking for
 *    'delete' records
 * \l when a delete record is found, pack the file
 *
 * To pack a file (needs review):
 * \l go through the metadata copying all data relative to non-delete records
 *    into a temporary data file, and the relative metadata into a temporary
 *    metadata file
 * \l touch flagfile that marks 'critical updates happening'
 * \l atomically rename the temporary data file to overwrite the old one
 * \l atomically rename the temporary metadata file to overwrite the old one
 * \l append to the index "journal" info about the moved metadata
 * \l flush and remove flagfile
 * \l update index from the "journal" (delayed)
 *
 * To check/repair a dataset (needs review):
 * \l if a data file is newer than its metadata file, rescan the data file
 * \l if a metadata file is newer than the summary, rebuild the summary
 * \l if a summary is newer than the parent summary, rebuild the parent summary
 * \l if there is the "critical updates happening" flagfile, and a metadata
 *    file is newer than the index journal or, if the journal is missing, the
 *    index, delete from the index all the records that point to metadata in
 *    that metadata file and reindex the file
 * \l if the index journal is newer than the index, update the index with the
 *    journal
 */

// vim:set ts=4 sw=4:
#endif
