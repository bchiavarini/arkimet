.. _segments-concat:

concat segments
===============

This kind of segment stores all data in a single file, simply concatenated one
after the other.

It is suitable for formats whose workflow naturally concatenates data, such as
GRIB or BUFR.

The file name represents the period of time stored in the segment. The file
extension represents the format of the data it contains.


Optional padding
----------------

For VM2 data, which is a line-based format, the segment supports padding data
with a newline. The newline is not considered part of the data, and it still
gets written, skipped on read, and not considered as a gap during checks.


.. toctree::
   :maxdepth: 2
   :caption: Contents:
