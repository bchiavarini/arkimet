--- Examples on how to use area objects from Lua

-- GRIB area

-- Create from a table
local t = { lat=45000, lon=12000 }
local o = arki_area.grib(t)

-- The shortcut call-function-with-table Lua syntax obviously also works
ensure_equals(o, arki_area.grib{lat=45000, lon=12000})

-- Values are in o.val, which creates a table with all the values
local vals = o.val
ensure_equals(vals.lat, 45000)
ensure_equals(vals.lon, 12000)
ensure_equals(tostring(o), "GRIB(lat=45000, lon=12000)")
