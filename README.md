Most importantly, borink-db is a light-weight control plane database.

It's designed to run on managed PostgreSQL (think PlanetScale) and to be usable directly in your application code. It's heavily expired by Convex, but more low-level.

It exposes all this using a C API.

Directly in your Rust, Zig or C++ code, it provides:
- Caching and real-time updates



get_property()
- Needs to be fast (i.e cached in memory, updated when )

set_property()
- update db
- notify
