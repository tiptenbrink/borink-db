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

## Development

Use the Nix development shell for local builds and editor support:

```sh
nix develop
```

The shell provides the Clang/libc++ toolchain, Wild linker, clangd, LLDB, CMake,
Ninja, nix language servers, libpqxx, llfio, and both release/debug mimalloc
prefixes used by the CMake presets.

### CMake presets

Configure once per build directory:

```sh
cmake --preset nix-clang
cmake --preset nix-clang-debug
```

Build after source changes:

```sh
cmake --build --preset nix-clang
cmake --build --preset nix-clang-debug
```

The presets write separate build trees:

- `build/` uses `RelWithDebInfo` and release mimalloc.
- `build-debug/` uses `Debug` and debug mimalloc.

The debug preset is the one used by the Zed debugger configuration.

### Nix package builds

Build the default release package:

```sh
nix build
```

Build explicit variants:

```sh
nix build .#release --out-link result-release
nix build .#debug --out-link result-debug
```

The release binary links `libmimalloc.so.3`; the debug binary links
`libmimalloc-debug.so.3`. Both are built with debug info and are not stripped.

`libpqxx` and `llfio` are separate Nix derivations pinned by flake inputs, so
normal source changes and version bumps in this project do not rebuild those
dependencies from scratch.

### Zed and LLDB

Start Zed from the development shell so it inherits the Nix toolchain and
runtime paths:

```sh
nix develop
zed .
```

Before launching the debugger, build the debug preset:

```sh
cmake --build --preset nix-clang-debug
```

Then run the `borink-db debug` launch configuration in Zed. It uses CodeLLDB and
starts `build-debug/borink_db`.

### mimalloc debug output

The Zed debug launch enables:

```sh
MIMALLOC_VERBOSE=1
```

This confirms which mimalloc variant is loaded. In debug runs, expected output
includes:

```text
mimalloc: v3.1.5, debug
mimalloc: debug level : 3
```

You may also see a warning like:

```text
mimalloc: warning: unable to allocate aligned OS memory directly, fall back to over-allocation
```

This is expected on Linux for some aligned allocations. mimalloc first tries a
direct aligned OS mapping; when Linux does not return a suitably aligned address,
it falls back to over-allocation, aligns inside the mapped range, and unmaps the
unused parts. Alignment is still guaranteed.
