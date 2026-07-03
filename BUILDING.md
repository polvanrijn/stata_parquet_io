# Building the `pq` plugin

The Stata `pq` command is a thin `.ado` wrapper around a Rust plugin
(`pq.plugin`, a platform-native shared library). To build it you compile the
`stata_parquet_io` crate as a `cdylib` and rename the output to `pq.plugin`.

| Platform | Rust target                  | Library file              | Ship as    |
|----------|------------------------------|---------------------------|------------|
| Windows  | `x86_64-pc-windows-msvc`     | `stata_parquet_io.dll`    | `pq.plugin`|
| macOS    | `x86_64-apple-darwin` / `aarch64-apple-darwin` | `libstata_parquet_io.dylib` | `pq.plugin`|
| Linux    | `x86_64-unknown-linux-gnu`   | `libstata_parquet_io.so`  | `pq.plugin`|

The plugin must export three symbols (`pginit`, `stata_call`, `_stata_`); these
come from `#[no_mangle] pub extern "C"` functions in `src/lib.rs` and are
exported automatically on every target.

## Prerequisites

- **Rust** (edition 2021, MSRV **1.93**) — install via <https://rustup.rs>.
- A **C toolchain** and **libclang** — `stata-sys` runs `bindgen` over
  `crates/stata-sys/vendor/stplugin.h`, and `polars-readstat-rs` compiles the
  ReadStat C library.
  - Debian/Ubuntu: `apt install clang llvm-dev libclang-dev`
  - macOS: `xcode-select --install`
  - Windows: the MSVC Build Tools (or run from a Developer prompt)

## Native build (build on the OS you are targeting)

```sh
cargo build --release --target <target-triple>
```

The artifact lands in `target/<target-triple>/release/`. Rename it to
`pq.plugin` and place it alongside `pq.ado` on Stata's adopath.

---

## Cross-compiling to Windows from macOS or Linux (no Windows machine)

The release Windows binary uses the **MSVC** ABI. You can produce that ABI from
macOS/Linux without a Windows box using
[`cargo-xwin`](https://github.com/rust-cross/cargo-xwin), which supplies the
Windows CRT/SDK and drives `clang-cl`. This is exactly how the `dist/pq-windows`
artifact in this repo was produced.

### Option A — inside Docker (recommended; nothing installed on the host)

Requires only Docker. From the repo root:

```sh
docker run --rm -v "$PWD":/work -w /work rust:latest bash -euxc '
  apt-get update
  apt-get install -y --no-install-recommends clang llvm-dev libclang-dev nasm cmake pkg-config
  rustup target add x86_64-pc-windows-msvc
  cargo install cargo-xwin --locked
  cargo xwin build --release --target x86_64-pc-windows-msvc
'
```

Output: `target/x86_64-pc-windows-msvc/release/stata_parquet_io.dll`.

> Tip: mount Docker volumes onto `/usr/local/cargo/registry` and keep `target/`
> on the host to cache the crate registry and object files between builds — the
> first build compiles all of polars (~9 min); later builds only relink.

### Option B — natively on the host (macOS/Linux toolchain)

```sh
# macOS needs the Xcode command-line tools; Linux needs clang/llvm-dev/libclang-dev
rustup target add x86_64-pc-windows-msvc
cargo install cargo-xwin --locked
cargo xwin build --release --target x86_64-pc-windows-msvc
```

### Package the plugin

```sh
cp target/x86_64-pc-windows-msvc/release/stata_parquet_io.dll dist/pq-windows/pq.plugin
cp src/ado/p/pq.ado src/ado/p/pq.sthlp src/ado/p/pq.pkg dist/pq-windows/
```

### Verify the result (from any OS)

```sh
file dist/pq-windows/pq.plugin
# -> PE32+ executable (DLL) (GUI) x86-64, for MS Windows
```

The exported symbols should include `pginit`, `stata_call`, and `_stata_`.

> **Note on `.cargo/config.toml`:** it points the `x86_64-pc-windows-gnu` linker
> at an MSYS2 install (`C:\msys64\...`). That is only for building the **GNU** ABI
> *on Windows* and is irrelevant to the MSVC cross-build above.

---

## Reproducible / multi-platform release builds (CI)

`.github/workflows/build.yml` builds all three platforms on native GitHub
runners (Windows-MSVC, macOS universal Intel+ARM, Linux via an AlmaLinux 8
container for broad glibc compatibility) and assembles the release zips. Trigger
it by pushing a `v*` tag or via **Actions → Build and Release → Run workflow**.
This is the canonical way to cut a full cross-platform release; the local
cross-build above is for producing a single-platform plugin quickly.
