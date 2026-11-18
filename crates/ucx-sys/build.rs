//! Locates a UCX install (never builds it) and generates bindings.
//! Search order: `$UCX_DIR` → `<workspace>/.ucx/release` → pkg-config (system).

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-env-changed=UCX_DIR");
    println!("cargo:rerun-if-changed=wrapper.h");

    let include_dirs = locate_ucx();

    let mut builder = bindgen::Builder::default()
        .header("wrapper.h")
        .allowlist_function("ucp_.*|ucs_.*")
        .allowlist_type("ucp_.*|ucs_.*")
        .allowlist_var("UCP_.*|UCS_.*")
        .derive_default(true)
        .prepend_enum_name(false)
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()));
    for dir in &include_dirs {
        builder = builder.clang_arg(format!("-I{}", dir.display()));
    }

    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("bindings.rs");
    builder
        .generate()
        .expect("bindgen failed on ucp.h")
        .write_to_file(out)
        .expect("failed to write bindings");
}

/// Emits link directives and returns the include dirs to hand to bindgen.
fn locate_ucx() -> Vec<PathBuf> {
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let local = manifest.join("../../.ucx/release");
    println!("cargo:rerun-if-changed={}", local.join("lib").display());

    let prefix = env::var_os("UCX_DIR").map(PathBuf::from).or_else(|| {
        local
            .join("include/ucp/api/ucp.h")
            .exists()
            .then_some(local)
    });

    if let Some(prefix) = prefix {
        let prefix = prefix.canonicalize().expect("UCX prefix does not exist");
        let lib = prefix.join("lib");
        // Runtime lookup is via the RUNPATH set in .cargo/config.toml (overridable with
        // LD_LIBRARY_PATH, e.g. to swap in .ucx/debug).
        println!("cargo:rustc-link-search=native={}", lib.display());
        println!("cargo:rustc-link-lib=dylib=ucp");
        println!("cargo:rustc-link-lib=dylib=ucs");
        // Exposed to dependents as DEP_UCP_ROOT / DEP_UCP_LIB.
        println!("cargo:root={}", prefix.display());
        println!("cargo:lib={}", lib.display());
        return vec![prefix.join("include")];
    }

    let lib = pkg_config::Config::new()
        .atleast_version("1.12")
        .probe("ucx")
        .expect(
            "UCX not found: run scripts/build-ucx.sh, set UCX_DIR, or install a system UCX package",
        );
    lib.include_paths
}
