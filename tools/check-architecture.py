#!/usr/bin/env python3
import pathlib
import sys
import tomllib


ROOT = pathlib.Path(__file__).resolve().parents[1]
RPC_REV = "af8d04dbbaee05fa1514caa07a37808d5973d100"
OAUTH_REV = "1b0043f27cc445e9d238a643ab32fd45e143ea6e"
FORBIDDEN_STAGE_THREE = (
    "coop.request_moq_ingest",
    "moq-relay",
)


def fail(message: str) -> None:
    print(f"architecture check failed: {message}", file=sys.stderr)
    raise SystemExit(1)


manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
dependencies = manifest["dependencies"]
for name in ("json-rpc-2-backend", "json-rpc-2-runtime", "json-rpc-2-server"):
    dependency = dependencies.get(name)
    if not isinstance(dependency, dict) or dependency.get("rev") != RPC_REV:
        fail(f"{name} must use pinned JSON-RPC-2 revision {RPC_REV}")
    if dependency.get("git") != "https://github.com/vsvictor/JSON-RPC-2.git":
        fail(f"{name} must come from the JSON-RPC-2 repository")

oauth = dependencies.get("json-rpc-2-oauth2")
if not isinstance(oauth, dict) or oauth.get("rev") != OAUTH_REV:
    fail(f"OAuth2 must use pinned revision {OAUTH_REV}")

patches = manifest.get("patch", {}).get("crates-io", {})
for crate in ("h3", "h3-quinn"):
    patch = patches.get(crate)
    if not isinstance(patch, dict) or patch.get("git") != "https://github.com/hyperium/h3":
        fail(f"missing ecosystem-compatible {crate} Cargo patch")

source_files = sorted((ROOT / "src").rglob("*.rs"))
source = "\n".join(path.read_text() for path in source_files)
for forbidden in FORBIDDEN_STAGE_THREE:
    if forbidden in source:
        fail(f"Stage 3 implementation leaked into source: {forbidden}")

for scope in ("tracking.ingest", "tracking.server_time", "coop.sync_peers"):
    if f'scopes = "{scope}"' not in source:
        fail(f"missing #[authorize] guard for {scope}")
if "pub trait SessionAuthorizer" not in source:
    fail("session authorization must be an injected host port")
if "default_allow" in source or "allow_all" in source:
    fail("permissive authorization implementation is forbidden")
if "pub trait CooperativeStorage" not in source or "pub trait TransitSource" not in source:
    fail("cooperative storage and transit must be injected host ports")
rpc = (ROOT / "src/rpc.rs").read_text()
if 'ctx.transport == "quic"' not in rpc:
    fail("cooperative RPC must be gated on server transport context")
for path in source_files:
    if "storage" in path.relative_to(ROOT / "src").parts and path.name != "tests.rs":
        if any(command in path.read_text() for command in ("GEOADD", "GEOSEARCH")):
            fail("cooperative geo storage must not leak into Stage 1 storage")
    if "#[test]" in path.read_text() or "#[tokio::test]" in path.read_text():
        if path.name != "tests.rs":
            fail(f"tests should reside in a separate tests.rs: {path}")

print(
    "architecture check passed: pinned plugin boundary, explicit auth scopes, "
    "QUIC-only cooperative RPC, and no Stage 3 implementation"
)
