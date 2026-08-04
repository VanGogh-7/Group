# Release Runbook

This is the authoritative manual procedure for the eight-crate `0.1.0`
family. It separates local preparation from a clean committed candidate and
from irreversible registry work. Passing an earlier phase does not authorize
or prove a later one.

Group is currently unpublished. Do not create a commit, push, tag, GitHub
Release, or crates.io publication unless the User / Product Owner separately
authorizes that exact operation. Never place a crates.io token, provider key,
MCP credential, or other secret in a command line, repository file, captured
diff, shell history, log, archive, or report. Authentication belongs in the
operator's preconfigured credential store and is not exercised by the current
Plan.

## Current evidence boundary

Plan 023 completed Phase 2 for commit
`9b069d430cae02e74134f37edb8d05b83c2cc6c7`: the full local matrix, eight clean
archives, and both required hosted CI jobs passed for that exact SHA. Later
release-facing README changes are package-input changes, so that evidence must
remain historical and cannot establish the final candidate. Plan 024 must bind
a new immutable candidate commit, archive source, local verification, hosted
CI, and sole proposed `v0.1.0` tag target to one identical SHA before Phase 3
can be considered. Phase 3 remains separately authorized and unperformed.

## Fixed crate order

Use this exact order, including between crates that could otherwise be
reordered:

```text
group-agent-core
  -> group-agent-model
  -> group-agent-tool
  -> group-agent-checkpoint-sqlite
  -> group-agent-observability-tokio
  -> group-agent-genai
  -> group-agent-mcp
  -> group-agent-prebuilt
```

The order respects every internal dependency and makes the manual procedure
deterministic. Never skip ahead to a dependent crate.

## Phase 1: dirty local package preflight

This phase is the bounded diagnostic gate used by Plan 022. It may use
`--allow-dirty`, generates archives locally, and never publishes. It proves
only the exact captured working tree; it is not a clean release-candidate gate
or registry-resolution proof.

Before the first `--allow-dirty` command, capture every tracked, staged,
unstaged, and untracked input. Use a fresh evidence directory outside the
repository and retain its resolved path:

```bash
release_evidence_dir="$(mktemp -d /tmp/group-release-preflight.XXXXXX)"
realpath "$release_evidence_dir"
git rev-parse HEAD > "$release_evidence_dir/head.txt"
git status --short > "$release_evidence_dir/status.txt"
git diff --binary > "$release_evidence_dir/unstaged.diff"
git diff --cached --binary > "$release_evidence_dir/staged.diff"
git ls-files --others --exclude-standard -z \
  > "$release_evidence_dir/untracked-files.zlist"
: > "$release_evidence_dir/untracked.diff"
while IFS= read -r -d '' untracked_file; do
  git diff --no-index --binary -- /dev/null "$untracked_file" \
    >> "$release_evidence_dir/untracked.diff" || test "$?" -eq 1
done < "$release_evidence_dir/untracked-files.zlist"
sha256sum "$release_evidence_dir"/* \
  > "$release_evidence_dir/evidence.sha256"
```

Review the complete captured material and record the authorized exception set.
After capture, compare the saved package-input hash set before accepting any
result. If a package-relevant input changes, stop and repeat archive generation
and inspection for every affected package; changes to shared workspace
metadata, `Cargo.lock`, root README, or canonical licenses require rechecking
all affected packages. A writeback-only change to this runbook,
`docs/quality.md`, or the active Plan may follow the preflight only when its
before/after hash is recorded, package-input hashes are unchanged, and every
package list proves the changed evidence file is absent. Preserve a
supplemental status and diff with the evidence rather than treating writeback
as part of the tested archive source.

Generate in fixed order. While the internal `0.1.0` versions are unpublished,
Cargo still resolves the archive lockfile against crates.io even with
`--no-verify`. Supply the unpublished dependency layers as non-persistent
command-line patches with absolute local paths. Core needs no patch; each
dependent command uses only the smallest layer that covers its normal and
development dependencies:

```bash
repo_root="$(git rev-parse --show-toplevel)"
repo_root="$(realpath "$repo_root")"
core_patches=(
  --config "patch.crates-io.group-agent-core.path=\"$repo_root/crates/group-agent-core\""
)
model_patches=(
  "${core_patches[@]}"
  --config "patch.crates-io.group-agent-model.path=\"$repo_root/crates/group-agent-model\""
)
tool_patches=(
  "${model_patches[@]}"
  --config "patch.crates-io.group-agent-tool.path=\"$repo_root/crates/group-agent-tool\""
)

cargo package --locked -p group-agent-core --allow-dirty
cargo package --locked -p group-agent-model --allow-dirty --no-verify \
  "${core_patches[@]}"
cargo package --locked -p group-agent-tool --allow-dirty --no-verify \
  "${model_patches[@]}"
cargo package --locked -p group-agent-checkpoint-sqlite --allow-dirty --no-verify \
  "${core_patches[@]}"
cargo package --locked -p group-agent-observability-tokio --allow-dirty --no-verify \
  "${core_patches[@]}"
cargo package --locked -p group-agent-genai --allow-dirty --no-verify \
  "${model_patches[@]}"
cargo package --locked -p group-agent-mcp --allow-dirty --no-verify \
  "${tool_patches[@]}"
cargo package --locked -p group-agent-prebuilt --allow-dirty --no-verify \
  "${tool_patches[@]}"
```

These patches bootstrap local Cargo resolution only. They must not be written
to repository or package manifests, must be absent from every normalized
archive manifest, and do not prove crates.io resolution. `--no-verify` retains
the diagnostic archive-only boundary for dependents. An operator may also add
`--offline` when all locked third-party dependencies are cached; that proves
only cached local assembly and makes registry resolution even less probative.

For every crate, save and review `cargo package --locked -p <crate>
--allow-dirty --list`. Then inspect the generated archive under
`target/package/<crate>-0.1.0.crate` in its own fresh extraction directory:

```bash
archive="target/package/group-agent-core-0.1.0.crate"
archive_extract_dir="$(mktemp -d /tmp/group-package-inspect.XXXXXX)"
tar -tzf "$archive"
tar -xzf "$archive" -C "$archive_extract_dir"
package_root="$archive_extract_dir/group-agent-core-0.1.0"
test -f "$package_root/Cargo.toml"
test -f "$package_root/Cargo.toml.orig"
test -f "$package_root/README.md"
test -f "$package_root/LICENSE-MIT"
test ! -L "$package_root/LICENSE-MIT"
test -f "$package_root/LICENSE-APACHE"
test ! -L "$package_root/LICENSE-APACHE"
cmp -s LICENSE-MIT "$package_root/LICENSE-MIT"
cmp -s LICENSE-APACHE "$package_root/LICENSE-APACHE"
sha256sum LICENSE-MIT "$package_root/LICENSE-MIT"
sha256sum LICENSE-APACHE "$package_root/LICENSE-APACHE"
python3 - "$package_root/Cargo.toml" <<'PY'
import pathlib
import sys
import tomllib

manifest_path = pathlib.Path(sys.argv[1])
with manifest_path.open("rb") as manifest_file:
    manifest = tomllib.load(manifest_file)

package = manifest.get("package")
required_metadata = (
    "name",
    "version",
    "description",
    "edition",
    "rust-version",
    "license",
    "repository",
    "homepage",
    "readme",
)
if not isinstance(package, dict):
    raise SystemExit(f"{manifest_path}: missing [package] table")
missing = [
    field for field in required_metadata
    if field not in package or package[field] == ""
]
if missing:
    raise SystemExit(
        f"{manifest_path}: missing package metadata: {', '.join(missing)}"
    )

internal_packages = {
    "group-agent-core",
    "group-agent-model",
    "group-agent-tool",
    "group-agent-checkpoint-sqlite",
    "group-agent-observability-tokio",
    "group-agent-genai",
    "group-agent-mcp",
    "group-agent-prebuilt",
}
dependency_tables = {
    "dependencies",
    "dev-dependencies",
    "build-dependencies",
}

def inspect_tables(value, location=()):
    if not isinstance(value, dict):
        return
    for key, child in value.items():
        child_location = (*location, key)
        if key in dependency_tables:
            if not isinstance(child, dict):
                raise SystemExit(
                    f"{manifest_path}: {'.'.join(child_location)} is not a table"
                )
            for alias, dependency in child.items():
                if isinstance(dependency, str):
                    dependency_package = alias
                    version = dependency
                    overrides = []
                elif isinstance(dependency, dict):
                    dependency_package = dependency.get("package", alias)
                    version = dependency.get("version")
                    overrides = [
                        field
                        for field in ("path", "git", "registry", "registry-index")
                        if field in dependency
                    ]
                else:
                    raise SystemExit(
                        f"{manifest_path}: invalid dependency {alias!r} in "
                        f"{'.'.join(child_location)}"
                    )
                if dependency_package in internal_packages:
                    dependency_location = ".".join((*child_location, alias))
                    if version != "0.1.0":
                        raise SystemExit(
                            f"{manifest_path}: {dependency_location} must require "
                            'version = "0.1.0"'
                        )
                    if overrides:
                        raise SystemExit(
                            f"{manifest_path}: {dependency_location} has forbidden "
                            f"source override(s): {', '.join(overrides)}"
                        )
        inspect_tables(child, child_location)

inspect_tables(manifest)
print(f"validated normalized manifest: {manifest_path}")
PY
```

Repeat with the matching archive and extracted root for all eight crates.
Read each complete normalized `Cargo.toml`: every internal dependency must
retain exactly `version = "0.1.0"` and no `path`, `git`, `registry`, or
`registry-index` source override, including in normal, development, build, and
target-specific dependency tables. The Python inspection parses only the
extracted `Cargo.toml`; it neither contacts a registry nor invokes Cargo
resolution. Compare the archive inventory with its package list and intended
crate source. The archive must contain README plus both byte-identical
licenses and must not contain repository-only or generated material.

Run filename and content checks against each extracted package. The content
scan prints filenames, not matching secret values; inspect every hit and
distinguish documented synthetic fixtures from actual credentials:

```bash
if find "$package_root" \
  \( -name .git -o -name .github -o -name '.env*' -o \
     -name .local-notes -o -name target -o -name criterion \) \
  -print -quit | grep -q .; then
  exit 1
fi
secret_pattern='api[_-]?key|access[_-]?token'
secret_pattern="$secret_pattern|authorization[[:space:]]*:[[:space:]]*bearer([[:space:]]|$)"
secret_pattern="$secret_pattern|-----BEGIN ([A-Z0-9]+[[:space:]]+)*PRIVATE KEY-----"
rg -l -i "($secret_pattern)" "$package_root" || true
```

The extended regular expression intentionally matches credential indicators,
not credential values. `rg -l` emits only the names of files containing a
match; do not replace it with matching-line output in release evidence.

Also run `git diff --check`, `./scripts/verify full`, and
`./scripts/verify msrv` for the captured tree. Preserve actual warnings,
skips, hashes, inventories, and outcomes. Remove only a known inspection
directory after checking its `realpath`; never delete a broad or unresolved
path.

## Phase 2: clean committed candidate and hosted CI

This phase requires separate commit and push authorization. Start from the
exact intended commit with an empty `git status --short`. Re-run the complete
verification matrix, package lists, archive generation, extraction,
normalized-manifest checks, license comparisons, and secret/content checks
from Phase 1 without `--allow-dirty`. Record the candidate commit and archive
hashes.

Dependent archive creation still needs `--no-verify` and the Phase 1
command-line patch arrays only while its internal dependencies are unindexed.
Remove each patch layer once the corresponding exact internal versions are
available from crates.io. The clean gate proves archive contents for the
committed candidate, not pre-publication crates.io resolution.

Push only when separately authorized. Require a successful GitHub-hosted
`full` job and layered `msrv` job for the same candidate commit. Local YAML
parsing, local script passes, or a dirty preflight do not establish hosted CI.
Any difference between the tested commit, packaged source, proposed tag, and
hosted-CI commit invalidates the candidate and restarts this phase.

## Phase 3: separately authorized tag and crates.io release

This phase is irreversible external work and requires a new, explicit release
authorization after Phase 2 passes. Immediately recheck ownership and the
exact eight names. `cargo search` returning no exact match is only a hint: it
does not reserve a name or guarantee publication rights.

Prepare the authorized `v0.1.0` tag only on the exact clean candidate commit
and verify the tag target before any publication. Then publish one crate at a
time in the fixed order:

```bash
cargo publish --locked -p group-agent-core
cargo publish --locked -p group-agent-model
cargo publish --locked -p group-agent-tool
cargo publish --locked -p group-agent-checkpoint-sqlite
cargo publish --locked -p group-agent-observability-tokio
cargo publish --locked -p group-agent-genai
cargo publish --locked -p group-agent-mcp
cargo publish --locked -p group-agent-prebuilt
```

Do not run these commands as one unattended script. After each successful
publication, wait for crates.io/index visibility and prove exact-version
resolution from a fresh temporary consumer before proceeding to the next
crate. A minimal check for the just-published crate is:

```bash
registry_check_dir="$(mktemp -d /tmp/group-registry-check.XXXXXX)"
cargo init --quiet --bin "$registry_check_dir/consumer"
cd "$registry_check_dir/consumer"
cargo add --registry crates-io group-agent-core@=0.1.0
cargo check
cargo tree
```

Replace the crate name for each step and use a new directory each time. Confirm
that Cargo resolved the exact crates.io version with no path, Git, patch, or
local registry override. An index lookup alone is insufficient if the fresh
consumer cannot resolve and build the exact version.

After all eight crates are indexed, create one more fresh consumer, add all
eight dependencies with `@=0.1.0` from `crates-io`, run `cargo tree` and
`cargo check`, and retain the lockfile and command evidence. Only this final
check supports the claim that a fresh consumer can use the complete published
family. Tag push and GitHub Release creation remain separately authorized
external operations; perform them only in the order approved by the User.

## Stop conditions

Stop immediately and do not skip ahead, retry publication blindly, or relabel
partial evidence when any of these occurs:

- the captured dirty state changes, is incomplete, or includes an unauthorized
  file;
- metadata, normalized internal versions, dependency direction, lockfile,
  archive inventory, README, or either license is missing or unexpected;
- a license is a symlink or differs by bytes or hash from the canonical root;
- an archive contains `.git`, `.github`, `.env*`, local notes, generated
  release output, actual credentials, secrets, or other unintended material;
- a required verification, clean-package, or hosted-CI gate fails or cannot
  run;
- the candidate commit, hosted-CI commit, archive source, or tag target differs;
- authorization, crates.io ownership, authentication, or exact-name
  availability is absent or ambiguous;
- publication is rejected, a checksum differs, or a required exact `0.1.0`
  dependency is not indexed and resolvable before its dependent;
- the index wait times out or a fresh exact-version consumer fails; or
- continuing would require a dependency, public API, durable format,
  migration, credential-handling, or release-scope change.

Record the failure and obtain direction. A local package pass, dry run, search
result, tag, partial publication, or index query is never evidence that the
complete release succeeded.
