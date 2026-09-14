# Release Runbook

This is the authoritative manual procedure for the eight-crate `0.1.0`
family. It separates local preparation from a clean committed candidate and
from irreversible registry work. Passing an earlier phase does not authorize
or prove a later one.

Publication preparation was cancelled by the User on 2026-09-15. Plan 025 has
been deleted. This Runbook remains a reference, not an active task or standing
authorization. The local `v0.1.0` tag is retained; remote publication status was
not rechecked during cancellation. A future release requires a new approved
plan and fresh evidence.

Do not create a commit, push, tag, GitHub
Release, or crates.io publication unless the User / Product Owner separately
authorizes that exact operation. Never place a crates.io token, provider key,
MCP credential, or other secret in a command line, repository file, captured
diff, shell history, log, archive, or report. Authentication belongs in the
operator's preconfigured credential store. Its contents are never inspected or
captured; a preflight may validate only owner, mode, file-kind, parent-directory,
and credential-provider metadata. An authenticated publication command is
permitted only after the User / Product Owner grants the separate crates.io
Human Checkpoint.

## Historical T4.1 evidence boundary

The following two sections preserve the former pre-tag procedure and its
point-in-time observations. Their tag-absence expectations are historical:
the local tag now exists. Do not execute these as a current release checklist;
a future approved plan must account for existing tag identity.

Plan 024 records the corrected candidate
`0cb9b9c334320c6e39881d5b14b7ca2122021d81`, its accepted clean archive and
local-verification evidence, and GitHub Actions run `30943603111` with both
required jobs passing for that SHA. The current T4.1 host-side capture at
`2026-08-14T20:22:34.334748+00:00` records the Actions run, remote-tag absence,
GitHub Release absence, all eight sparse-index `404` results, and safe
credential-store metadata. The detailed selected-field record and its SHA-256
are in the [`v0.1.0 preflight record`](../release/v0.1.0-preflight.md) and
[`host preflight evidence`](../release/v0.1.0-host-preflight-evidence.json).
No historical or point-in-time observation is a standing reservation or
authorization: every Human Checkpoint repeats the applicable external checks
immediately before its irreversible operation. Phase 3 remains separately
authorized and unperformed.

## Historical T4.1 original-source capture

The supplied host-side capture completes the current-original-source portion
of T4.1. It is a credential-safe selected-field record, not a raw external
payload, and it must not be relabeled as evidence for a later checkpoint. For
each required recheck, the Orchestrator captures one short UTC observation
window in an owner-only, mode-`0700`, ignored evidence directory. Use direct
unauthenticated HTTPS reads with curl configuration, netrc, redirect following,
retries, and insecure TLS disabled. Retain only canonical URLs, UTC start/end
times, transport exits, HTTP classifications, selected non-secret result
fields, and a verified `sha256sum` manifest. Do not retain request headers,
cookies, authentication diagnostics, or credential content.

The required original sources and results are:

- `GET https://api.github.com/repos/VanGogh-7/Group/actions/runs/30943603111`
  and its `jobs?filter=latest&per_page=100` resource: HTTP `200`, the approved
  candidate SHA, completed/success run state, and both required successful jobs;
- `GET https://api.github.com/repos/VanGogh-7/Group/git/ref/tags/v0.1.0`: HTTP
  `404`, which proves no lightweight or annotated exact tag ref exists;
- `GET https://api.github.com/repos/VanGogh-7/Group/releases/tags/v0.1.0`: HTTP
  `404`;
- exact `refs/tags/v0.1.0` local absence, with `git show-ref` exit `1` and an
  empty exact-tag listing; and
- HTTP `404` from each exact sparse path under
  `https://index.crates.io/gr/ou/` for the eight fixed package names.

Any transport failure, unexpected response, candidate mismatch, non-empty
local-tag output, or unavailable credential metadata is a stop condition, not
absence evidence. Summarize a successful capture in the T4.1 preflight record
without copying raw external payloads. These checks are point-in-time only and
must be repeated at the relevant tag and publication checkpoints.

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
`full` job and workspace `msrv` job for the same candidate commit. Local YAML
parsing, local script passes, or a dirty preflight do not establish hosted CI.
Any difference between the tested commit, packaged source, proposed tag, and
hosted-CI commit invalidates the candidate and restarts this phase.

## Phase 3: separately authorized tag and crates.io release

This phase is irreversible external work and requires a new, explicit release
authorization after Phase 2 passes. Immediately recheck ownership and the
exact eight names. `cargo search` returning no exact match is only a hint: it
does not reserve a name or guarantee publication rights.

Prepare the authorized `v0.1.0` tag only on the exact clean candidate commit
and verify the tag target before any publication. For each eligible crate, run
the following dry run only after every earlier dependency layer is indexed and
resolvable from a fresh crates.io-only consumer:

```bash
cargo publish --registry crates-io --locked --dry-run -p <crate>
```

The explicit registry selection is mandatory. A dry run must never use a
default registry selected by inherited Cargo configuration. Do not run dry
runs or publication commands as one unattended script. After the dry run and
all current checks pass, publish exactly one crate in the fixed order:

```bash
cargo publish --registry crates-io --locked -p group-agent-core
cargo publish --registry crates-io --locked -p group-agent-model
cargo publish --registry crates-io --locked -p group-agent-tool
cargo publish --registry crates-io --locked -p group-agent-checkpoint-sqlite
cargo publish --registry crates-io --locked -p group-agent-observability-tokio
cargo publish --registry crates-io --locked -p group-agent-genai
cargo publish --registry crates-io --locked -p group-agent-mcp
cargo publish --registry crates-io --locked -p group-agent-prebuilt
```

After each successful publication, wait for crates.io/index visibility and
prove exact-version resolution from a fresh isolated consumer before proceeding
to the next crate. Each consumer gets a distinct, empty, audited Cargo home;
creating only a new project directory is insufficient because Cargo can inherit
source replacement, a non-crates.io default registry, or other configuration.

Use this procedure for the just-published crate:

```bash
registry_check_dir="$(mktemp -d /tmp/group-registry-check.XXXXXX)"
registry_cargo_home="$(mktemp -d /tmp/group-registry-cargo-home.XXXXXX)"

test -d "$registry_check_dir"
test ! -L "$registry_check_dir"
test -d "$registry_cargo_home"
test ! -L "$registry_cargo_home"
test "$(stat -c '%u' "$registry_cargo_home")" = "$(id -u)"
registry_cargo_home_mode="$(stat -c '%a' "$registry_cargo_home")"
test $((8#$registry_cargo_home_mode & 8#077)) -eq 0

for cargo_private_file in credentials credentials.toml config config.toml; do
  test ! -e "$registry_cargo_home/$cargo_private_file"
  test ! -L "$registry_cargo_home/$cargo_private_file"
done

assert_no_cargo_config() {
  local config_root="$1"
  while :; do
    for cargo_config in "$config_root/.cargo/config" \
      "$config_root/.cargo/config.toml"; do
      test ! -e "$cargo_config"
      test ! -L "$cargo_config"
    done
    if test "$config_root" = /; then
      break
    fi
    config_root="$(dirname "$config_root")"
  done
}

assert_no_cargo_config "$registry_check_dir"

registry_consumer_env=(
  env -i
  "PATH=$PATH"
  "HOME=$HOME"
  "RUSTUP_HOME=$HOME/.rustup"
  "CARGO_HOME=$registry_cargo_home"
  "CARGO_BUILD_JOBS=2"
)

(
  cd "$registry_check_dir"
  "${registry_consumer_env[@]}" cargo init --quiet --bin consumer
)
cd "$registry_check_dir/consumer"
assert_no_cargo_config "$PWD"
"${registry_consumer_env[@]}" cargo add --registry crates-io group-agent-core@=0.1.0
"${registry_consumer_env[@]}" cargo metadata --locked --format-version 1 \
  > metadata.json
"${registry_consumer_env[@]}" cargo check --locked
"${registry_consumer_env[@]}" cargo tree --locked
```

`env -i` is deliberate: do not pass inherited `CARGO_REGISTRY_*`,
`CARGO_REGISTRIES_*`, `CARGO_SOURCE_*`, `CARGO_CONFIG`, or other Cargo
configuration variables into the consumer. The only Cargo variables passed are
the newly created `CARGO_HOME` and the bounded build-job setting. The audit
rejects both legacy `config` and `config.toml` in that Cargo home and in every
consumer-directory ancestor, so a source replacement, path override, patch,
local registry, or unexpected default registry cannot be inherited.

Replace the crate name for each step and allocate a new project directory and
Cargo home each time. Retain `metadata.json`, `Cargo.lock`, `cargo tree`, and
the command outcomes. Confirm that every non-root resolved package has only the
canonical crates.io registry source and that no path, Git, patch, local
registry, source replacement, or unexpected registry-default configuration is
present. A registry-looking lockfile alone is corroboration, not proof of this
isolation; the configuration-origin audit and controlled environment are
required. An index lookup alone is likewise insufficient if the fresh consumer
cannot resolve and build the exact version.

After all eight crates are indexed, repeat the complete preceding procedure
with another new project directory and another new audited Cargo home. Add all
eight dependencies with exact `@=0.1.0` versions using `cargo add --registry
crates-io`, then run the same metadata, lockfile, tree, and check audits. Only
this final isolated check supports the claim that a fresh consumer can use the
complete published family. Tag push and GitHub Release creation remain
separately authorized external operations; perform them only in the order
approved by the User.

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
- an exact sparse-index read cannot be completed with its expected response, or
  the metadata-only credential-store audit cannot establish safe availability;
- a fresh consumer's isolated Cargo-home or configuration-origin audit finds a
  source replacement, patch, local registry, unexpected default registry, or
  inherited Cargo configuration;
- publication is rejected, a checksum differs, or a required exact `0.1.0`
  dependency is not indexed and resolvable before its dependent;
- the index wait times out or a fresh exact-version consumer fails; or
- continuing would require a dependency, public API, durable format,
  migration, credential-handling, or release-scope change.

Record the failure and obtain direction. A local package pass, dry run, search
result, tag, partial publication, or index query is never evidence that the
complete release succeeded.
