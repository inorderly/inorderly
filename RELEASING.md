# Releasing Inorderly

Releases go out on their own. Nobody clicks Publish. This file is the record of
how that works, for whoever (person or Claude session) works on this repo next.

## The short version

1. Raise `const Version = "X.Y.Z"` in `inorderly.go`.
2. Write what changed into the commit message.
3. Push the commit to `main`.

When the `test` workflow passes on that commit (the full suite on Linux, macOS
and Windows, the race detector, and the smoke check on each), the `release`
workflow builds the binaries, smoke-tests them, then tags the commit `vX.Y.Z`
and publishes the release "Inorderly vX.Y.Z" with the archives and SHA256SUMS
attached. If any build or check fails, nothing is tagged and nothing goes
public. Fix it, push again, and the next green run picks it up.

Pushing without raising the version is fine: the release workflow sees the tag
already exists and stops quietly.

## What it does, step by step

[.github/workflows/release.yml](.github/workflows/release.yml) starts when a
`test` run finishes; the push alone doesn't start it.

- **plan.** Goes on only if the `test` run passed, came from a push to `main` in
  this repo, and `main` still points at that commit (if `main` moved on, the run
  for the newer commit decides). Reads `Version` from `inorderly.go`. Stops
  quietly if `vX.Y.Z` is already a tag. Fails loudly if the version is lower than
  the newest tag. [scripts/release-plan.py](scripts/release-plan.py) has the
  rules, and [scripts/release_plan_test.py](scripts/release_plan_test.py) its cases.
- **build.** Inorderly is pure Go, so one Linux runner builds all five binaries
  from that exact commit with cgo off, checks the Linux one is static and the
  Intel macOS one is x86-64, and packs each with LICENSE, NOTICE,
  THIRD_PARTY_LICENSES.md and README.md.
- **check.** Runs [scripts/smoke.py](scripts/smoke.py) on Linux x86-64, Linux
  ARM64, macOS Apple silicon and Windows runners, each on its own binary.
- **release.** Runs only after every build and check passed. Writes SHA256SUMS
  and the notes, then creates the tag and the release in one step with
  `gh release create --target <commit>`, on the repo's own `GITHUB_TOKEN`
  (`contents: write` on this job only). A `concurrency` group per tag means two
  runs for the same version can't publish twice.

The smoke check, on each binary: it reports the expected version; four shuffled
arrival orders with duplicates, made by the Python producer, give the same
output as the sorted one, and the replay hash is the output's SHA-256; late,
conflicting and invalid lines land in the rejects file and `-strict` exits 3; a
run with `-state` is killed three times part way and finishes with the same
output as a clean run; and `inorderly demo` passes.

## Files that go out

| System | File |
|---|---|
| Linux, x86-64 (static) | inorderly_linux_amd64.tar.gz |
| Linux, ARM64 (static) | inorderly_linux_arm64.tar.gz |
| macOS, Apple silicon | inorderly_darwin_arm64.tar.gz |
| macOS, Intel | inorderly_darwin_amd64.tar.gz |
| Windows, x86-64 | inorderly_windows_amd64.zip |
| Checksums | SHA256SUMS |

Archive names carry no version on purpose: links like
`https://github.com/inorderly/inorderly/releases/latest/download/inorderly_linux_amd64.tar.gz`
keep working from one release to the next, so the README and the site never need
editing for a release.

## After a release

A Claude session can't create tags or releases itself, which is why the workflow
does it on the repository's token. After pushing a raised version, check:

1. The `test` run and then the `release` run finished green
   (`gh run list --repo inorderly/inorderly`).
2. The release exists with all six files
   (`gh release view vX.Y.Z --repo inorderly/inorderly`).
3. The `latest/download` links resolve, and a downloaded archive's SHA256 matches
   SHA256SUMS.
4. A downloaded Linux binary runs: `inorderly version` names the release.

## By hand

- A release published by hand gets the binaries attached by the same workflow.
- To rebuild the files of an existing tag, run the `release` workflow by hand
  with the tag (`gh workflow run release --repo inorderly/inorderly -f tag=vX.Y.Z`).
