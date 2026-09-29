# Maintained patch history

This orphan branch records the maintained downstream commit train in two forms:
the normal source commits remain on the source branch, while this branch retains
an ordered, applyable patch snapshot for every recorded revision of that train.
Updating this branch after an upstream rebase preserves the previous patch
forms in ordinary Git history instead of replacing them.

The generated snapshot contains:

- `UPSTREAM`: the exact upstream commit on which the series applies;
- `SOURCE`: the last source commit that contributes to the exported series;
- `SERIES`: patch filenames paired with their source commit IDs;
- `RESULT_TREE`: the exact tree produced by applying the series; and
- `patches/`: stable `git format-patch` files with commit messages and authors.

The canonical updater at `$HOME/.local/lib/patch-history/update.sh` is the
supported way to refresh generated files. Run it from this history worktree;
it derives the exact upstream base from the reviewed upstream tracking ref, regenerates the
complete series, applies every patch to a temporary index, compares the result
with the source tree, and creates the history commit. Use `--history-root` when
invoking it from another directory. For example:

```sh
"$HOME/.local/lib/patch-history/update.sh" --message "integrate upstream executor fixes" ../convex-rs
"$HOME/.local/lib/patch-history/update.sh" --push origin --message "integrate upstream executor fixes" ../convex-rs
```

The required `--message` is a concise human summary of why this snapshot is
being recorded; it becomes the subject of the history commit. The generated
source and upstream identifiers remain in the commit body and files. The first
command records locally. The second also publishes the resulting fast-forward
update to the remote `patch-history` branch. Use `--no-commit` only to inspect
generated changes before recording them; it does not accept `--message`.

Run the updater before rebasing or otherwise rewriting the source train, while
the version being replaced is still checked out, and again after the rewrite.
This makes both patch forms durable history. Also run it after adding or
amending an ordinary downstream patch commit.

To reconstruct the source commits on a clean checkout at `UPSTREAM`:

```sh
./scripts/apply.sh /path/to/upstream-checkout
```

The apply command uses `git am`, retaining commit authors and messages, and
then requires the resulting tree to equal `RESULT_TREE`.

Immutable archives are owned by the distribution repository. The maintained source
train and this generated series contain no package payloads; historical snapshots
retain their original export rules and result trees.

The generated patches deliberately use zeroed mail-header commit IDs. Source
commit IDs remain in `SERIES`, while unchanged patch content stays stable across
a conflict-free rebase. A rebase still records the new upstream and source IDs,
and any actual patch change remains visible in this branch's diff.
