# Core Physical Design admission rules

These rules apply to `netbadb-core` in addition to the repository and
`crates/` rules.

Index output admission consumes the fresh storage-authored Heap participant
writer bound. The bound excludes Coordinator and whole-mutation resources and
MUST NOT be described as total Index mutation cost.

Improved Index evidence uses the existing `OutputWriteBytes` dimension and
creates no new proposal, permit, cached report, mutation authority, persistent
field, or wire authority.
