# Physical Design Mutation Receipts — NBMR v3

NBMR v3 is the current bounded, Server-owned receipt journal for explicit
Physical Design mutations. Current binaries read v1 and v2, migrate complete
valid legacy histories, and write only v3. NBMR records operational evidence
about Database truth; it is not a transaction participant, mutation authority,
tamper-resistant audit log, or defense against a privileged administrator.

## Identity and file header

The stable receipt identity remains `(journal incarnation, receipt ID)`. The
44-byte little-endian header retains the v2 layout: `NBMR`, version `3`, two
zero reserved bytes, the 16-byte durable database incarnation, the 16-byte
nonzero journal incarnation, and CRC32C over bytes 0 through 39. A v2-to-v3
migration preserves the journal incarnation; v1 has no such namespace and gets
one nonzero OS-random incarnation only in the successfully published v3
history. A byte-for-byte copy therefore keeps its namespace, while a truly new
journal gets a new namespace.

## Protected record framing

Every record is at most 64 KiB. Its fixed 24-byte header is validated before
the body length is trusted or used for allocation:

| Offset | Bytes | Meaning |
| ---: | ---: | --- |
| 0 | 4 | body length |
| 4 | 4 | bitwise complement of body length |
| 8 | 1 | record tag: `1` Begin, `2` Outcome |
| 9 | 3 | zero reserved bytes |
| 12 | 8 | nonzero receipt ID |
| 20 | 4 | CRC32C over bytes 0 through 19 |

The bounded body follows, then a four-byte CRC32C over the complete fixed
header and body. Begin/Outcome body tags and semantics are unchanged from v1
and v2. Receipt IDs are nonzero and Begin IDs are strictly increasing. Only one
Begin may be unresolved. An Outcome must reference it and must match its target
domain: Index outcomes cannot finish a Columnar Begin, and Columnar outcomes
cannot finish an Index Begin. `AlreadyCovered`, `Rejected`, `Failed`,
`RecoveredNotApplied`, and `RecoveredConflict` are domain-neutral. Tag 7
`Failed` remains decodable for historical compatibility; the current runtime
does not emit it because Core does not expose a sufficiently narrow
definitely-not-applied execution-failure class.

At EOF, an incomplete fixed header or a validated fixed header followed by an
incomplete body/trailer is a provable torn v3 tail and is truncated to the last
verified record boundary. An invalid complete header, length complement,
reserved field, tag, header checksum, body checksum, order, or domain pairing
fails closed. Corruption in a middle record is never skipped. CRC32C protects
against accidental corruption and crash damage, not malicious rewriting.

## Legacy migration and capacity

The v1/v2 length prefix is covered only by the final record checksum. A legacy
claim larger than the remaining tail is therefore ambiguous: it might be a
torn write or length corruption. Current binaries fail closed and preserve the
source bytes; they do not scan for a guessed CRC boundary or silently omit a
possibly durable Begin. Only fully valid legacy histories migrate.

Migration decodes the legacy records and re-encodes the same IDs, targets, and
outcomes in v3 framing. Before publication it admits the complete v3 image and,
when the final Begin is unresolved, reserves `MAX_RECOVERED_OUTCOME_BYTES` for
startup reconciliation. Insufficient space returns typed
`MigrationCapacityExceeded`; the legacy source remains byte-for-byte unchanged
and no new version is published.

Fresh creation and migration use an unpredictable same-directory temporary
name opened with `create_new`, write and sync a complete image, publish
atomically, and sync the parent directory. Fresh publication fails if the final
path appeared. Migration never truncates, removes, chmods, or interprets the
historical fixed `<journal>.next`; regular files, other journals, hard links,
symlinks, dangling symlinks, and directories there remain untouched.

On Unix, existing final journals are opened with final-component no-follow
semantics. This closes the check-to-open symlink swap for the journal itself;
it does not claim to secure every parent component. The configured parent is
the existing canonical Server-owned namespace from configuration. Platforms
without an equivalent no-follow guarantee fail conservatively instead of
following a symlink while claiming Unix-equivalent safety.

## Active journal ownership

On Unix, one cooperative active runtime owns one journal inode. After the
final component is opened with `O_NOFOLLOW` and verified as a regular file,
the Server takes a nonblocking `LOCK_EX` advisory lock before reading the
header or history. `EWOULDBLOCK`/`EAGAIN` is the typed `AlreadyInUse` startup
failure. The exact locked handle remains the writer until the journal runtime
is dropped; there is no decode/reopen ownership gap.

A fresh v3 temporary is locked before its no-replace hard-link publication,
and the published inode retains that same handle and lock. Legacy migration
locks the opened v1/v2 source before decoding, locks the new v3 temporary
before writing or publication, verifies immediately before rename that the
final path still has the source file's device and inode, and retains the new
locked handle before releasing the legacy handle. A replaced final path fails
closed and is not overwritten. Startup also revalidates the locked handle against
the final component before recovery truncation and readiness. Runtime Begin and
Outcome appends check that identity before writing and after sync; detected
replacement fails the append and gates subsequent mutations until reopen.
These checks do not make pathname replacement atomic with Core mutation.

These are cooperative advisory locks in a trusted Server-owned parent
namespace. They do not defend against a privileged administrator, a process
that ignores advisory locks, a continually racing namespace attacker, or
filesystem rollback. Network filesystem implementations may also provide
different `flock` semantics and must be qualified by the deployer.

## Mutation and operator semantics

A synced Begin still precedes Core mutation. If Begin durability fails, the
mutation does not start; a complete unsynced Begin that remains visible is
reconciled as not applied after reopen. An already recovery-gated journal also
rejects a new request before Core. If a durable Begin exists and Core returns
an error that cannot prove the mutation did not occur, or if Outcome durability
fails, the result is typed mutation uncertainty with the known receipt and the
journal gates later mutations. Server does not compensate. Status and
scoped/unscoped receipt reads stay pure and available during that gate. They expose no journal path,
private Columnar recovery path, database incarnation, runtime token, SQL,
principal, session, address, or timestamp.

Manifest v10 may now provision this same NBMR v3 journal and independently
authorize local NBOP v5 reads. NBOP v5 exposes only journal incarnation plus
nonzero receipt ID, and adds an explicit receipt-bearing Outcome-durability
uncertainty error. Whole-response loss remains locally uncertain without an
invented receipt and retains exact idempotent-retry guidance. Outcome failure
instead instructs restart/reopen, reconciliation, then inspection of the known
receipt. Neither path retries, refreshes a token/epoch, changes placement/name,
or enables Change Stream automatically.

Tests inject failures at explicit journal I/O stages: before write, after a
partial write, and after a full write but before the sync call can succeed.
Those unit seams validate conservative runtime and reopen behavior; they are
not evidence of real hardware power-loss durability.
