# SDK and language-boundary rules

These rules apply under `sdk/` in addition to the repository root rules.

- Rust is the native core and embedded language. Go and future languages are
  clients/frontends, not implementation dependencies of the core.
- SDK-specific types, reflection metadata, tags, and naming conventions MUST
  NOT become Canonical Schema IR, logical-plan, physical-plan, type-system, or
  storage semantics.
- Generated SDKs MUST preserve semantic IDs, physical types, nullability,
  parameter types, result shape, and cardinality.
- Independent processes SHOULD use a versioned language-neutral protocol. FFI
  is not the default cross-language mechanism.
- The SDK Schema Spec is language-neutral code-generation input, not Canonical
  Schema encoding or a deployment manifest.
- Go generation MUST use Rust `TableDef` fingerprints; Go code compares
  fingerprints and MUST NOT implement the canonical fingerprint algorithm.
- Generated files MUST NOT be manually edited. They MUST identify their source
  and exact regeneration command and remain byte-reproducible under the CI
  generated-source check.

If FFI is justified:

- keep the exported ABI small and explicitly C-compatible;
- never expose Rust references or internal Rust struct layout;
- define ownership, allocation, and freeing responsibilities;
- validate all foreign input;
- prevent Rust panics from unwinding across the boundary;
- isolate unsafe code behind safe native APIs.

Executable Go changes require `gofmt` and `go test ./...` from the owning Go
module. Documentation-only Go SDK changes do not require a nonexistent Go
module or fabricated client implementation.
