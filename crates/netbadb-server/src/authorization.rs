use std::error::Error;
use std::fmt;

use netbadb_types::TableId;

use crate::{ClientIdentity, TransportKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthorizationAction {
    Read,
    Write,
    Transaction,
    Analyze,
}

impl fmt::Display for AuthorizationAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::Read => "read",
            Self::Write => "write",
            Self::Transaction => "transaction",
            Self::Analyze => "analyze",
        };
        formatter.write_str(name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TablePermissions {
    table_id: TableId,
    read: bool,
    write: bool,
    transaction: bool,
    analyze: bool,
}

impl TablePermissions {
    pub(crate) const fn new(
        table_id: TableId,
        read: bool,
        write: bool,
        transaction: bool,
        analyze: bool,
    ) -> Self {
        Self {
            table_id,
            read,
            write,
            transaction,
            analyze,
        }
    }

    const fn allows(self, action: AuthorizationAction) -> bool {
        match action {
            AuthorizationAction::Read => self.read,
            AuthorizationAction::Write => self.write,
            AuthorizationAction::Transaction => self.transaction,
            AuthorizationAction::Analyze => self.analyze,
        }
    }

    const fn has_any_permission(self) -> bool {
        self.read || self.write || self.transaction || self.analyze
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrincipalAuthorization {
    tables: Vec<TablePermissions>,
}

impl PrincipalAuthorization {
    fn new(
        tables: Vec<TablePermissions>,
        known_tables: &[TableId],
    ) -> Result<Self, AuthorizationConfigError> {
        if tables.is_empty() {
            return Err(AuthorizationConfigError::EmptyPrincipalPolicy);
        }
        for (index, permissions) in tables.iter().enumerate() {
            if !known_tables.contains(&permissions.table_id) {
                return Err(AuthorizationConfigError::UnknownTable {
                    table_id: permissions.table_id,
                });
            }
            if !permissions.has_any_permission() {
                return Err(AuthorizationConfigError::EmptyTablePermissions {
                    table_id: permissions.table_id,
                });
            }
            if tables[..index]
                .iter()
                .any(|other| other.table_id == permissions.table_id)
            {
                return Err(AuthorizationConfigError::DuplicateTableGrant {
                    table_id: permissions.table_id,
                });
            }
        }
        Ok(Self { tables })
    }

    pub(crate) fn authorize(
        &self,
        action: AuthorizationAction,
        table_id: TableId,
    ) -> Result<(), AuthorizationDenied> {
        if self
            .tables
            .iter()
            .any(|permissions| permissions.table_id == table_id && permissions.allows(action))
        {
            Ok(())
        } else {
            Err(AuthorizationDenied::Operation { action, table_id })
        }
    }

    pub(crate) fn can_see(&self, table_id: TableId) -> bool {
        self.tables
            .iter()
            .any(|permissions| permissions.table_id == table_id)
    }
}

#[derive(Clone, PartialEq, Eq)]
struct ClientAuthorization {
    certificate_sha256: [u8; 32],
    principal: PrincipalAuthorization,
}

#[derive(Clone, PartialEq, Eq)]
pub(crate) struct AuthorizationPolicy {
    local_plaintext: Option<PrincipalAuthorization>,
    clients: Vec<ClientAuthorization>,
}

impl AuthorizationPolicy {
    pub(crate) fn new(
        transport: TransportKind,
        local_plaintext: Option<Vec<TablePermissions>>,
        clients: Vec<([u8; 32], Vec<TablePermissions>)>,
        known_tables: &[TableId],
    ) -> Result<Self, AuthorizationConfigError> {
        match transport {
            TransportKind::PlaintextLoopback => {
                if local_plaintext.is_none() {
                    return Err(AuthorizationConfigError::PlaintextLocalPolicyRequired);
                }
                if !clients.is_empty() {
                    return Err(AuthorizationConfigError::PlaintextClientsNotAllowed);
                }
            }
            TransportKind::MutualTls => {
                if local_plaintext.is_some() {
                    return Err(AuthorizationConfigError::MutualTlsLocalPolicyNotAllowed);
                }
                if clients.is_empty() {
                    return Err(AuthorizationConfigError::MutualTlsClientsRequired);
                }
            }
        }

        let local_plaintext = local_plaintext
            .map(|tables| PrincipalAuthorization::new(tables, known_tables))
            .transpose()?;
        let mut validated_clients = Vec::with_capacity(clients.len());
        for (certificate_sha256, tables) in clients {
            if validated_clients
                .iter()
                .any(|client: &ClientAuthorization| client.certificate_sha256 == certificate_sha256)
            {
                return Err(AuthorizationConfigError::DuplicateClientFingerprint);
            }
            validated_clients.push(ClientAuthorization {
                certificate_sha256,
                principal: PrincipalAuthorization::new(tables, known_tables)?,
            });
        }
        Ok(Self {
            local_plaintext,
            clients: validated_clients,
        })
    }

    pub(crate) fn admit(
        &self,
        identity: &ClientIdentity,
    ) -> Result<PrincipalAuthorization, AuthorizationDenied> {
        self.principal_for(identity)
            .cloned()
            .ok_or(AuthorizationDenied::UnconfiguredPrincipal)
    }

    fn principal_for(&self, identity: &ClientIdentity) -> Option<&PrincipalAuthorization> {
        match identity {
            ClientIdentity::LocalPlaintext => self.local_plaintext.as_ref(),
            ClientIdentity::MutualTls(identity) => self
                .clients
                .iter()
                .find(|client| client.certificate_sha256 == *identity.certificate_sha256())
                .map(|client| &client.principal),
        }
    }
}

impl fmt::Debug for AuthorizationPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AuthorizationPolicy")
            .field("has_local_plaintext", &self.local_plaintext.is_some())
            .field("client_count", &self.clients.len())
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthorizationConfigError {
    InvalidFingerprintLength { length: usize },
    InvalidFingerprintHex { index: usize },
    DuplicateClientFingerprint,
    DuplicateTableGrant { table_id: TableId },
    UnknownTable { table_id: TableId },
    EmptyPrincipalPolicy,
    EmptyTablePermissions { table_id: TableId },
    PlaintextLocalPolicyRequired,
    PlaintextClientsNotAllowed,
    MutualTlsLocalPolicyNotAllowed,
    MutualTlsClientsRequired,
}

impl fmt::Display for AuthorizationConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFingerprintLength { length } => write!(
                formatter,
                "certificate SHA-256 fingerprint must contain exactly 64 hexadecimal characters, got {length}"
            ),
            Self::InvalidFingerprintHex { index } => write!(
                formatter,
                "certificate SHA-256 fingerprint contains a non-hexadecimal character at byte {index}"
            ),
            Self::DuplicateClientFingerprint => {
                formatter.write_str("authorization contains a duplicate client fingerprint")
            }
            Self::DuplicateTableGrant { table_id } => write!(
                formatter,
                "authorization principal configures table {} more than once",
                table_id.0
            ),
            Self::UnknownTable { table_id } => write!(
                formatter,
                "authorization references unknown table {}",
                table_id.0
            ),
            Self::EmptyPrincipalPolicy => {
                formatter.write_str("authorization principal must configure at least one table")
            }
            Self::EmptyTablePermissions { table_id } => write!(
                formatter,
                "authorization table {} grants no operations",
                table_id.0
            ),
            Self::PlaintextLocalPolicyRequired => formatter
                .write_str("plaintext loopback transport requires a local_plaintext policy"),
            Self::PlaintextClientsNotAllowed => formatter
                .write_str("plaintext loopback transport must not configure TLS client policies"),
            Self::MutualTlsLocalPolicyNotAllowed => {
                formatter.write_str("mutual TLS transport must not configure local_plaintext")
            }
            Self::MutualTlsClientsRequired => {
                formatter.write_str("mutual TLS transport requires at least one client policy")
            }
        }
    }
}

impl Error for AuthorizationConfigError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthorizationDenied {
    UnconfiguredPrincipal,
    Operation {
        action: AuthorizationAction,
        table_id: TableId,
    },
}

impl fmt::Display for AuthorizationDenied {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnconfiguredPrincipal => {
                formatter.write_str("authorization denied for unconfigured principal")
            }
            Self::Operation { action, table_id } => write!(
                formatter,
                "authorization denied for `{action}` on table {}",
                table_id.0
            ),
        }
    }
}

pub(crate) fn parse_certificate_sha256(
    encoded: &str,
) -> Result<[u8; 32], AuthorizationConfigError> {
    if encoded.len() != 64 {
        return Err(AuthorizationConfigError::InvalidFingerprintLength {
            length: encoded.len(),
        });
    }
    let mut fingerprint = [0_u8; 32];
    for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
        let high = decode_hex(pair[0])
            .ok_or(AuthorizationConfigError::InvalidFingerprintHex { index: index * 2 })?;
        let low = decode_hex(pair[1]).ok_or(AuthorizationConfigError::InvalidFingerprintHex {
            index: index * 2 + 1,
        })?;
        fingerprint[index] = (high << 4) | low;
    }
    Ok(fingerprint)
}

const fn decode_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn permissions(
        table_id: u64,
        read: bool,
        write: bool,
        transaction: bool,
        analyze: bool,
    ) -> TablePermissions {
        TablePermissions::new(TableId(table_id), read, write, transaction, analyze)
    }

    #[test]
    fn fingerprint_parser_is_exact_and_normalizes_hex_case() {
        let lower = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let upper = lower.to_uppercase();
        assert_eq!(
            parse_certificate_sha256(lower).unwrap(),
            parse_certificate_sha256(&upper).unwrap()
        );
        assert!(matches!(
            parse_certificate_sha256(&lower[..63]),
            Err(AuthorizationConfigError::InvalidFingerprintLength { .. })
        ));
        assert!(matches!(
            parse_certificate_sha256(&format!("{}:", &lower[..63])),
            Err(AuthorizationConfigError::InvalidFingerprintHex { .. })
        ));
    }

    #[test]
    fn policy_validates_transport_principals_tables_and_grants() {
        let known = [TableId(1), TableId(2)];
        assert!(matches!(
            AuthorizationPolicy::new(TransportKind::PlaintextLoopback, None, Vec::new(), &known),
            Err(AuthorizationConfigError::PlaintextLocalPolicyRequired)
        ));
        assert!(matches!(
            AuthorizationPolicy::new(
                TransportKind::MutualTls,
                Some(vec![permissions(1, true, false, false, false)]),
                Vec::new(),
                &known,
            ),
            Err(AuthorizationConfigError::MutualTlsLocalPolicyNotAllowed)
        ));
        assert!(matches!(
            AuthorizationPolicy::new(
                TransportKind::PlaintextLoopback,
                Some(vec![permissions(1, true, false, false, false)]),
                vec![([1; 32], vec![permissions(1, true, false, false, false)])],
                &known,
            ),
            Err(AuthorizationConfigError::PlaintextClientsNotAllowed)
        ));
        assert!(matches!(
            AuthorizationPolicy::new(TransportKind::MutualTls, None, Vec::new(), &known),
            Err(AuthorizationConfigError::MutualTlsClientsRequired)
        ));
        assert!(matches!(
            AuthorizationPolicy::new(
                TransportKind::PlaintextLoopback,
                Some(Vec::new()),
                Vec::new(),
                &known,
            ),
            Err(AuthorizationConfigError::EmptyPrincipalPolicy)
        ));
        assert!(matches!(
            AuthorizationPolicy::new(
                TransportKind::PlaintextLoopback,
                Some(vec![permissions(3, true, false, false, false)]),
                Vec::new(),
                &known,
            ),
            Err(AuthorizationConfigError::UnknownTable {
                table_id: TableId(3)
            })
        ));
        assert!(matches!(
            AuthorizationPolicy::new(
                TransportKind::PlaintextLoopback,
                Some(vec![
                    permissions(1, true, false, false, false),
                    permissions(1, false, true, false, false),
                ]),
                Vec::new(),
                &known,
            ),
            Err(AuthorizationConfigError::DuplicateTableGrant {
                table_id: TableId(1)
            })
        ));
        assert!(matches!(
            AuthorizationPolicy::new(
                TransportKind::MutualTls,
                None,
                vec![
                    ([1; 32], vec![permissions(1, true, false, false, false)]),
                    ([1; 32], vec![permissions(2, true, false, false, false)]),
                ],
                &known,
            ),
            Err(AuthorizationConfigError::DuplicateClientFingerprint)
        ));
    }

    #[test]
    fn local_policy_checks_every_operation_independently() {
        let policy = AuthorizationPolicy::new(
            TransportKind::PlaintextLoopback,
            Some(vec![
                permissions(1, true, false, true, false),
                permissions(2, false, true, false, true),
            ]),
            Vec::new(),
            &[TableId(1), TableId(2)],
        )
        .unwrap();
        let principal = policy.admit(&ClientIdentity::LocalPlaintext).unwrap();
        assert!(
            principal
                .authorize(AuthorizationAction::Read, TableId(1))
                .is_ok()
        );
        assert!(
            principal
                .authorize(AuthorizationAction::Transaction, TableId(1))
                .is_ok()
        );
        assert!(
            principal
                .authorize(AuthorizationAction::Write, TableId(1))
                .is_err()
        );
        assert!(
            principal
                .authorize(AuthorizationAction::Analyze, TableId(1))
                .is_err()
        );
        assert!(
            principal
                .authorize(AuthorizationAction::Write, TableId(2))
                .is_ok()
        );
        assert!(
            principal
                .authorize(AuthorizationAction::Analyze, TableId(2))
                .is_ok()
        );
        assert!(
            policy
                .admit(&ClientIdentity::MutualTls(
                    crate::AuthenticatedClientIdentity::from_certificate_sha256_for_test([9; 32])
                ))
                .is_err()
        );
    }

    #[test]
    fn mutual_tls_policy_looks_up_exact_fingerprint_and_rejects_unlisted_clients() {
        let policy = AuthorizationPolicy::new(
            TransportKind::MutualTls,
            None,
            vec![([7; 32], vec![permissions(1, true, false, false, false)])],
            &[TableId(1)],
        )
        .unwrap();
        let listed = ClientIdentity::MutualTls(
            crate::AuthenticatedClientIdentity::from_certificate_sha256_for_test([7; 32]),
        );
        let unlisted = ClientIdentity::MutualTls(
            crate::AuthenticatedClientIdentity::from_certificate_sha256_for_test([8; 32]),
        );

        assert!(policy.admit(&listed).is_ok());
        assert!(matches!(
            policy.admit(&unlisted),
            Err(AuthorizationDenied::UnconfiguredPrincipal)
        ));
    }
}
