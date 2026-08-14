use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ServerConnection, VerifierBuilderError, WebPkiClientVerifier};
use rustls::{RootCertStore, StreamOwned};
use rustls_pemfile::Item;
use sha2::{Digest, Sha256};

/// The transport mode selected by a deployment manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    /// Unencrypted transport restricted to an IP loopback listener.
    PlaintextLoopback,
    /// TLS with a required client certificate verified by the configured CA.
    MutualTls,
}

impl TransportKind {
    /// Returns the stable diagnostic label for this transport mode.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PlaintextLoopback => "plaintext-loopback",
            Self::MutualTls => "mutual-tls",
        }
    }
}

impl fmt::Display for TransportKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Stable server-side identity derived from a verified client leaf certificate.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct AuthenticatedClientIdentity {
    certificate_sha256: [u8; 32],
}

impl AuthenticatedClientIdentity {
    fn from_verified_leaf(certificate: &CertificateDer<'_>) -> Self {
        Self {
            certificate_sha256: Sha256::digest(certificate.as_ref()).into(),
        }
    }

    /// Returns the SHA-256 fingerprint of the verified client leaf certificate.
    #[must_use]
    pub const fn certificate_sha256(&self) -> &[u8; 32] {
        &self.certificate_sha256
    }
}

impl fmt::Debug for AuthenticatedClientIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthenticatedClientIdentity { certificate_sha256_prefix: ")?;
        for byte in &self.certificate_sha256[..8] {
            write!(formatter, "{byte:02x}")?;
        }
        formatter.write_str(" }")
    }
}

/// Identity associated with one worker session for its complete lifetime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientIdentity {
    /// A local development connection accepted over loopback plaintext.
    LocalPlaintext,
    /// A client authenticated by the configured mutual-TLS client CA.
    MutualTls(AuthenticatedClientIdentity),
}

impl ClientIdentity {
    #[must_use]
    pub(crate) const fn is_authenticated(&self) -> bool {
        matches!(self, Self::MutualTls(_))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TlsMaterialPaths {
    pub(crate) server_certificate: PathBuf,
    pub(crate) server_private_key: PathBuf,
    pub(crate) client_ca: PathBuf,
}

#[derive(Clone)]
pub(crate) struct MutualTlsConfig {
    paths: TlsMaterialPaths,
    server: Arc<rustls::ServerConfig>,
}

impl MutualTlsConfig {
    pub(crate) fn load(paths: TlsMaterialPaths) -> Result<Self, TlsConfigError> {
        let server_certificates =
            load_certificates("server_certificate", &paths.server_certificate)?;
        let server_private_key = load_private_key(&paths.server_private_key)?;
        let client_roots = load_client_roots(&paths.client_ca)?;

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = WebPkiClientVerifier::builder_with_provider(
            Arc::new(client_roots),
            Arc::clone(&provider),
        )
        .build()
        .map_err(TlsConfigError::ClientVerifier)?;
        let server = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(TlsConfigError::ServerConfiguration)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(server_certificates, server_private_key)
            .map_err(TlsConfigError::ServerConfiguration)?;

        Ok(Self {
            paths,
            server: Arc::new(server),
        })
    }

    pub(crate) fn into_transport(self) -> TransportSecurity {
        TransportSecurity::MutualTls(self.server)
    }
}

impl fmt::Debug for MutualTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MutualTlsConfig")
            .field("server_certificate", &self.paths.server_certificate)
            .field("server_private_key", &self.paths.server_private_key)
            .field("client_ca", &self.paths.client_ca)
            .finish_non_exhaustive()
    }
}

impl PartialEq for MutualTlsConfig {
    fn eq(&self, other: &Self) -> bool {
        self.paths == other.paths
    }
}

impl Eq for MutualTlsConfig {}

#[derive(Clone)]
pub(crate) enum TransportSecurity {
    PlaintextLoopback,
    MutualTls(Arc<rustls::ServerConfig>),
}

impl TransportSecurity {
    pub(crate) const fn kind(&self) -> TransportKind {
        match self {
            Self::PlaintextLoopback => TransportKind::PlaintextLoopback,
            Self::MutualTls(_) => TransportKind::MutualTls,
        }
    }

    pub(crate) fn establish(
        &self,
        stream: TcpStream,
    ) -> Result<(ConnectionStream, ClientIdentity), TlsHandshakeError> {
        match self {
            Self::PlaintextLoopback => Ok((
                ConnectionStream::Plaintext(stream),
                ClientIdentity::LocalPlaintext,
            )),
            Self::MutualTls(config) => establish_mutual_tls(stream, Arc::clone(config)),
        }
    }
}

impl fmt::Debug for TransportSecurity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind().fmt(formatter)
    }
}

pub(crate) enum ConnectionStream {
    Plaintext(TcpStream),
    MutualTls(Box<StreamOwned<ServerConnection, TcpStream>>),
}

impl ConnectionStream {
    pub(crate) fn close(&mut self) {
        match self {
            Self::Plaintext(stream) => {
                let _ = stream.shutdown(Shutdown::Both);
            }
            Self::MutualTls(stream) => {
                stream.conn.send_close_notify();
                let _ = stream.flush();
                let _ = stream.get_ref().shutdown(Shutdown::Both);
            }
        }
    }
}

impl Read for ConnectionStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plaintext(stream) => stream.read(buffer),
            Self::MutualTls(stream) => stream.read(buffer),
        }
    }
}

impl Write for ConnectionStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plaintext(stream) => stream.write(buffer),
            Self::MutualTls(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plaintext(stream) => stream.flush(),
            Self::MutualTls(stream) => stream.flush(),
        }
    }
}

fn establish_mutual_tls(
    mut stream: TcpStream,
    config: Arc<rustls::ServerConfig>,
) -> Result<(ConnectionStream, ClientIdentity), TlsHandshakeError> {
    let mut connection = ServerConnection::new(config).map_err(TlsHandshakeError::Tls)?;
    while connection.is_handshaking() {
        connection
            .complete_io(&mut stream)
            .map_err(TlsHandshakeError::Io)?;
    }
    let leaf = connection
        .peer_certificates()
        .and_then(|certificates| certificates.first())
        .ok_or(TlsHandshakeError::MissingVerifiedClientCertificate)?;
    let identity = AuthenticatedClientIdentity::from_verified_leaf(leaf);
    Ok((
        ConnectionStream::MutualTls(Box::new(StreamOwned::new(connection, stream))),
        ClientIdentity::MutualTls(identity),
    ))
}

#[derive(Debug)]
pub(crate) enum TlsHandshakeError {
    Io(io::Error),
    Tls(rustls::Error),
    MissingVerifiedClientCertificate,
}

impl TlsHandshakeError {
    pub(crate) fn is_timeout(&self) -> bool {
        matches!(self, Self::Io(error) if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut))
    }
}

impl fmt::Display for TlsHandshakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "mutual TLS handshake I/O failed: {error}"),
            Self::Tls(error) => write!(formatter, "mutual TLS handshake failed: {error}"),
            Self::MissingVerifiedClientCertificate => {
                formatter.write_str("mutual TLS completed without a verified client certificate")
            }
        }
    }
}

impl Error for TlsHandshakeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Tls(error) => Some(error),
            Self::MissingVerifiedClientCertificate => None,
        }
    }
}

#[derive(Debug)]
pub enum TlsConfigError {
    Read {
        field: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    Pem {
        field: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    MissingCertificate {
        field: &'static str,
        path: PathBuf,
    },
    MissingPrivateKey {
        path: PathBuf,
    },
    MultiplePrivateKeys {
        path: PathBuf,
        count: usize,
    },
    InvalidClientCa {
        path: PathBuf,
        certificate_index: usize,
        source: rustls::Error,
    },
    ClientVerifier(VerifierBuilderError),
    ServerConfiguration(rustls::Error),
}

impl fmt::Display for TlsConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read {
                field,
                path,
                source,
            } => write!(
                formatter,
                "failed to read TLS field `{field}` file `{}`: {source}",
                path.display()
            ),
            Self::Pem {
                field,
                path,
                source,
            } => write!(
                formatter,
                "failed to parse TLS field `{field}` PEM file `{}`: {source}",
                path.display()
            ),
            Self::MissingCertificate { field, path } => write!(
                formatter,
                "TLS field `{field}` file `{}` contains no certificates",
                path.display()
            ),
            Self::MissingPrivateKey { path } => write!(
                formatter,
                "TLS private-key file `{}` contains no supported private key",
                path.display()
            ),
            Self::MultiplePrivateKeys { path, count } => write!(
                formatter,
                "TLS private-key file `{}` contains {count} private keys; exactly one is required",
                path.display()
            ),
            Self::InvalidClientCa {
                path,
                certificate_index,
                source,
            } => write!(
                formatter,
                "TLS client-CA certificate {certificate_index} in `{}` is invalid: {source}",
                path.display()
            ),
            Self::ClientVerifier(error) => {
                write!(
                    formatter,
                    "failed to configure mandatory TLS client verification: {error}"
                )
            }
            Self::ServerConfiguration(error) => {
                write!(
                    formatter,
                    "failed to configure TLS server certificate and key: {error}"
                )
            }
        }
    }
}

impl Error for TlsConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { source, .. } | Self::Pem { source, .. } => Some(source),
            Self::InvalidClientCa { source, .. } | Self::ServerConfiguration(source) => {
                Some(source)
            }
            Self::ClientVerifier(error) => Some(error),
            Self::MissingCertificate { .. }
            | Self::MissingPrivateKey { .. }
            | Self::MultiplePrivateKeys { .. } => None,
        }
    }
}

fn load_certificates(
    field: &'static str,
    path: &Path,
) -> Result<Vec<CertificateDer<'static>>, TlsConfigError> {
    let file = File::open(path).map_err(|source| TlsConfigError::Read {
        field,
        path: path.to_path_buf(),
        source,
    })?;
    let certificates = rustls_pemfile::certs(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsConfigError::Pem {
            field,
            path: path.to_path_buf(),
            source,
        })?;
    if certificates.is_empty() {
        return Err(TlsConfigError::MissingCertificate {
            field,
            path: path.to_path_buf(),
        });
    }
    Ok(certificates)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsConfigError> {
    let field = "server_private_key";
    let file = File::open(path).map_err(|source| TlsConfigError::Read {
        field,
        path: path.to_path_buf(),
        source,
    })?;
    let items = rustls_pemfile::read_all(&mut BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| TlsConfigError::Pem {
            field,
            path: path.to_path_buf(),
            source,
        })?;
    let mut keys = items.into_iter().filter_map(|item| match item {
        Item::Pkcs1Key(key) => Some(PrivateKeyDer::from(key)),
        Item::Pkcs8Key(key) => Some(PrivateKeyDer::from(key)),
        Item::Sec1Key(key) => Some(PrivateKeyDer::from(key)),
        _ => None,
    });
    let Some(key) = keys.next() else {
        return Err(TlsConfigError::MissingPrivateKey {
            path: path.to_path_buf(),
        });
    };
    let additional = keys.count();
    if additional != 0 {
        return Err(TlsConfigError::MultiplePrivateKeys {
            path: path.to_path_buf(),
            count: additional + 1,
        });
    }
    Ok(key)
}

fn load_client_roots(path: &Path) -> Result<RootCertStore, TlsConfigError> {
    let certificates = load_certificates("client_ca", path)?;
    let mut roots = RootCertStore::empty();
    for (certificate_index, certificate) in certificates.into_iter().enumerate() {
        roots
            .add(certificate)
            .map_err(|source| TlsConfigError::InvalidClientCa {
                path: path.to_path_buf(),
                certificate_index,
                source,
            })?;
    }
    Ok(roots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authenticated_identity_hashes_verified_leaf_der_and_limits_debug_output() {
        let certificate = CertificateDer::from(vec![1, 2, 3]);
        let identity = AuthenticatedClientIdentity::from_verified_leaf(&certificate);
        assert_eq!(
            identity.certificate_sha256(),
            &[
                0x03, 0x90, 0x58, 0xc6, 0xf2, 0xc0, 0xcb, 0x49, 0x2c, 0x53, 0x3b, 0x0a, 0x4d, 0x14,
                0xef, 0x77, 0xcc, 0x0f, 0x78, 0xab, 0xcc, 0xce, 0xd5, 0x28, 0x7d, 0x84, 0xa1, 0xa2,
                0x01, 0x1c, 0xfb, 0x81,
            ]
        );
        assert_eq!(
            format!("{identity:?}"),
            "AuthenticatedClientIdentity { certificate_sha256_prefix: 039058c6f2c0cb49 }"
        );
    }
}
