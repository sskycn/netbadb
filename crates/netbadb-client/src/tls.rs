use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::{self, BufReader, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConnection, RootCertStore, StreamOwned};
use rustls_pemfile::Item;

/// Verified mutual-TLS material for a NetbaDB client connection.
#[derive(Clone)]
pub struct TlsConfig {
    server_name: String,
    root_ca: PathBuf,
    client_certificate: PathBuf,
    client_private_key: PathBuf,
    config: Arc<rustls::ClientConfig>,
}

impl TlsConfig {
    /// Loads a server trust root, client certificate chain, and exactly one
    /// client private key. Anonymous and verification-bypassing TLS modes are
    /// intentionally not exposed.
    pub fn from_pem_files(
        server_name: impl Into<String>,
        root_ca: impl AsRef<Path>,
        client_certificate: impl AsRef<Path>,
        client_private_key: impl AsRef<Path>,
    ) -> Result<Self, TlsConfigError> {
        let server_name = server_name.into();
        validate_server_name(&server_name)?;
        let root_ca = root_ca.as_ref().to_path_buf();
        let client_certificate = client_certificate.as_ref().to_path_buf();
        let client_private_key = client_private_key.as_ref().to_path_buf();

        let root_certificates = load_certificates("root_ca", &root_ca)?;
        let client_certificates = load_certificates("client_certificate", &client_certificate)?;
        let client_key = load_private_key(&client_private_key)?;

        let mut roots = RootCertStore::empty();
        for (certificate_index, certificate) in root_certificates.into_iter().enumerate() {
            roots
                .add(certificate)
                .map_err(|source| TlsConfigError::InvalidRootCertificate {
                    path: root_ca.clone(),
                    certificate_index,
                    source,
                })?;
        }

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(TlsConfigError::Configuration)?
            .with_root_certificates(roots)
            .with_client_auth_cert(client_certificates, client_key)
            .map_err(TlsConfigError::Configuration)?;

        Ok(Self {
            server_name,
            root_ca,
            client_certificate,
            client_private_key,
            config: Arc::new(config),
        })
    }

    pub(crate) fn establish(
        &self,
        mut stream: TcpStream,
    ) -> Result<ConnectionStream, TlsHandshakeError> {
        let server_name = ServerName::try_from(self.server_name.clone())
            .map_err(|_| TlsHandshakeError::InvalidServerName(self.server_name.clone()))?;
        let mut connection = ClientConnection::new(Arc::clone(&self.config), server_name)
            .map_err(TlsHandshakeError::Tls)?;
        while connection.is_handshaking() {
            connection
                .complete_io(&mut stream)
                .map_err(TlsHandshakeError::Io)?;
        }
        Ok(ConnectionStream::Tls(Box::new(StreamOwned::new(
            connection, stream,
        ))))
    }
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsConfig")
            .field("server_name", &self.server_name)
            .field("root_ca", &self.root_ca)
            .field("client_certificate", &self.client_certificate)
            .field("client_private_key", &self.client_private_key)
            .finish_non_exhaustive()
    }
}

/// Errors while loading or validating mutual-TLS client material.
#[derive(Debug)]
pub enum TlsConfigError {
    InvalidServerName(String),
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
    InvalidRootCertificate {
        path: PathBuf,
        certificate_index: usize,
        source: rustls::Error,
    },
    Configuration(rustls::Error),
}

impl fmt::Display for TlsConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidServerName(name) => {
                write!(formatter, "invalid TLS server name `{name}`")
            }
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
            Self::InvalidRootCertificate {
                path,
                certificate_index,
                source,
            } => write!(
                formatter,
                "TLS root certificate {certificate_index} in `{}` is invalid: {source}",
                path.display()
            ),
            Self::Configuration(error) => {
                write!(formatter, "failed to configure mutual TLS client: {error}")
            }
        }
    }
}

impl Error for TlsConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Read { source, .. } | Self::Pem { source, .. } => Some(source),
            Self::InvalidRootCertificate { source, .. } | Self::Configuration(source) => {
                Some(source)
            }
            Self::InvalidServerName(_)
            | Self::MissingCertificate { .. }
            | Self::MissingPrivateKey { .. }
            | Self::MultiplePrivateKeys { .. } => None,
        }
    }
}

/// Errors while completing the TLS transport handshake.
#[derive(Debug)]
pub enum TlsHandshakeError {
    InvalidServerName(String),
    Io(io::Error),
    Tls(rustls::Error),
}

impl fmt::Display for TlsHandshakeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidServerName(name) => write!(formatter, "invalid TLS server name `{name}`"),
            Self::Io(error) => write!(formatter, "mutual TLS handshake I/O failed: {error}"),
            Self::Tls(error) => write!(formatter, "mutual TLS handshake failed: {error}"),
        }
    }
}

impl Error for TlsHandshakeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Tls(error) => Some(error),
            Self::InvalidServerName(_) => None,
        }
    }
}

pub(crate) enum ConnectionStream {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ClientConnection, TcpStream>>),
}

impl ConnectionStream {
    pub(crate) fn close(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.shutdown(Shutdown::Both),
            Self::Tls(stream) => {
                stream.conn.send_close_notify();
                let flush_result = stream.flush();
                let shutdown_result = stream.get_ref().shutdown(Shutdown::Both);
                flush_result.and(shutdown_result)
            }
        }
    }
}

impl Read for ConnectionStream {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buffer),
            Self::Tls(stream) => stream.read(buffer),
        }
    }
}

impl Write for ConnectionStream {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buffer),
            Self::Tls(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

fn validate_server_name(server_name: &str) -> Result<(), TlsConfigError> {
    ServerName::try_from(server_name.to_owned())
        .map(|_| ())
        .map_err(|_| TlsConfigError::InvalidServerName(server_name.to_owned()))
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
    let field = "client_private_key";
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
