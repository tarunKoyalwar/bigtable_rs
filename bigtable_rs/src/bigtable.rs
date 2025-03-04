//! `bigtable` module provides a few convenient structs for calling Google Bigtable from Rust code.
//!
//!
//! Example usage:
//! ```rust,no_run
//! use bigtable_rs::bigtable;
//! use bigtable_rs::google::bigtable::v2::row_filter::{Chain, Filter};
//! use bigtable_rs::google::bigtable::v2::row_range::{EndKey, StartKey};
//! use bigtable_rs::google::bigtable::v2::{ReadRowsRequest, RowFilter, RowRange, RowSet};
//! use env_logger;
//! use std::error::Error;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     env_logger::init();
//!
//!     let project_id = "project-1";
//!     let instance_name = "instance-1";
//!     let table_name = "table-1";
//!     let channel_size = 4;
//!     let timeout = Duration::from_secs(10);
//!
//!     let key_start: String = "key1".to_owned();
//!     let key_end: String = "key4".to_owned();
//!
//!     // make a bigtable client
//!     let connection = bigtable::BigTableConnection::new(
//!         project_id,
//!         instance_name,
//!         true,
//!         channel_size,
//!         Some(timeout),
//!     )
//!         .await?;
//!     let mut bigtable = connection.client();
//!
//!     // prepare a ReadRowsRequest
//!     let request = ReadRowsRequest {
//!         app_profile_id: "default".to_owned(),
//!         table_name: bigtable.get_full_table_name(table_name),
//!         rows_limit: 10,
//!         rows: Some(RowSet {
//!             row_keys: vec![], // use this field to put keys for reading specific rows
//!             row_ranges: vec![RowRange {
//!                 start_key: Some(StartKey::StartKeyClosed(key_start.into_bytes())),
//!                 end_key: Some(EndKey::EndKeyOpen(key_end.into_bytes())),
//!             }],
//!         }),
//!         filter: Some(RowFilter {
//!             filter: Some(Filter::Chain(Chain {
//!                 filters: vec![
//!                     RowFilter {
//!                         filter: Some(Filter::FamilyNameRegexFilter("cf1".to_owned())),
//!                     },
//!                     RowFilter {
//!                         filter: Some(Filter::ColumnQualifierRegexFilter("c1".as_bytes().to_vec())),
//!                     },
//!                     RowFilter {
//!                         filter: Some(Filter::CellsPerColumnLimitFilter(1)),
//!                     },
//!                 ],
//!             })),
//!         }),
//!         ..ReadRowsRequest::default()
//!     };
//!
//!     // calling bigtable API to get results
//!     let response = bigtable.read_rows(request).await?;
//!
//!     // simply print results for example usage
//!     response.into_iter().for_each(|(key, data)| {
//!         println!("------------\n{}", String::from_utf8(key.clone()).unwrap());
//!         data.into_iter().for_each(|row_cell| {
//!             println!(
//!                 "    [{}:{}] \"{}\" @ {}",
//!                 row_cell.family_name,
//!                 String::from_utf8(row_cell.qualifier).unwrap(),
//!                 String::from_utf8(row_cell.value).unwrap(),
//!                 row_cell.timestamp_micros
//!             )
//!         })
//!     });
//!
//!     Ok(())
//! }
//! ```

use debug_ignore::DebugIgnore;
use futures_util::Stream;
use gcp_auth::TokenProvider;
use log::info;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::net::UnixStream;
use tonic::transport::Endpoint;
use tonic::{codec::Streaming, transport::Channel, transport::ClientTlsConfig, Response};
use tower::ServiceBuilder;

use crate::auth_service::AuthSvc;
use crate::bigtable::read_rows::{decode_read_rows_response, decode_read_rows_response_stream};
use crate::google::bigtable::v2::{
    bigtable_client::BigtableClient, MutateRowRequest, MutateRowResponse, MutateRowsRequest,
    MutateRowsResponse, ReadRowsRequest, RowSet, SampleRowKeysRequest, SampleRowKeysResponse,
};
use crate::google::bigtable::v2::{CheckAndMutateRowRequest, CheckAndMutateRowResponse};
use crate::{root_ca_certificate, util::get_row_range_from_prefix};

pub mod read_rows;


/// The type of client to be used
/// 
/// - ReadOnly: for read-only access
/// - ReadWrite: for read-write access (default)
/// - Admin: for admin access
#[derive(Debug, Clone)]
pub enum ClientType {
    ReadOnly,
    ReadWrite,
    Admin,
}

impl Default for ClientType {
    fn default() -> Self {
        Self::ReadWrite
    }
}

/// An alias for Vec<u8> as row key
type RowKey = Vec<u8>;
/// A convenient Result type
type Result<T> = std::result::Result<T, Error>;

/// A data structure for returning the read content of a cell in a row.
///
/// Each cell in a Bigtable row contains:
/// - A family name (column family)
/// - A qualifier (column name)
/// - The actual value
/// - A timestamp in microseconds
/// - Optional labels
///
/// # Example
/// ```ignore
/// let cell = RowCell {
///     family_name: "cf1".to_string(),
///     qualifier: "col1".as_bytes().to_vec(),
///     value: "value1".as_bytes().to_vec(),
///     timestamp_micros: 1234567890,
///     labels: vec!["label1".to_string()],
/// };
/// ```
#[derive(Debug)]
pub struct RowCell {
    pub family_name: String,
    pub qualifier: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp_micros: i64,
    pub labels: Vec<String>,
}

/// Configuration for establishing a BigTable connection.
#[derive(Debug, Clone)]
pub struct Config {
    pub concurrency_limit: usize,
    pub timeout: Option<Duration>,
    pub max_grpc_connections: usize,
    pub token_provider: Option<DebugIgnore<Arc<dyn TokenProvider>>>,
    pub endpoint_cb: Option<fn(Endpoint) -> Endpoint>,
    pub tonic_cb: Option<
        fn(
            ServiceBuilder<tower::layer::util::Identity>,
        ) -> ServiceBuilder<tower::layer::util::Identity>,
    >,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            concurrency_limit: 100,
            timeout: None,
            max_grpc_connections: 1,
            token_provider: None,
            endpoint_cb: None,
            tonic_cb: None,
        }
    }
}

/// Error types that may occur during Bigtable operations.
///
/// This enum covers various error scenarios including:
/// - Authentication and access token errors
/// - Transport and RPC errors
/// - Row operation failures
/// - Timeout errors
///
/// # Example
/// ```ignore
/// match bigtable.read_rows(request).await {
///     Ok(rows) => {
///         // Process rows
///     }
///     Err(Error::TimeoutError(duration)) => {
///         println!("Operation timed out after {} seconds", duration);
///     }
///     Err(Error::RowNotFound) => {
///         println!("The requested row was not found");
///     }
///     Err(e) => {
///         println!("An error occurred: {}", e);
///     }
/// }
/// ```
#[derive(Debug, Error)]
pub enum Error {
    #[error("AccessToken error: {0}")]
    AccessTokenError(String),

    #[error("Certificate error: {0}")]
    CertificateError(String),

    #[error("I/O Error: {0}")]
    IoError(std::io::Error),

    #[error("Transport error: {0}")]
    TransportError(tonic::transport::Error),

    #[error("Chunk error")]
    ChunkError(String),

    #[error("Row not found")]
    RowNotFound,

    #[error("Row write failed")]
    RowWriteFailed,

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Object is corrupt: {0}")]
    ObjectCorrupt(String),

    #[error("RPC error: {0}")]
    RpcError(tonic::Status),

    #[error("Timeout error after {0} seconds")]
    TimeoutError(u64),

    #[error("GCPAuthError error: {0}")]
    GCPAuthError(#[from] gcp_auth::Error),
}

impl std::convert::From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl std::convert::From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        Self::TransportError(err)
    }
}

impl std::convert::From<tonic::Status> for Error {
    fn from(err: tonic::Status) -> Self {
        Self::RpcError(err)
    }
}

/// For initiate a Bigtable connection, then a `Bigtable` client can be made from it.
#[derive(Clone)]
pub struct BigTableConnection {
    client: BigtableClient<AuthSvc>,
    table_prefix: Arc<String>,
    timeout: Arc<Option<Duration>>,
}

impl BigTableConnection {
    /// Establish a connection to the BigTable instance named `instance_name`. By default,
    /// read-write access is provided. Use the `client_type` parameter to specify different
    /// access levels (ReadOnly, ReadWrite, or Admin).
    ///
    /// The GOOGLE_APPLICATION_CREDENTIALS environment variable will be used to determine the
    /// program name that contains the BigTable instance in addition to access credentials.
    ///
    /// The BIGTABLE_EMULATOR_HOST environment variable is also respected.
    ///
    /// `channel_size` defines the number of connections (or channels) established to Bigtable
    /// service, and the requests are load balanced onto all the channels. You must therefore
    /// make sure all of these connections are open when a new request is to be sent.
    /// Idle connections are automatically closed in "a few minutes". Therefore it is important to
    /// make sure you have a high enough QPS to send at least one request through all the
    /// connections (in every service host) every minute. If not, you should consider decreasing the
    /// channel size. If you are not sure what value to pick and your load is low, just start with 1.
    /// The recommended value could be 2 x the thread count in your tokio environment see info here
    /// https://docs.rs/tokio/latest/tokio/attr.main.html, but it might be a very different case for
    /// different applications.
    ///
    pub async fn new(
        project_id: &str,
        instance_name: &str,
        client_type: Option<ClientType>,
        channel_size: usize,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        match std::env::var("BIGTABLE_EMULATOR_HOST") {
            Ok(endpoint) => Self::new_with_emulator(
                endpoint.as_str(),
                project_id,
                instance_name,
                client_type.unwrap_or_default(),
                timeout,
            ),

            Err(_) => {
                let token_provider = gcp_auth::provider().await?;
                Self::new_with_token_provider(
                    project_id,
                    instance_name,
                    client_type.unwrap_or_default(),
                    channel_size,
                    timeout,
                    token_provider,
                )
                .await
            }
        }
    }

    /// Establish a connection to the BigTable instance named `instance_name`. By default,
    /// read-write access is provided. Use the `client_type` parameter to specify different
    /// access levels (ReadOnly, ReadWrite, or Admin).
    ///
    /// The `authentication_manager` variable will be used to determine the
    /// program name that contains the BigTable instance in addition to access credentials.
    ///
    /// `channel_size` defines the number of connections (or channels) established to Bigtable
    /// service, and the requests are load balanced onto all the channels. You must therefore
    /// make sure all of these connections are open when a new request is to be sent.
    /// Idle connections are automatically closed in "a few minutes". Therefore it is important to
    /// make sure you have a high enough QPS to send at least one request through all the
    /// connections (in every service host) every minute. If not, you should consider decreasing the
    /// channel size. If you are not sure what value to pick and your load is low, just start with 1.
    /// The recommended value could be 2 x the thread count in your tokio environment see info here
    /// https://docs.rs/tokio/latest/tokio/attr.main.html, but it might be a very different case for
    /// different applications.
    ///
    pub async fn new_with_token_provider(
        project_id: &str,
        instance_name: &str,
        client_type: ClientType,
        channel_size: usize,
        timeout: Option<Duration>,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Result<Self> {
        let config = Config {
            concurrency_limit: 100,
            timeout,
            max_grpc_connections: channel_size,
            token_provider: Some(DebugIgnore(token_provider)),
            endpoint_cb: None,
            tonic_cb: None,
        };

        Self::from_config(project_id, instance_name, client_type, config).await
    }

    pub async fn from_config(
        project_id: &str,
        instance_name: &str,
        client_type: ClientType,
        config: Config,
    ) -> Result<Self> {
        match std::env::var("BIGTABLE_EMULATOR_HOST") {
            Ok(endpoint) => Self::new_with_emulator(
                endpoint.as_str(),
                project_id,
                instance_name,
                client_type,
                config.timeout,
            ),

            Err(_) => {
                let table_prefix = format!(
                    "projects/{}/instances/{}/tables/",
                    project_id, instance_name
                );

                let endpoints: Result<Vec<Endpoint>> = vec![0; config.max_grpc_connections.max(1)]
                    .iter()
                    .map(move |_| {
                        Channel::from_static("https://bigtable.googleapis.com")
                            .concurrency_limit(config.concurrency_limit)
                            .tls_config(
                                ClientTlsConfig::new()
                                    .ca_certificate(
                                        root_ca_certificate::load()
                                            .map_err(Error::CertificateError)
                                            .expect("root certificate error"),
                                    )
                                    .domain_name("bigtable.googleapis.com"),
                            )
                            .map_err(Error::TransportError)
                    })
                    .collect();

                let endpoints: Vec<Endpoint> = endpoints?
                    .into_iter()
                    .map(|ep| {
                        ep.http2_keep_alive_interval(Duration::from_secs(60))
                            .keep_alive_while_idle(true)
                    })
                    .map(|ep| {
                        if let Some(timeout) = config.timeout {
                            ep.timeout(timeout)
                        } else {
                            ep
                        }
                    })
                    .map(|ep| {
                        // allow customizing the endpoint
                        if let Some(cb) = config.endpoint_cb {
                            cb(ep)
                        } else {
                            ep
                        }
                    })
                    .collect();

                // construct a channel, by balancing over all endpoints.
                let channel = Channel::balance_list(endpoints.into_iter());
                // Unwrap the DebugIgnore wrapper to get the Arc<dyn TokenProvider>
                let mut token_provider = match config.token_provider {
                    Some(wrapped_provider) => Some(wrapped_provider.0),
                    None => None,
                };
                if token_provider.is_none() {
                    token_provider = Some(gcp_auth::provider().await?);
                }
                Ok(Self {
                    client: create_client(channel, token_provider, client_type, config.tonic_cb),
                    table_prefix: Arc::new(table_prefix),
                    timeout: Arc::new(config.timeout),
                })
            }
        }
    }

    /// Establish a connection to a BigTable emulator at [emulator_endpoint].
    /// This is usually covered by [Self::new] or [Self::new_with_auth_manager],
    /// which both support the `BIGTABLE_EMULATOR_HOST` env variable. However,
    /// this function can also be used directly, in case setting
    /// `BIGTABLE_EMULATOR_HOST` is inconvenient.
    pub fn new_with_emulator(
        emulator_endpoint: &str,
        project_id: &str,
        instance_name: &str,
        client_type: ClientType,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        info!("Connecting to bigtable emulator at {}", emulator_endpoint);

        // configures the endpoint with the specified parameters
        fn configure_endpoint(endpoint: Endpoint, timeout: Option<Duration>) -> Endpoint {
            let endpoint = endpoint
                .http2_keep_alive_interval(Duration::from_secs(60))
                .keep_alive_while_idle(true);

            if let Some(timeout) = timeout {
                endpoint.timeout(timeout)
            } else {
                endpoint
            }
        }

        // Parse emulator_endpoint. Officially, it's only host:port,
        // but unix:///path/to/unix.sock also works in the Go SDK at least.
        // Having the emulator listen on unix domain sockets without ip2unix is
        // covered in https://github.com/googleapis/google-cloud-go/pull/9665.
        let channel = if let Some(path) = emulator_endpoint.strip_prefix("unix://") {
            // the URL doesn't matter, we use a custom connector.
            let endpoint = Endpoint::from_static("http://[::]:50051");
            let endpoint = configure_endpoint(endpoint, timeout);

            let path: String = path.to_string();
            let connector = tower::service_fn({
                move |_: tonic::transport::Uri| {
                    let path = path.clone();
                    async move {
                        let stream = UnixStream::connect(path).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                    }
                }
            });

            endpoint.connect_with_connector_lazy(connector)
        } else {
            let endpoint = Channel::from_shared(format!("http://{}", emulator_endpoint))
                .expect("invalid connection emulator uri");
            let endpoint = configure_endpoint(endpoint, timeout);

            endpoint.connect_lazy()
        };

        Ok(Self {
            client: create_client(channel, None, client_type, None),
            table_prefix: Arc::new(format!(
                "projects/{}/instances/{}/tables/",
                project_id, instance_name
            )),
            timeout: Arc::new(timeout),
        })
    }

    /// Create a new BigTable client by cloning needed properties.
    ///
    /// Clients require `&mut self`, due to `Tonic::transport::Channel` limitations, however
    /// the created new clients can be cheaply cloned and thus can be send to different threads
    pub fn client(&self) -> BigTable {
        BigTable {
            client: self.client.clone(),
            table_prefix: self.table_prefix.clone(),
            timeout: self.timeout.clone(),
        }
    }

    /// Provide a convenient method to update the inner `BigtableClient` so a newly configured client can be set
    pub fn configure_inner_client(
        &mut self,
        config_fn: fn(BigtableClient<AuthSvc>) -> BigtableClient<AuthSvc>,
    ) {
        self.client = config_fn(self.client.clone());
    }
}

/// Helper function to create a BigtableClient<AuthSvc>
/// from a channel.
fn create_client(
    channel: Channel,
    token_provider: Option<Arc<dyn TokenProvider>>,
    client_type: ClientType,
    cb: Option<
        fn(
            ServiceBuilder<tower::layer::util::Identity>,
        ) -> ServiceBuilder<tower::layer::util::Identity>,
    >,
) -> BigtableClient<AuthSvc> {
    let scopes = match client_type {
        ClientType::ReadOnly => "https://www.googleapis.com/auth/bigtable.data.readonly",
        ClientType::ReadWrite => "https://www.googleapis.com/auth/bigtable.data",
        ClientType::Admin => "https://www.googleapis.com/auth/bigtable.admin",
    };
    let mut builder = ServiceBuilder::new();
    // Apply the callback first if it exists
    if let Some(cb) = cb {
        builder = cb(builder);
    }
    // Then add the auth layer
    let auth_svc = builder
        .layer_fn(|c| AuthSvc::new(c, token_provider.clone(), scopes.to_string()))
        .service(channel);

    // Create client with custom codec config
    BigtableClient::new(auth_svc)
        .max_decoding_message_size(64 * 1024 * 1024) // 64MB
        .max_encoding_message_size(64 * 1024 * 1024) // 64MB
}

/// The core struct for Bigtable client, which wraps a gPRC client defined by Bigtable proto.
/// In order to easily use this struct in multiple threads, we only store cloneable references here.
/// `BigtableClient<AuthSvc>` is a type alias of `BigtableClient` and it wraps a tonic Channel.
/// Cloning on `Bigtable` is cheap.
///
/// Bigtable can be created via `bigtable::BigTableConnection::new()` and cloned
/// ```rust,no_run
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///   use bigtable_rs::bigtable;
///   let connection = bigtable::BigTableConnection::new("p-id", "i-id", true, 1, None).await?;
///   let bt_client = connection.client();
///   // Cheap to clone clients and used in other places.
///   let bt_client2 = bt_client.clone();
///   Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct BigTable {
    // clone is cheap with Channel, see https://docs.rs/tonic/latest/tonic/transport/struct.Channel.html
    client: BigtableClient<AuthSvc>,
    table_prefix: Arc<String>,
    timeout: Arc<Option<Duration>>,
}

impl BigTable {
    /// Performs a conditional mutation on a row.
    ///
    /// This method atomically checks a condition on a row and mutates it if the condition is true.
    ///
    /// # Example
    /// ```ignore
    /// use bigtable_rs::google::bigtable::v2::{CheckAndMutateRowRequest, Mutation};
    ///
    /// let request = CheckAndMutateRowRequest {
    ///     table_name: bigtable.get_full_table_name("table-1"),
    ///     predicate_filter: Some(/* row filter */),
    ///     true_mutations: vec![/* mutations if condition is true */],
    ///     false_mutations: vec![/* mutations if condition is false */],
    ///     ..Default::default()
    /// };
    ///
    /// let response = bigtable.check_and_mutate_row(request).await?;
    /// println!("Predicate matched: {}", response.predicate_matched);
    /// ```
    pub async fn check_and_mutate_row(
        &mut self,
        request: CheckAndMutateRowRequest,
    ) -> Result<CheckAndMutateRowResponse> {
        let response = self
            .client
            .check_and_mutate_row(request)
            .await?
            .into_inner();
        Ok(response)
    }

    /// Reads rows from a table using a prefix-based row range.
    ///
    /// This is a convenience method that constructs a row range from a prefix and reads all rows
    /// that start with that prefix.
    ///
    /// # Example
    /// ```ignore
    /// use bigtable_rs::google::bigtable::v2::ReadRowsRequest;
    ///
    /// let request = ReadRowsRequest {
    ///     table_name: bigtable.get_full_table_name("table-1"),
    ///     ..Default::default()
    /// };
    ///
    /// let prefix = "user_".as_bytes().to_vec();
    /// let rows = bigtable.read_rows_with_prefix(request, prefix).await?;
    ///
    /// for (key, cells) in rows {
    ///     println!("Row key: {}", String::from_utf8_lossy(&key));
    ///     for cell in cells {
    ///         println!("  {} : {} = {}",
    ///             cell.family_name,
    ///             String::from_utf8_lossy(&cell.qualifier),
    ///             String::from_utf8_lossy(&cell.value)
    ///         );
    ///     }
    /// }
    /// ```
    pub async fn read_rows_with_prefix(
        &mut self,
        mut request: ReadRowsRequest,
        prefix: Vec<u8>,
    ) -> Result<Vec<(RowKey, Vec<RowCell>)>> {
        let row_range = get_row_range_from_prefix(prefix);
        request.rows = Some(RowSet {
            row_keys: vec![], // use this field to put keys for reading specific rows
            row_ranges: vec![row_range],
        });
        let response = self.client.read_rows(request).await?.into_inner();
        decode_read_rows_response(self.timeout.as_ref(), response).await
    }

    /// Streams rows from a table, returning them as they are received.
    ///
    /// This method is useful when dealing with large result sets as it allows processing
    /// rows as they arrive rather than waiting for all rows to be received.
    ///
    /// # Example
    /// ```ignore
    /// use futures_util::StreamExt;
    /// use bigtable_rs::google::bigtable::v2::ReadRowsRequest;
    ///
    /// let request = ReadRowsRequest {
    ///     table_name: bigtable.get_full_table_name("table-1"),
    ///     rows_limit: 1000,
    ///     ..Default::default()
    /// };
    ///
    /// let mut stream = bigtable.stream_rows(request).await?;
    ///
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok((key, cells)) => {
    ///             println!("Received row: {}", String::from_utf8_lossy(&key));
    ///             for cell in cells {
    ///                 println!("  Data: {}", String::from_utf8_lossy(&cell.value));
    ///             }
    ///         }
    ///         Err(e) => println!("Error processing row: {}", e),
    ///     }
    /// }
    /// ```
    pub async fn stream_rows(
        &mut self,
        request: ReadRowsRequest,
    ) -> Result<impl Stream<Item = Result<(RowKey, Vec<RowCell>)>>> {
        let response = self.client.read_rows(request).await?.into_inner();
        let stream = decode_read_rows_response_stream(response).await;
        Ok(stream)
    }

    /// Streams rows from a table using a prefix-based row range.
    ///
    /// This combines the functionality of `stream_rows` and `read_rows_with_prefix`,
    /// allowing streaming access to rows matching a prefix.
    ///
    /// # Example
    /// ```ignore
    /// use futures_util::StreamExt;
    /// use bigtable_rs::google::bigtable::v2::ReadRowsRequest;
    ///
    /// let request = ReadRowsRequest {
    ///     table_name: bigtable.get_full_table_name("table-1"),
    ///     ..Default::default()
    /// };
    ///
    /// let prefix = "user_".as_bytes().to_vec();
    /// let mut stream = bigtable.stream_rows_with_prefix(request, prefix).await?;
    ///
    /// while let Some(result) = stream.next().await {
    ///     match result {
    ///         Ok((key, cells)) => {
    ///             println!("Received row: {}", String::from_utf8_lossy(&key));
    ///             for cell in cells {
    ///                 println!("  {} : {} = {}",
    ///                     cell.family_name,
    ///                     String::from_utf8_lossy(&cell.qualifier),
    ///                     String::from_utf8_lossy(&cell.value)
    ///                 );
    ///             }
    ///         }
    ///         Err(e) => println!("Error processing row: {}", e),
    ///     }
    /// }
    /// ```
    pub async fn stream_rows_with_prefix(
        &mut self,
        mut request: ReadRowsRequest,
        prefix: Vec<u8>,
    ) -> Result<impl Stream<Item = Result<(RowKey, Vec<RowCell>)>>> {
        let row_range = get_row_range_from_prefix(prefix);
        request.rows = Some(RowSet {
            row_keys: vec![],
            row_ranges: vec![row_range],
        });
        let response = self.client.read_rows(request).await?.into_inner();
        let stream = decode_read_rows_response_stream(response).await;
        Ok(stream)
    }

    /// Returns a set of sample row keys in the table.
    ///
    /// This method is useful for:
    /// - Determining table size and row distribution
    /// - Planning parallel reads
    /// - Choosing row key ranges for batch operations
    ///
    /// # Example
    /// ```ignore
    /// use futures_util::StreamExt;
    /// use bigtable_rs::google::bigtable::v2::SampleRowKeysRequest;
    ///
    /// let request = SampleRowKeysRequest {
    ///     table_name: bigtable.get_full_table_name("table-1"),
    ///     ..Default::default()
    /// };
    ///
    /// let mut stream = bigtable.sample_row_keys(request).await?;
    ///
    /// while let Some(response) = stream.message().await? {
    ///     println!(
    ///         "Row key: {}, Offset: {}",
    ///         String::from_utf8_lossy(&response.row_key),
    ///         response.offset_bytes
    ///     );
    /// }
    /// ```
    pub async fn sample_row_keys(
        &mut self,
        request: SampleRowKeysRequest,
    ) -> Result<Streaming<SampleRowKeysResponse>> {
        let response = self.client.sample_row_keys(request).await?.into_inner();
        Ok(response)
    }

    /// Mutates a single row atomically.
    ///
    /// # Example
    /// ```ignore
    /// use bigtable_rs::google::bigtable::v2::{MutateRowRequest, Mutation};
    /// use bigtable_rs::google::bigtable::v2::mutation::SetCell;
    ///
    /// let mutation = Mutation {
    ///     mutation: Some(bigtable_rs::google::bigtable::v2::mutation::Mutation::SetCell(
    ///         SetCell {
    ///             family_name: "cf1".to_string(),
    ///             column_qualifier: "col1".as_bytes().to_vec(),
    ///             timestamp_micros: -1, // Server-assigned timestamp
    ///             value: "new_value".as_bytes().to_vec(),
    ///         }
    ///     )),
    /// };
    ///
    /// let request = MutateRowRequest {
    ///     table_name: bigtable.get_full_table_name("table-1"),
    ///     row_key: "key1".as_bytes().to_vec(),
    ///     mutations: vec![mutation],
    ///     ..Default::default()
    /// };
    ///
    /// let response = bigtable.mutate_row(request).await?;
    /// ```
    pub async fn mutate_row(
        &mut self,
        request: MutateRowRequest,
    ) -> Result<Response<MutateRowResponse>> {
        let response = self.client.mutate_row(request).await?;
        Ok(response)
    }

    /// Mutates multiple rows in a single request.
    ///
    /// This method is more efficient than calling `mutate_row` multiple times
    /// when you need to modify multiple rows.
    ///
    /// # Example
    /// ```ignore
    /// use bigtable_rs::google::bigtable::v2::{MutateRowsRequest, Mutation};
    /// use bigtable_rs::google::bigtable::v2::mutation::SetCell;
    ///
    /// let mutation = Mutation {
    ///     mutation: Some(bigtable_rs::google::bigtable::v2::mutation::Mutation::SetCell(
    ///         SetCell {
    ///             family_name: "cf1".to_string(),
    ///             column_qualifier: "col1".as_bytes().to_vec(),
    ///             timestamp_micros: -1,
    ///             value: "new_value".as_bytes().to_vec(),
    ///         }
    ///     )),
    /// };
    ///
    /// let entries = vec![
    ///     bigtable_rs::google::bigtable::v2::mutate_rows_request::Entry {
    ///         row_key: "key1".as_bytes().to_vec(),
    ///         mutations: vec![mutation.clone()],
    ///     },
    ///     bigtable_rs::google::bigtable::v2::mutate_rows_request::Entry {
    ///         row_key: "key2".as_bytes().to_vec(),
    ///         mutations: vec![mutation],
    ///     },
    /// ];
    ///
    /// let request = MutateRowsRequest {
    ///     table_name: bigtable.get_full_table_name("table-1"),
    ///     entries,
    ///     ..Default::default()
    /// };
    ///
    /// let mut stream = bigtable.mutate_rows(request).await?;
    /// while let Some(response) = stream.message().await? {
    ///     for entry in response.entries {
    ///         println!(
    ///             "Row {} mutation status: {:?}",
    ///             entry.index,
    ///             entry.status
    ///         );
    ///     }
    /// }
    /// ```
    pub async fn mutate_rows(
        &mut self,
        request: MutateRowsRequest,
    ) -> Result<Streaming<MutateRowsResponse>> {
        let response = self.client.mutate_rows(request).await?.into_inner();
        Ok(response)
    }

    /// Provide a convenient method to get the inner `BigtableClient` so user can use any methods
    /// defined from the Bigtable V2 gRPC API
    pub fn get_client(&mut self) -> &mut BigtableClient<AuthSvc> {
        &mut self.client
    }

    /// Provide a convenient method to update the inner `BigtableClient` config
    pub fn configure_inner_client(
        &mut self,
        config_fn: fn(BigtableClient<AuthSvc>) -> BigtableClient<AuthSvc>,
    ) {
        self.client = config_fn(self.client.clone());
    }

    /// Provide a convenient method to get full table, which can be used for building requests
    pub fn get_full_table_name(&self, table_name: &str) -> String {
        [&self.table_prefix, table_name].concat()
    }

    /// Reads rows from a table.
    ///
    /// This method allows reading multiple rows from a table using various filters and row ranges.
    /// For reading rows with a specific prefix, see `read_rows_with_prefix`.
    ///
    /// # Example
    /// ```ignore
    /// use bigtable_rs::google::bigtable::v2::{ReadRowsRequest, RowSet, RowRange};
    /// use bigtable_rs::google::bigtable::v2::row_range::{StartKey, EndKey};
    ///
    /// let request = ReadRowsRequest {
    ///     table_name: bigtable.get_full_table_name("table-1"),
    ///     rows: Some(RowSet {
    ///         row_keys: vec!["specific_key".as_bytes().to_vec()],
    ///         row_ranges: vec![RowRange {
    ///             start_key: Some(StartKey::StartKeyClosed("start".as_bytes().to_vec())),
    ///             end_key: Some(EndKey::EndKeyOpen("end".as_bytes().to_vec())),
    ///         }],
    ///     }),
    ///     rows_limit: 1000,
    ///     ..Default::default()
    /// };
    ///
    /// let rows = bigtable.read_rows(request).await?;
    ///
    /// for (key, cells) in rows {
    ///     println!("Row key: {}", String::from_utf8_lossy(&key));
    ///     for cell in cells {
    ///         println!("  {} : {} = {} @ {}",
    ///             cell.family_name,
    ///             String::from_utf8_lossy(&cell.qualifier),
    ///             String::from_utf8_lossy(&cell.value),
    ///             cell.timestamp_micros
    ///         );
    ///     }
    /// }
    /// ```
    pub async fn read_rows(
        &mut self,
        request: ReadRowsRequest,
    ) -> Result<Vec<(RowKey, Vec<RowCell>)>> {
        let response = self.client.read_rows(request).await?.into_inner();
        decode_read_rows_response(self.timeout.as_ref(), response).await
    }
}
