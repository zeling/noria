use crate::consensus::{self, Authority};
use crate::debug::stats;
use crate::table::{Table, TableBuilder, TableRpc};
use crate::view::{View, ViewBuilder, ViewRpc};
use crate::ActivationResult;
#[cfg(debug_assertions)]
use assert_infrequent;
use bincode;
use failure::{self, ResultExt};
use futures_util::{future, try_stream::TryStreamExt};
use hyper;
use petgraph::graph::NodeIndex;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::Write;
use std::net::SocketAddr;
use std::os::unix::io::FromRawFd;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{
    future::Future,
    task::{Context, Poll},
};
use tower_buffer::Buffer;
use tower_service::Service;

/// Describes a running controller instance.
///
/// A serialized version of this struct is stored in ZooKeeper so that clients can reach the
/// currently active controller.
#[derive(Clone, Serialize, Deserialize)]
#[doc(hidden)]
pub struct ControllerDescriptor {
    pub external_addr: SocketAddr,
    pub worker_addr: SocketAddr,
    pub domain_addr: SocketAddr,
    pub nonce: u64,
}

struct Controller<A> {
    authority: Arc<A>,
    client: hyper::Client<hyper::client::HttpConnector>,
}

#[derive(Debug)]
struct ControllerRequest {
    path: &'static str,
    request: Vec<u8>,
}

impl ControllerRequest {
    fn new<Q: Serialize>(path: &'static str, r: Q) -> Result<Self, serde_json::Error> {
        Ok(ControllerRequest {
            path,
            request: serde_json::to_vec(&r)?,
        })
    }
}

impl<A> Service<ControllerRequest> for Controller<A>
where
    A: 'static + Authority,
{
    type Response = hyper::Chunk;
    type Error = failure::Error;

    type Future = impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: ControllerRequest) -> Self::Future {
        let client = self.client.clone();
        let auth = self.authority.clone();
        let path = req.path;
        let body = req.request;

        async move {
            let mut url = None;

            loop {
                if url.is_none() {
                    // TODO: don't do blocking things here...
                    // TODO: cache this value?
                    let descriptor: ControllerDescriptor = serde_json::from_slice(
                        &auth.get_leader().context("failed to get current leader")?.1,
                    )
                    .context("failed to deserialize authority reply")?;

                    url = Some(format!("http://{}/{}", descriptor.external_addr, path));
                }

                let r = hyper::Request::post(url.as_ref().unwrap())
                    .body(hyper::Body::from(body.clone()))
                    .unwrap();

                let res = client
                    .request(r)
                    .await
                    .map_err(|he| failure::Error::from(he).context("hyper request failed"))?;

                let status = res.status();
                let body = res
                    .into_body()
                    .try_concat()
                    .await
                    .map_err(|he| failure::Error::from(he).context("hyper response failed"))?;

                match status {
                    hyper::StatusCode::OK => return Ok(body),
                    hyper::StatusCode::INTERNAL_SERVER_ERROR => bail!(
                        "rpc call to {} failed: {}",
                        path,
                        String::from_utf8_lossy(&*body)
                    ),
                    s => {
                        if s == hyper::StatusCode::SERVICE_UNAVAILABLE {
                            url = None;
                        }

                        tokio::timer::delay(Instant::now() + Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}

/// A handle to a Noria controller.
///
/// This handle is the primary mechanism for interacting with a running Noria instance, and lets
/// you add and remove queries, retrieve handles for inserting or querying the underlying data, and
/// to perform meta-operations such as fetching the dataflow's GraphViz visualization.
///
/// To establish a new connection to Noria, use `ControllerHandle::new`, and pass in the
/// appropriate `Authority`. In the likely case that you are using Zookeeper, use
/// `ControllerHandle::from_zk`.
///
/// Note that whatever Tokio Runtime you use to execute the `Future` that resolves into the
/// `ControllerHandle` will also be the one that executes all your reads and writes through `View`
/// and `Table`. Make sure that that `Runtime` stays alive, and continues to be driven, otherwise
/// none of your operations will ever complete! Furthermore, you *must* use the `Runtime` to
/// execute any futures returned from `ControllerHandle` (that is, you cannot just call `.wait()`
/// on them).
// TODO: this should be renamed to NoriaHandle, or maybe just Connection, since it also provides
// reads and writes, which aren't controller actions!
pub struct ControllerHandle<A>
where
    A: 'static + Authority,
{
    handle: Buffer<Controller<A>, ControllerRequest>,
    domains: Arc<Mutex<HashMap<(SocketAddr, usize), TableRpc>>>,
    views: Arc<Mutex<HashMap<(SocketAddr, usize), ViewRpc>>>,
    tracer: tracing::Dispatch,
}

impl<A> Clone for ControllerHandle<A>
where
    A: 'static + Authority,
{
    fn clone(&self) -> Self {
        ControllerHandle {
            handle: self.handle.clone(),
            domains: self.domains.clone(),
            views: self.views.clone(),
            tracer: self.tracer.clone(),
        }
    }
}

impl ControllerHandle<consensus::ZookeeperAuthority> {
    /// Fetch information about the current Soup controller from Zookeeper running at the given
    /// address, and create a `ControllerHandle` from that.
    pub async fn from_zk(zookeeper_address: &str) -> Result<Self, failure::Error> {
        let auth = consensus::ZookeeperAuthority::new(zookeeper_address)?;
        ControllerHandle::new(auth).await
    }
}

type RpcFuture<A, R> = impl Future<Output = Result<R, failure::Error>>;

// Needed b/c of https://github.com/rust-lang/rust/issues/65442
async fn finalize<R, E>(
    fut: impl Future<Output = Result<hyper::Chunk, E>>,
    err: &'static str,
) -> Result<R, failure::Error>
where
    for<'de> R: Deserialize<'de>,
    E: std::fmt::Display + Send + Sync + 'static,
{
    let body: hyper::Chunk = fut.await.map_err(failure::Context::new).context(err)?;

    serde_json::from_slice::<R>(&body)
        .context("failed to response")
        .context(err)
        .map_err(failure::Error::from)
}

impl<A: Authority + 'static> ControllerHandle<A> {
    #[doc(hidden)]
    pub async fn make(authority: Arc<A>) -> Result<Self, failure::Error> {
        // need to use lazy otherwise current executor won't be known
        let tracer = tracing::dispatcher::get_default(|d| d.clone());
        Ok(ControllerHandle {
            views: Default::default(),
            domains: Default::default(),
            handle: Buffer::new(
                Controller {
                    authority,
                    client: hyper::Client::new(),
                },
                1,
            ),
            tracer,
        })
    }

    /// Check that the `ControllerHandle` can accept another request.
    ///
    /// Note that this method _must_ return `Poll::Ready` before any other methods that return
    /// a `Future` on `ControllerHandle` can be called.
    pub fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), failure::Error>> {
        self.handle
            .poll_ready(cx)
            .map_err(failure::Error::from_boxed_compat)
    }

    /// A future that resolves when the controller can accept more messages.
    ///
    /// When this future resolves, you it is safe to call any methods that require `poll_ready` to
    /// have returned `Poll::Ready`.
    pub async fn ready(&mut self) -> Result<(), failure::Error> {
        future::poll_fn(move |cx| self.poll_ready(cx)).await
    }

    /// Create a `ControllerHandle` that bootstraps a connection to Noria via the configuration
    /// stored in the given `authority`.
    ///
    /// You *probably* want to use `ControllerHandle::from_zk` instead.
    pub async fn new(authority: A) -> Result<Self, failure::Error>
    where
        A: Send + 'static,
    {
        Self::make(Arc::new(authority)).await
    }

    /// Enumerate all known base tables.
    ///
    /// These have all been created in response to a `CREATE TABLE` statement in a recipe.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn inputs(
        &mut self,
    ) -> impl Future<Output = Result<BTreeMap<String, NodeIndex>, failure::Error>> {
        let fut = self
            .handle
            .call(ControllerRequest::new("inputs", &()).unwrap());

        async move {
            let body: hyper::Chunk = fut
                .await
                .map_err(failure::Context::new)
                .context("failed to fetch inputs")?;

            serde_json::from_slice(&body)
                .context("couldn't parse input response")
                .map_err(failure::Error::from)
        }
    }

    /// Enumerate all known external views.
    ///
    /// These have all been created in response to a `CREATE EXT VIEW` statement in a recipe.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn outputs(
        &mut self,
    ) -> impl Future<Output = Result<BTreeMap<String, NodeIndex>, failure::Error>> {
        let fut = self
            .handle
            .call(ControllerRequest::new("outputs", &()).unwrap());

        async move {
            let body: hyper::Chunk = fut
                .await
                .map_err(failure::Context::new)
                .context("failed to fetch outputs")?;

            serde_json::from_slice(&body)
                .context("couldn't parse output response")
                .map_err(failure::Error::from)
        }
    }

    /// Enumerate all know base tables along with there purposes.
    ///
    /// Currently the query name that is assigned serves as the purpose.
    pub fn purposes(
        &mut self,
    ) -> impl Future<Output = Result<BTreeMap<String, String>, failure::Error>> {
        let fut = self
            .handle
            .call(ControllerRequest::new("purposes", &()).unwrap());

        async move {
            let body: hyper::Chunk = fut
                .await
                .map_err(failure::Context::new)
                .context("failed to fetch purpose")?;

            serde_json::from_slice(&body)
                .context("couldn't parse purpose response")
                .map_err(failure::Error::from)
        }
    }

    /// Export user shard from the dataflow.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn export_user_shard(
        &mut self,
        user_id: String,
    ) -> impl Future<Output = Result<HashMap<String, Vec<Vec<u8>>>, failure::Error>> {
        self.rpc("export_user_shard", user_id, "failed to export user shard")
    }

    /// Import user shard to the dataflow.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn import_user_shard(
        &mut self,
        user_shard: HashMap<String, Vec<Vec<u8>>>,
    ) -> impl Future<Output = Result<(), failure::Error>> {
        self.rpc(
            "import_user_shard",
            user_shard,
            "failed to import user shard",
        )
    }

    /// Obtain a `View` that allows you to query the given external view.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn view(&mut self, name: &str) -> impl Future<Output = Result<View, failure::Error>> {
        // This call attempts to detect if this function is being called in a loop. If this is
        // getting false positives, then it is safe to increase the allowed hit count, however, the
        // limit_mutator_creation test in src/controller/handle.rs should then be updated as well.
        #[cfg(debug_assertions)]
        assert_infrequent::at_most(200);

        let views = self.views.clone();
        let name = name.to_string();
        let fut = self
            .handle
            .call(ControllerRequest::new("view_builder", &name).unwrap());
        async move {
            let body: hyper::Chunk = fut
                .await
                .map_err(failure::Context::new)
                .context("failed to fetch view builder")?;

            match serde_json::from_slice::<Option<ViewBuilder>>(&body) {
                Ok(Some(vb)) => Ok(vb.build(views)?),
                Ok(None) => Err(failure::err_msg("view does not exist")),
                Err(e) => Err(failure::Error::from(e)),
            }
            .map_err(move |e| e.context(format!("building view for {}", name)).into())
        }
    }

    /// Obtain a `Table` that allows you to perform writes, deletes, and other operations on the
    /// given base table.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn table(&mut self, name: &str) -> impl Future<Output = Result<Table, failure::Error>> {
        // This call attempts to detect if this function is being called in a loop. If this
        // is getting false positives, then it is safe to increase the allowed hit count.
        #[cfg(debug_assertions)]
        assert_infrequent::at_most(200);

        let domains = self.domains.clone();
        let name = name.to_string();
        let fut = self
            .handle
            .call(ControllerRequest::new("table_builder", &name).unwrap());

        async move {
            let body: hyper::Chunk = fut
                .await
                .map_err(failure::Context::new)
                .context("failed to fetch table builder")?;

            match serde_json::from_slice::<Option<TableBuilder>>(&body) {
                Ok(Some(tb)) => Ok(tb.build(domains)?),
                Ok(None) => Err(failure::err_msg("view table not exist")),
                Err(e) => Err(failure::Error::from(e)),
            }
            .map_err(move |e| e.context(format!("building table for {}", name)).into())
        }
    }

    #[doc(hidden)]
    pub fn rpc<Q: Serialize, R: 'static>(
        &mut self,
        path: &'static str,
        r: Q,
        err: &'static str,
    ) -> RpcFuture<A, R>
    where
        for<'de> R: Deserialize<'de>,
        R: Send,
    {
        let fut = self.handle.call(ControllerRequest::new(path, r).unwrap());

        finalize(fut, err)
    }

    /// Get statistics about the time spent processing different parts of the graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn statistics(
        &mut self,
    ) -> impl Future<Output = Result<stats::GraphStats, failure::Error>> {
        self.rpc("get_statistics", (), "failed to get stats")
    }

    /// Flush all partial state, evicting all rows present.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn flush_partial(&mut self) -> impl Future<Output = Result<(), failure::Error>> {
        self.rpc("flush_partial", (), "failed to flush partial")
    }

    /// Extend the existing recipe with the given set of queries.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn extend_recipe(
        &mut self,
        recipe_addition: &str,
    ) -> impl Future<Output = Result<ActivationResult, failure::Error>> {
        self.rpc("extend_recipe", recipe_addition, "failed to extend recipe")
    }

    /// Replace the existing recipe with this one.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn install_recipe(
        &mut self,
        new_recipe: &str,
    ) -> impl Future<Output = Result<ActivationResult, failure::Error>> {
        self.rpc("install_recipe", new_recipe, "failed to install recipe")
    }

    /// Fetch a graphviz description of the dataflow graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn graphviz(&mut self) -> impl Future<Output = Result<String, failure::Error>> {
        self.rpc("graphviz", (), "failed to fetch graphviz output")
    }

    /// Fetch a simplified graphviz description of the dataflow graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn simple_graphviz(&mut self) -> impl Future<Output = Result<String, failure::Error>> {
        self.rpc(
            "simple_graphviz",
            (),
            "failed to fetch simple graphviz output",
        )
    }

    /// Remove the given external view from the graph.
    ///
    /// `Self::poll_ready` must have returned `Async::Ready` before you call this method.
    pub fn remove_node(
        &mut self,
        view: NodeIndex,
    ) -> impl Future<Output = Result<(), failure::Error>> {
        // TODO: this should likely take a view name, and we should verify that it's a Reader.
        self.rpc("remove_node", view, "failed to remove node")
    }
}

fn encrypt_and_sign<S: Serialize, F: FnMut(&mut File)>(
    s: S,
    mut f: F,
    me: &str,
    recipient: &str,
) -> Result<String, failure::Error> {
    let (r, w) = nix::unistd::pipe()?;
    let mut gpg = Command::new("gpg")
        .arg("--armor")
        .arg("--encrypt")
        .arg("--local-user")
        .arg(me)
        .arg("-r")
        .arg(recipient)
        .arg("--passphrase-fd")
        .arg(r.to_string())
        .arg("--sign")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    {
        let mut passphrase = unsafe { File::from_raw_fd(w) };
        f(&mut passphrase);
        passphrase.flush()?;
    }
    {
        let input = gpg.stdin.as_mut().expect("failed to get stdin");
        bincode::serialize_into(input, &s)?;
    }
    let output = gpg.wait_with_output()?;
    if !output.status.success() {
        return Err(format_err!("failed to run gpg"));
    }
    Ok(String::from_utf8(output.stdout)?)
}

fn verify_and_decrypt<D: for<'a> Deserialize<'a>, F: FnMut(&mut File)>(
    armored: &str,
    me: &str,
    sender: &str,
    mut f: F,
) -> Result<D, failure::Error> {
    let (r, w) = nix::unistd::pipe()?;
    let mut gpg = Command::new("gpg")
        .arg("--armor")
        .arg("--decrypt")
        .arg("--local-user")
        .arg(me)
        .arg("--passphrase-fd")
        .arg(r.to_string())
        .arg("--status-fd") // machine readable status
        .arg("2")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    {
        let mut passphrase = unsafe { File::from_raw_fd(w) };
        f(&mut passphrase);
        passphrase.flush()?;
    }
    {
        let input = gpg.stdin.as_mut().expect("failed to get stdin");
        input.write_all(armored.as_bytes())?;
    }
    let output = gpg.wait_with_output()?;
    if !output.status.success() {
        return Err(format_err!("failed to run gpg"));
    }
    let re = Regex::new(&format!(r"GOODSIG .* <{}>", sender)).unwrap();
    println!("{}", std::str::from_utf8(&output.stderr[..]).unwrap());
    if !re.is_match(std::str::from_utf8(&output.stderr[..]).unwrap()) {
        return Err(format_err!("failed to verify the sender {}", sender));
    }
    Ok(bincode::deserialize(&output.stdout[..])?)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Write;
    #[test]
    fn test_encrypt_and_sign_and_verify_and_decrypt() {
        let sender = "test.sender@example.com";
        let receiver = "test.receiver@example.com";
        let encrypted = encrypt_and_sign(
            "abc",
            |f| {
                f.write_all(b"123456\n").unwrap();
            },
            sender,
            receiver,
        )
        .expect("should encrypt");
        let decrypted: String = verify_and_decrypt(&encrypted, receiver, sender, |f| {
            f.write_all(b"123456\n").unwrap();
        })
        .expect("should decrypt");
        assert_eq!(decrypted, "abc");
    }
}
