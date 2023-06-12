use std::{convert::Infallible, future::Future};

use executor::{
    net::{Async, TcpListener},
    spawn_local, Executor,
};
use futures_lite::StreamExt;
use hyper::{server::conn::Http, service::service_fn, Body, Request, Response};

async fn hello_world(_req: Request<Body>) -> Result<Response<Body>, Infallible> {
    Ok(Response::new("Hello, World".into()))
}

fn main() {
    let address = "127.0.0.1:1090".parse().unwrap();
    let service = service_fn(hello_world);
    Executor::builder()
        .worker_num(16)
        .build()
        .expect("start async executor failed.")
        .run(|| async {
            #[derive(Clone)]
            struct LocalExecutor;

            impl<Fut> hyper::rt::Executor<Fut> for LocalExecutor
            where
                Fut: 'static + Future,
            {
                fn execute(&self, fut: Fut) {
                    spawn_local(fut).detach()
                }
            }

            let mut listener = Async::<TcpListener>::connect(address)
                .expect(format!("server bind {} failed.", address).as_ref());
            while let Some(stream) = listener.next().await {
                match stream {
                    Ok(stream) => {
                        let peer_addr = stream
                            .as_ref()
                            .peer_addr()
                            .expect("can not get peer address, is it under TCP mode?");
                        spawn_local(async move {
                            Http::new()
                                .with_executor(LocalExecutor)
                                .http1_keep_alive(true)
                                .serve_connection(stream, service)
                                .await
                                .unwrap_or_else(|e| {
                                    tracing::warn!("send response to {} error: {}.", peer_addr, e);
                                });
                        })
                        .detach();
                    }
                    Err(e) => {
                        tracing::warn!("get tcp stream error: {}.", e);
                        continue;
                    }
                }
            }
        });
}
