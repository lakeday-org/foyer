// Copyright 2026 foyer Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    fmt::Debug,
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

use tokio::{
    runtime::{Handle, Runtime},
    task::JoinHandle,
};

use crate::error::{Error, ErrorKind, Result};

/// A wrapper around a dedicated tokio [`Runtime`] that can shut the runtime down in the background.
///
/// The runtime is shared by [`Arc`], so dropping the *last* clone does **not** deterministically run
/// the shutdown: tasks spawned on the runtime may keep a clone alive themselves (e.g. via a captured
/// [`Spawner`]), forming a self-cycle that prevents the strong count from ever reaching zero. To
/// break such a cycle, call [`BackgroundShutdownRuntime::shutdown`] explicitly; the wrapper also
/// defers the shutdown to a background step when the last clone is eventually dropped.
///
/// This is necessary because directly dropping a nested runtime is not allowed in a parent runtime;
/// [`BackgroundShutdownRuntime::shutdown`] (like [`Runtime::shutdown_background`]) may be called from
/// within the runtime itself or a parent runtime.
pub struct BackgroundShutdownRuntime {
    runtime: Mutex<Option<Runtime>>,
    handle: Handle,
}

impl Debug for BackgroundShutdownRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackgroundShutdownRuntime").finish()
    }
}

impl BackgroundShutdownRuntime {
    /// Deterministically shut the dedicated runtime down in the background.
    ///
    /// This is idempotent: the first call initiates the shutdown and subsequent calls are no-ops.
    ///
    /// This may be called from within the runtime itself or from a parent runtime. After this
    /// returns, the runtime is being torn down in the background: all spawned tasks are dropped
    /// (releasing any [`Arc`] clones they captured) and the worker threads exit.
    pub fn shutdown(&self) {
        if let Some(runtime) = self.runtime.lock().unwrap().take() {
            #[cfg(madsim)]
            drop(runtime);
            #[cfg(not(madsim))]
            runtime.shutdown_background();
        }
    }
}

impl Drop for BackgroundShutdownRuntime {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl Deref for BackgroundShutdownRuntime {
    type Target = Handle;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl DerefMut for BackgroundShutdownRuntime {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.handle
    }
}

impl From<Runtime> for BackgroundShutdownRuntime {
    fn from(runtime: Runtime) -> Self {
        let handle = runtime.handle().clone();
        Self {
            runtime: Mutex::new(Some(runtime)),
            handle,
        }
    }
}

/// A wrapper for [`JoinHandle`].
#[derive(Debug)]
pub struct SpawnHandle<T> {
    inner: JoinHandle<T>,
}

impl<T> std::future::Future for SpawnHandle<T> {
    type Output = Result<T>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        match std::pin::Pin::new(&mut self.inner).poll(cx) {
            std::task::Poll::Ready(res) => match res {
                Ok(v) => std::task::Poll::Ready(Ok(v)),
                Err(e) => std::task::Poll::Ready(Err(Error::new(ErrorKind::Join, "tokio join error").with_source(e))),
            },
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

/// A wrapper around a dedicated tokio runtime or handle to spawn tasks.
#[derive(Debug, Clone)]
pub enum Spawner {
    /// A dedicated runtime to spawn tasks.
    Runtime(Arc<BackgroundShutdownRuntime>),
    /// A handle to spawn tasks.
    Handle(Handle),
}

impl From<Runtime> for Spawner {
    fn from(runtime: Runtime) -> Self {
        Self::Runtime(Arc::new(runtime.into()))
    }
}

impl From<Handle> for Spawner {
    fn from(handle: Handle) -> Self {
        Self::Handle(handle)
    }
}

impl Spawner {
    /// Wrapper for [`Runtime::spawn`] or [`Handle::spawn`].
    pub fn spawn<F>(&self, future: F) -> SpawnHandle<<F as std::future::Future>::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let inner = match self {
            Spawner::Runtime(rt) => rt.spawn(future),
            Spawner::Handle(h) => h.spawn(future),
        };
        SpawnHandle { inner }
    }

    /// Wrapper for [`Runtime::spawn_blocking`] or [`Handle::spawn_blocking`].
    pub fn spawn_blocking<F, R>(&self, func: F) -> SpawnHandle<R>
    where
        F: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let inner = match self {
            Spawner::Runtime(rt) => rt.spawn_blocking(func),
            Spawner::Handle(h) => h.spawn_blocking(func),
        };
        SpawnHandle { inner }
    }

    /// Get the current spawner.
    pub fn current() -> Self {
        Spawner::Handle(Handle::current())
    }

    /// Deterministically shut down the dedicated runtime, if this is a [`Spawner::Runtime`].
    ///
    /// This breaks the self-cycle where tasks spawned on the dedicated runtime keep a clone of the
    /// spawner (and thus the runtime) alive, preventing the runtime from ever being dropped. After
    /// this call the runtime is torn down in the background; all spawned tasks are dropped,
    /// releasing any spawner clones they captured.
    ///
    /// For [`Spawner::Handle`], there is no owned runtime to shut down (the handle references an
    /// externally-owned runtime whose lifetime the caller controls), so this is a no-op.
    ///
    /// This is safe to call from within the runtime itself or a parent runtime, and is idempotent.
    pub fn shutdown(&self) {
        match self {
            Spawner::Runtime(rt) => rt.shutdown(),
            Spawner::Handle(_) => {}
        }
    }
}
