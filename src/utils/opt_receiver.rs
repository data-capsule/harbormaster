use std::future::pending;
use tokio::sync::mpsc::UnboundedReceiver;
use super::channel::Receiver;

/// A wrapper around `Option<Receiver<T>>` that hangs indefinitely if the option is `None`,
/// and calls `recv()` on the inner receiver if `Some`.
///
/// Useful in `tokio::select!` branches where a receiver may or may not be present.
pub struct OptReceiver<T>(Option<Receiver<T>>);

impl<T> OptReceiver<T> {
    pub fn new(rx: Option<Receiver<T>>) -> Self {
        Self(rx)
    }

    pub fn some(rx: Receiver<T>) -> Self {
        Self(Some(rx))
    }

    pub fn none() -> Self {
        Self(None)
    }

    /// If the inner receiver is `Some`, calls `recv()` on it.
    /// If `None`, returns a future that is permanently pending (never resolves).
    pub async fn recv(&self) -> Option<T> {
        match &self.0 {
            Some(rx) => rx.recv().await,
            None => pending().await,
        }
    }

    pub fn is_some(&self) -> bool {
        self.0.is_some()
    }

    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }
}

/// A wrapper around `Option<UnboundedReceiver<T>>` that hangs indefinitely if the option is `None`,
/// and calls `recv()` on the inner receiver if `Some`.
///
/// Useful in `tokio::select!` branches where an unbounded receiver may or may not be present.
pub struct OptUnboundedReceiver<T>(Option<UnboundedReceiver<T>>);

impl<T> OptUnboundedReceiver<T> {
    pub fn new(rx: Option<UnboundedReceiver<T>>) -> Self {
        Self(rx)
    }

    pub fn some(rx: UnboundedReceiver<T>) -> Self {
        Self(Some(rx))
    }

    pub fn none() -> Self {
        Self(None)
    }

    /// If the inner receiver is `Some`, calls `recv()` on it.
    /// If `None`, returns a future that is permanently pending (never resolves).
    pub async fn recv(&mut self) -> Option<T> {
        match &mut self.0 {
            Some(rx) => rx.recv().await,
            None => pending().await,
        }
    }

    pub fn is_some(&self) -> bool {
        self.0.is_some()
    }

    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }
}
