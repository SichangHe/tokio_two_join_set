//! Provides the [`TwoJoinSet`] useful for tasks that are
//! constantly rerun to get the latest result but also need to
//! complete from time to time to provide intermediate results.
//! Please see the documentation for [`TwoJoinSet`] for more details.

use std::{future::Future, mem, pin::Pin};

use futures::{stream, Stream, StreamExt};
use tokio::{
    runtime::Handle,
    select, spawn,
    task::{spawn_blocking, spawn_local, JoinError, JoinHandle, LocalSet},
};

#[allow(unused_imports)] // For docstring.
use tokio::task::JoinSet;

/// A collection of at most two tasks spawned on a Tokio runtime;
/// cancels the second running task when a third task is spawned.
///
/// If you need to rerun a task again and again because your input changes and
/// you want to get the latest result,
/// but you also want to get at least some intermediate results,
/// a two-JoinSet is for you.
/// The oldest running task is guaranteed to be kept alive,
/// while the newer running tasks are cancelled when new tasks are spawned.
/// That is, under high load,
/// you at least get one stale task guaranteed to be processed to completion,
/// and always get a fresh task being processed.
///
/// APIs of this two-JoinSet mimics that of [`JoinSet`].
/// All of the tasks must have the same return type `T`.
/// When the two-JoinSet is dropped,
/// all tasks in the two-JoinSet are immediately aborted.
#[derive(Debug, Default)]
pub struct TwoJoinSet<T> {
    first: Option<JoinHandle<T>>,
    second: Option<JoinHandle<T>>,
}

impl<T> TwoJoinSet<T> {
    /// Create a new, empty two-JoinSet.
    pub const fn new() -> Self {
        Self {
            first: None,
            second: None,
        }
    }

    /// Returns the number of tasks in the two-JoinSet,
    /// no matter running or not.
    pub fn len(&self) -> usize {
        match (&self.first, &self.second) {
            (Some(_), Some(_)) => 2,
            (Some(_), None) | (None, Some(_)) => 1,
            (None, None) => 0,
        }
    }

    /// If the two-JoinSet contains no tasks, running or not.
    pub fn is_empty(&self) -> bool {
        matches!((&self.first, &self.second), (None, None))
    }

    /// Reference to maybe the first task in the two-JoinSet.
    pub fn first(&self) -> Option<&JoinHandle<T>> {
        self.first.as_ref()
    }

    /// Reference to maybe the second task in the two-JoinSet.
    pub fn second(&self) -> Option<&JoinHandle<T>> {
        self.second.as_ref()
    }

    /// Waits until one of the tasks in the set completes and
    /// returns its output. Returns `None` if the set is empty.
    ///
    /// # Cancel Safety
    /// This method is cancel safe.
    /// If `join_next` is used as the event in
    /// a [`select`]! statement and some other branch completes first,
    /// it is guaranteed that no tasks were removed from this two-JoinSet.
    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        match (&mut self.first, &mut self.second) {
            (Some(first), Some(second)) => {
                let (first, second) = (Pin::new(first), Pin::new(second));
                let (result, first_finished) = select! {
                    biased;
                    r = first => (r, true),
                    r = second => (r, false),
                };
                match first_finished {
                    true => self.first = self.second.take(),
                    false => self.second = None,
                }
                Some(result)
            }
            (handle @ Some(_), None) | (None, handle @ Some(_)) => {
                Some(handle.take().unwrap().await)
            }
            (None, None) => None,
        }
    }

    /// Abort all tasks and wait for them to finish shutting down.
    ///
    /// Calling this method is equivalent to
    /// calling [`TwoJoinSet::abort_all`] and then
    /// calling [`TwoJoinSet::join_next`] in a loop until it returns `None`.
    ///
    /// This method ignores any panics in the tasks shutting down.
    /// When this call returns, the two-JoinSet will be empty.
    pub async fn shutdown(&mut self) {
        self.abort_all();
        for maybe in self.iter_raw_mut() {
            if let Some(handle) = maybe.take() {
                _ = handle.await;
            }
        }
    }

    /// Abort all tasks on this two-JoinSet.
    ///
    /// This does not remove the tasks from the two-JoinSet.
    /// To wait for the tasks to complete cancellation,
    /// use [`TwoJoinSet::shutdown`] instead.
    #[inline]
    pub fn abort_all(&mut self) {
        self.iter().for_each(|handle| handle.abort())
    }

    /// Remove all tasks from this two-JoinSet without aborting them.
    ///
    /// The tasks removed by this call will continue to run in
    /// the background even if the two-JoinSet is dropped.
    pub fn detach_all(&mut self) {
        self.iter_raw_mut().for_each(|maybe| _ = maybe.take())
    }

    /// Pick out all the finished tasks and return a stream of their results.
    pub fn try_join_both(&mut self) -> impl Stream<Item = Result<T, JoinError>> {
        // NOTE: This array avoids `finished_handles` from capturing `'self`.
        let mut maybe_handles = [None, None];
        for (index, maybe) in self.iter_raw_mut().enumerate() {
            match maybe {
                Some(handle) if handle.is_finished() => maybe_handles[index] = mem::take(maybe),
                _ => {}
            }
        }
        self.fix_order();
        let finished_handles = maybe_handles.into_iter().flatten();
        stream::iter(finished_handles).then(|handle| handle)
    }

    /// Fix the order of the two tasks if applicable.
    #[inline]
    fn fix_order(&mut self) {
        if let (None, Some(_)) = (&mut self.first, &mut self.second) {
            self.first = self.second.take();
        }
    }

    /// Push a spawned `join_handle` into the two-JoinSet,
    /// overwriting the newest handle if needed.
    #[inline]
    pub fn push_handle(&mut self, join_handle: JoinHandle<T>) {
        match self.first {
            Some(_) => {
                if let Some(handle) = self.second.replace(join_handle) {
                    handle.abort();
                }
            }
            None => self.first = Some(join_handle),
        }
    }

    /// Iterates over all the [JoinHandle]s in the [TwoJoinSet],
    /// from the older to the newer.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &JoinHandle<T>> {
        [&self.first, &self.second]
            .into_iter()
            .filter_map(|maybe| maybe.as_ref())
    }

    #[inline]
    fn iter_raw_mut(&mut self) -> impl DoubleEndedIterator<Item = &mut Option<JoinHandle<T>>> {
        [&mut self.first, &mut self.second].into_iter()
    }
}

impl<T> TwoJoinSet<T>
where
    T: Send + 'static,
{
    /// Spawn `task` and return the results of all previously finished tasks.
    ///
    /// If we contain two running tasks, we cancel and ignore the newer task
    /// (if we await, its result would be a "cancelled" `JoinError` anyway).
    ///
    /// The provided future will start running in
    /// the background immediately when this method is called,
    /// even if you don’t await anything on this `TwoJoinSet`.
    ///
    /// # Panics
    /// This method panics if called outside of a Tokio runtime.
    pub fn spawn<F>(&mut self, task: F) -> impl Stream<Item = Result<T, JoinError>>
    where
        F: Future<Output = T> + Send + 'static,
    {
        let finished_results = self.try_join_both();
        self.push_handle(spawn(task));
        finished_results
    }

    /// Same as [`TwoJoinSet::spawn`], but spawns on runtime `handle`.
    pub fn spawn_on<F>(
        &mut self,
        task: F,
        handle: &Handle,
    ) -> impl Stream<Item = Result<T, JoinError>>
    where
        F: Future<Output = T> + Send + 'static,
    {
        let finished_results = self.try_join_both();
        self.push_handle(handle.spawn(task));
        finished_results
    }

    /// Same as [`TwoJoinSet::spawn`],
    /// but spawn on the blocking code `f` on the blocking thread pool.
    ///
    /// # Panics
    /// This method panics if called outside of a Tokio runtime.
    pub fn spawn_blocking<F>(&mut self, f: F) -> impl Stream<Item = Result<T, JoinError>>
    where
        F: FnOnce() -> T + Send + 'static,
    {
        let finished_results = self.try_join_both();
        self.push_handle(spawn_blocking(f));
        finished_results
    }

    /// Same as [`TwoJoinSet::spawn_blocking`], but spawn on runtime `handle`.
    ///
    /// # Panics
    /// This method panics if called outside of a Tokio runtime.
    pub fn spawn_blocking_on<F>(
        &mut self,
        f: F,
        handle: &Handle,
    ) -> impl Stream<Item = Result<T, JoinError>>
    where
        F: FnOnce() -> T + Send + 'static,
    {
        let finished_results = self.try_join_both();
        self.push_handle(handle.spawn_blocking(f));
        finished_results
    }
}

impl<T> TwoJoinSet<T>
where
    T: 'static,
{
    /// Same as [`TwoJoinSet::spawn`], but spawn on the current [LocalSet].
    ///
    /// # Panics
    /// This method panics if called outside of a [LocalSet].
    pub fn spawn_local<F>(&mut self, task: F) -> impl Stream<Item = Result<T, JoinError>>
    where
        F: Future<Output = T> + 'static,
    {
        let finished_results = self.try_join_both();
        self.push_handle(spawn_local(task));
        finished_results
    }

    /// Same as [`TwoJoinSet::spawn_local`], but spawn on `local_set`.
    ///
    /// Unlike [`TwoJoinSet::spawn_local`],
    /// this method may be used to spawn local tasks on a [LocalSet] that
    /// is not currently running.
    /// The provided future will start running whenever
    /// the LocalSet is next started.
    pub fn spawn_local_on<F>(
        &mut self,
        task: F,
        local_set: &LocalSet,
    ) -> impl Stream<Item = Result<T, JoinError>>
    where
        F: Future<Output = T> + 'static,
    {
        let finished_results = self.try_join_both();
        self.push_handle(local_set.spawn_local(task));
        finished_results
    }
}

impl<T> Drop for TwoJoinSet<T> {
    fn drop(&mut self) {
        self.abort_all();
    }
}

#[cfg(test)]
mod tests;
