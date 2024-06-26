use std::time::Duration;

use tokio::{
    sync::mpsc::{channel, Sender},
    time::sleep,
};

use super::*;

#[tokio::test]
async fn succeed_one() {
    let mut s = TwoJoinSet::new();
    let count = s.spawn(succeed()).count().await;
    assert_eq!(count, 0);
    let result = s.join_next().await.unwrap().unwrap();
    assert!(result);
    assert!(s.is_empty());
}

#[tokio::test]
async fn fail_one() {
    let mut s = TwoJoinSet::new();
    let count = s.spawn(fail()).count().await;
    assert_eq!(count, 0);
    s.join_next().await.unwrap().unwrap_err();
    assert!(s.is_empty());
}

#[tokio::test]
async fn spawn_10() {
    let mut s = TwoJoinSet::new();
    assert_eq!(s.len(), 0);
    assert!(s.is_empty());

    _ = s.spawn(succeed());
    assert_eq!(s.len(), 1);
    assert!(!s.is_empty());

    for _ in 0..9 {
        _ = s.spawn(succeed());
        assert_eq!(s.len(), 2);
        assert!(!s.is_empty());
    }
}

#[tokio::test]
async fn first_and_second() {
    let mut s = TwoJoinSet::new();
    assert!(s.first().is_none());
    assert!(s.second().is_none());

    _ = s.spawn(succeed());
    assert!(s.first().is_some());
    assert!(s.second().is_none());

    _ = s.spawn(succeed());
    assert!(s.first().is_some());
    assert!(s.second().is_some());
}

#[tokio::test]
async fn shutdown() {
    let mut s = TwoJoinSet::new();
    _ = s.spawn(succeed());
    _ = s.spawn(succeed());
    s.shutdown().await;
    assert!(s.is_empty());
}

#[tokio::test]
async fn abort_all() {
    let (tx, mut rx) = channel(8);
    let mut s = TwoJoinSet::new();
    _ = s.spawn(send_after_wait(tx.clone()));
    _ = s.spawn(send_after_wait(tx));
    s.abort_all();
    assert!(!s.is_empty());
    let is_none = rx.recv().await.is_none();
    assert!(is_none);
}

#[tokio::test]
async fn second_aborted() {
    let (tx, mut rx) = channel(8);
    let mut s = TwoJoinSet::new();
    let count = s.spawn(send_n_after_wait(tx.clone(), 0)).count().await;
    assert_eq!(count, 0);
    assert_eq!(s.len(), 1);
    for _ in 0..99 {
        let count = s.spawn(send_n_after_wait(tx.clone(), 1)).count().await;
        assert_eq!(count, 0);
        assert_eq!(s.len(), 2);
    }
    let count = s.spawn(send_n_after_wait(tx, 69)).count().await;
    assert_eq!(count, 0);
    assert_eq!(s.len(), 2);
    let first_msg = rx.recv().await.unwrap();
    assert_eq!(first_msg, 0);
    let last_msg = rx.recv().await.unwrap();
    assert_eq!(last_msg, 69);
    let is_none = rx.recv().await.is_none();
    assert!(is_none);
}

#[tokio::test]
async fn detach_all() {
    let (tx, mut rx) = channel(8);
    let mut s = TwoJoinSet::new();
    _ = s.spawn(send_after_wait(tx.clone()));
    _ = s.spawn(send_after_wait(tx));
    s.detach_all();
    assert!(s.is_empty());
    rx.recv().await.unwrap();
    rx.recv().await.unwrap();
}

#[tokio::test]
async fn try_join_both() {
    let (tx, mut rx) = channel(8);
    let mut s = TwoJoinSet::new();
    _ = s.spawn(send_after_wait(tx.clone()));
    _ = s.spawn(send_after_wait(tx));
    assert!(!s.is_empty());
    rx.recv().await.unwrap();
    rx.recv().await.unwrap();
    let count = s.try_join_both().count().await;
    assert_eq!(count, 2);
}

#[tokio::test]
async fn spawn_on_handle() {
    let mut s = TwoJoinSet::new();
    let handle = Handle::current();
    let count = s.spawn_on(succeed(), &handle).count().await;
    assert_eq!(count, 0);
    let result = s.join_next().await.unwrap().unwrap();
    assert!(result);
    assert!(s.is_empty());
}

#[tokio::test]
async fn spawn_blocking() {
    let mut s = TwoJoinSet::new();
    let count = s.spawn_blocking(|| true).count().await;
    assert_eq!(count, 0);
    let result = s.join_next().await.unwrap().unwrap();
    assert!(result);
    assert!(s.is_empty());
}

#[tokio::test]
async fn spawn_blocking_on_handle() {
    let mut s = TwoJoinSet::new();
    let handle = Handle::current();
    let count = s.spawn_blocking_on(|| true, &handle).count().await;
    assert_eq!(count, 0);
    let result = s.join_next().await.unwrap().unwrap();
    assert!(result);
    assert!(s.is_empty());
}

#[tokio::test]
async fn spawn_local() {
    let mut s = TwoJoinSet::new();
    let local = LocalSet::new();
    let result = local
        .run_until(async {
            let count = s.spawn_local(succeed()).count().await;
            assert_eq!(count, 0);
            s.join_next().await.unwrap().unwrap()
        })
        .await;
    assert!(result);
    assert!(s.is_empty());
}

#[tokio::test]
async fn spawn_local_on() {
    let mut s = TwoJoinSet::new();
    let local = LocalSet::new();
    let result = local
        .run_until(async {
            let count = s.spawn_local_on(succeed(), &local).count().await;
            assert_eq!(count, 0);
            s.join_next().await.unwrap().unwrap()
        })
        .await;
    assert!(result);
    assert!(s.is_empty());
}

async fn succeed() -> bool {
    true
}

async fn fail() -> bool {
    panic!("kaboom")
}

async fn send_after_wait(tx: Sender<()>) {
    sleep(Duration::from_millis(1)).await;
    _ = tx.send(()).await;
}

async fn send_n_after_wait(tx: Sender<usize>, n: usize) {
    sleep(Duration::from_millis(1)).await;
    _ = tx.send(n).await;
}
