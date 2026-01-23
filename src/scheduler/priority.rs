//! Priority Queue - Order executions by priority

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use uuid::Uuid;

/// An item in the priority queue
#[derive(Debug, Clone)]
pub struct PriorityItem<T> {
    /// The item data
    pub data: T,
    /// Priority (higher = processed first)
    pub priority: i32,
    /// Sequence number for FIFO ordering within same priority
    pub sequence: u64,
    /// When the item was enqueued
    pub enqueued_at: Instant,
}

impl<T> PriorityItem<T> {
    pub fn new(data: T, priority: i32, sequence: u64) -> Self {
        Self {
            data,
            priority,
            sequence,
            enqueued_at: Instant::now(),
        }
    }
}

impl<T> PartialEq for PriorityItem<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl<T> Eq for PriorityItem<T> {}

impl<T> PartialOrd for PriorityItem<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for PriorityItem<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Higher priority first
        match self.priority.cmp(&other.priority) {
            Ordering::Equal => {
                // Within same priority, lower sequence (older) first (FIFO)
                other.sequence.cmp(&self.sequence)
            }
            other => other,
        }
    }
}

/// Priority queue with fair scheduling
pub struct PriorityQueue<T> {
    heap: Mutex<BinaryHeap<PriorityItem<T>>>,
    sequence: std::sync::atomic::AtomicU64,
    max_size: Option<usize>,
}

impl<T: Clone> PriorityQueue<T> {
    /// Create a new priority queue
    pub fn new() -> Self {
        Self {
            heap: Mutex::new(BinaryHeap::new()),
            sequence: std::sync::atomic::AtomicU64::new(0),
            max_size: None,
        }
    }

    /// Create with maximum size
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            heap: Mutex::new(BinaryHeap::with_capacity(max_size)),
            sequence: std::sync::atomic::AtomicU64::new(0),
            max_size: Some(max_size),
        }
    }

    /// Push an item with given priority
    pub async fn push(&self, data: T, priority: i32) -> Result<(), QueueFullError> {
        let mut heap = self.heap.lock().await;

        if let Some(max) = self.max_size {
            if heap.len() >= max {
                return Err(QueueFullError { size: max });
            }
        }

        let sequence = self
            .sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        heap.push(PriorityItem::new(data, priority, sequence));
        Ok(())
    }

    /// Pop the highest priority item
    pub async fn pop(&self) -> Option<PriorityItem<T>> {
        let mut heap = self.heap.lock().await;
        heap.pop()
    }

    /// Peek at the highest priority item without removing
    pub async fn peek(&self) -> Option<PriorityItem<T>> {
        let heap = self.heap.lock().await;
        heap.peek().cloned()
    }

    /// Get current queue size
    pub async fn len(&self) -> usize {
        let heap = self.heap.lock().await;
        heap.len()
    }

    /// Check if queue is empty
    pub async fn is_empty(&self) -> bool {
        let heap = self.heap.lock().await;
        heap.is_empty()
    }

    /// Drain up to N items
    pub async fn drain(&self, n: usize) -> Vec<PriorityItem<T>> {
        let mut heap = self.heap.lock().await;
        let mut items = Vec::with_capacity(n.min(heap.len()));

        for _ in 0..n {
            if let Some(item) = heap.pop() {
                items.push(item);
            } else {
                break;
            }
        }

        items
    }

    /// Clear all items
    pub async fn clear(&self) {
        let mut heap = self.heap.lock().await;
        heap.clear();
    }
}

impl<T: Clone> Default for PriorityQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Error when queue is full
#[derive(Debug, thiserror::Error)]
#[error("Queue is full ({size} items)")]
pub struct QueueFullError {
    pub size: usize,
}

/// Priority levels for common use cases
pub mod levels {
    pub const CRITICAL: i32 = 100;
    pub const HIGH: i32 = 75;
    pub const NORMAL: i32 = 50;
    pub const LOW: i32 = 25;
    pub const BACKGROUND: i32 = 0;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_priority_ordering() {
        let queue = PriorityQueue::new();

        queue.push("low", levels::LOW).await.unwrap();
        queue.push("high", levels::HIGH).await.unwrap();
        queue.push("normal", levels::NORMAL).await.unwrap();

        assert_eq!(queue.pop().await.unwrap().data, "high");
        assert_eq!(queue.pop().await.unwrap().data, "normal");
        assert_eq!(queue.pop().await.unwrap().data, "low");
    }

    #[tokio::test]
    async fn test_fifo_within_priority() {
        let queue = PriorityQueue::new();

        queue.push("first", levels::NORMAL).await.unwrap();
        queue.push("second", levels::NORMAL).await.unwrap();
        queue.push("third", levels::NORMAL).await.unwrap();

        assert_eq!(queue.pop().await.unwrap().data, "first");
        assert_eq!(queue.pop().await.unwrap().data, "second");
        assert_eq!(queue.pop().await.unwrap().data, "third");
    }

    #[tokio::test]
    async fn test_max_size() {
        let queue = PriorityQueue::with_max_size(2);

        queue.push("a", 0).await.unwrap();
        queue.push("b", 0).await.unwrap();

        assert!(queue.push("c", 0).await.is_err());
    }
}
