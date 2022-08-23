use futures::future::join_all;
use std::collections::VecDeque;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::{collections::HashMap, sync::atomic::Ordering};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::{
    sync::mpsc::{channel, unbounded_channel, Receiver, Sender},
    time::{sleep, sleep_until, Duration, Instant},
};

/// RateLimiter provides a rate limiting service that takes in a queue of thread ids, and responds
/// to each in order, delivering a chunk of timestamps, which the requesting thread may use the
/// self rate limit. Each timestamp is a token by which a process may continue operation after
/// which the itme has elapsed.
pub struct RateLimiter {
    /// Last assigned timestamp.
    last_timestamp: Instant,
    /// Represents the duration between each allowed thread operation, or rather the inverted rate.
    rate_inverted: Duration,
    /// A map of registered threads mapped back to sending channels to send.
    thread_channels: HashMap<u32, Sender<VecDeque<Instant>>>,
    /// Incoming queue of thread ids, for threads requesting a new chunk of timestamps.
    queue: UnboundedReceiver<u32>,
    /// The configured amount of timestamps to return as a chunk to a requesting thread.
    chunk_size: u32,
}

impl RateLimiter {
    /// Constructs a new RateLimiter returning both the RateLimiter, and a sending channel which
    /// may be used to send requests for new timestamp chunks. The channel takes the registered
    /// thread id that is requesting a new chunk.
    pub fn new(rate_inverted: Duration, chunk_size: u32) -> (RateLimiter, UnboundedSender<u32>) {
        let (tx, rx) = unbounded_channel::<u32>();
        (
            RateLimiter {
                last_timestamp: Instant::now(),
                rate_inverted,
                thread_channels: HashMap::new(),
                queue: rx,
                chunk_size,
            },
            tx,
        )
    }

    /// Registers a new thread by it's id, returning a receiving channel the thread may use to
    /// receive timestamp chunks used for self rate limiting in the thread.
    pub fn register_channel(&mut self, thread_id: u32) -> Receiver<VecDeque<Instant>> {
        let (tx, rx) = channel::<VecDeque<Instant>>(1);
        self.thread_channels.insert(thread_id, tx);
        rx
    }

    pub async fn deliver_next_timestamps(&mut self) {
        let conn_id = self.queue.recv().await.unwrap();

        // Determine next timestamp.
        let maybe_next = self.last_timestamp + self.rate_inverted;
        let now = Instant::now();
        let next = if maybe_next > now { maybe_next } else { now };

        let chunk: VecDeque<Instant> = (1..=self.chunk_size)
            .map(|i| next + (i * self.rate_inverted))
            .collect();

        // Send timestamp to requesting conn.
        self.thread_channels
            .get_mut(&conn_id)
            .expect(
                "Requesting thread hasn't been registered. This should be impossible by design.",
            )
            .try_send(chunk)
            .unwrap();

        // Update last timestamp assigned.
        self.last_timestamp = next + (self.chunk_size * self.rate_inverted);
    }
}

/**
 * TESTING BELOW
*/

pub async fn rate_limiter_work_loop(mut rate_limiter: RateLimiter) {
    loop {
        rate_limiter.deliver_next_timestamps().await;
    }
}

pub struct Connection {
    pub id: u32,
    pub timestamp_requester: UnboundedSender<u32>,
    pub timestamp_receiver: Receiver<VecDeque<Instant>>,
    pub iterations: Arc<AtomicU64>,
    pub tokens: VecDeque<Instant>,
}

impl Connection {
    async fn refresh_tokens(&mut self) {
        if !self.tokens.is_empty() {
            return;
        }

        if let Err(e) = self.timestamp_requester.send(self.id) {
            eprintln!("error trying to send: {:?}", e);
        }

        self.tokens = self.timestamp_receiver.recv().await.unwrap();
    }

    pub async fn wait_on_token(&mut self) {
        if self.tokens.is_empty() {
            self.refresh_tokens().await;
        }

        let next = self.tokens.pop_front().unwrap();
        if next < Instant::now() {
            return;
        }
        sleep_until(next).await;
    }
}

pub async fn connection_work_loop(mut connection: Connection, use_rate_limiter: bool) {
    // Simulate huge per connection QPS
    let qps = 500_000;
    let duration = Duration::new(1, 0) / qps;
    let mut next_work_time = Instant::now();

    sleep(Duration::new(0, 10 * connection.id)).await;
    loop {
        if use_rate_limiter {
            connection.wait_on_token().await;
        } else {
            // sleep to simulate standard absurdly high QPS.
            sleep_until(next_work_time).await;
            next_work_time = next_work_time + duration;
        }

        connection.iterations.fetch_add(1, Ordering::SeqCst);
    }
}

pub async fn run_all_conns(connections: Vec<Connection>, use_rate_limiter: bool) {
    let futs: Vec<_> = connections
        .into_iter()
        .map(|c| connection_work_loop(c, use_rate_limiter))
        .collect();

    join_all(futs).await;
}

#[cfg(test)]
mod tests {
    use tokio::task;
    use tokio::time::timeout;

    use super::*;

    #[tokio::test]
    async fn throttle_works() {
        let iteration_counter = Arc::new(AtomicU64::new(0));
        let secs_to_run_test = 10;
        let chunk_size = 100;

        let qps = 50_000;
        let duration = Duration::new(1, 0) / qps;
        let (mut rate_limiter, tx) = RateLimiter::new(duration, chunk_size);
        let mut connections = vec![];
        for id in 0..1_000 {
            let rx = rate_limiter.register_channel(id);
            let tx = tx.clone();
            let conn = Connection {
                id,
                timestamp_receiver: rx,
                timestamp_requester: tx,
                iterations: iteration_counter.clone(),
                tokens: VecDeque::new(),
            };

            connections.push(conn);
        }

        // Run for 10 seconds.
        let _ = timeout(Duration::new(secs_to_run_test, 0), async move {
            tokio::join!(
                rate_limiter_work_loop(rate_limiter),
                run_all_conns(connections, true)
            )
        })
        .await;

        // Report qps.
        let measured_qps = iteration_counter.load(Ordering::SeqCst) / secs_to_run_test;
        println!("Total QPS: {}", measured_qps);

        assert!(measured_qps as u32 <= qps);
    }

    #[tokio::test]
    async fn throttle_is_performant() {
        let iteration_counter = Arc::new(AtomicU64::new(0));
        let secs_to_run_test = 20;
        let chunk_size = 100;

        let qps = 10_000_000;
        let duration = Duration::new(1, 0) / qps;
        let (mut rate_limiter, tx) = RateLimiter::new(duration, chunk_size);
        let mut connections = vec![];
        for id in 0..1_000 {
            let rx = rate_limiter.register_channel(id);
            let tx = tx.clone();
            let conn = Connection {
                id,
                timestamp_receiver: rx,
                timestamp_requester: tx,
                iterations: iteration_counter.clone(),
                tokens: VecDeque::new(),
            };

            connections.push(conn);
        }

        let _ = timeout(Duration::new(secs_to_run_test, 0), async move {
            tokio::join!(
                task::spawn(rate_limiter_work_loop(rate_limiter)),
                task::spawn(run_all_conns(connections, true))
            )
        })
        .await;

        // Report qps.
        let measured_qps = iteration_counter.load(Ordering::SeqCst) / secs_to_run_test;
        println!("Total QPS: {}", measured_qps);
    }
}
