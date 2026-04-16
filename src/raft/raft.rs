use futures::{channel::mpsc, stream::FuturesUnordered, StreamExt};
use madsim::{
    fs, net,
    rand::{self, Rng},
    task,
    time::*,
};
use serde::{Deserialize, Serialize};
use std::{
    fmt, io,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct RaftHandle {
    peers: Vec<SocketAddr>,
    inner: Arc<Mutex<Raft>>,
    me: usize,
    num_peers: usize,
    num_half_vote: usize,
}

#[derive(Debug)]
enum VoteResult {
    Granted,
    Denied,
    HigherTerm(u64),
}

type MsgSender = mpsc::UnboundedSender<ApplyMsg>;
pub type MsgRecver = mpsc::UnboundedReceiver<ApplyMsg>;

/// As each Raft peer becomes aware that successive log entries are committed,
/// the peer should send an `ApplyMsg` to the service (or tester) on the same
/// server, via the `apply_ch` passed to `Raft::new`.
pub enum ApplyMsg {
    Command {
        data: Vec<u8>,
        index: u64,
    },
    // For 2D:
    Snapshot {
        data: Vec<u8>,
        term: u64,
        index: u64,
    },
}

#[derive(Debug)]
pub struct Start {
    /// The index that the command will appear at if it's ever committed.
    pub index: u64,
    /// The current term.
    pub term: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("this node is not a leader, next leader: {0}")]
    NotLeader(usize),
    #[error("IO error")]
    IO(#[from] io::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

struct Raft {
    peers: Vec<SocketAddr>,
    me: usize,
    apply_ch: MsgSender,

    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    role: Role,
    current_term: u64,
    received_valid_rpc: bool,
    voted_for: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

impl Default for Role {
    fn default() -> Self {
        Role::Follower
    }
}

/// Data needs to be persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Persist {
    // Your data here.
}

impl fmt::Debug for Raft {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raft({})", self.me)
    }
}

// HINT: put async functions here
impl RaftHandle {
    pub async fn new(peers: Vec<SocketAddr>, me: usize) -> (Self, MsgRecver) {
        let (apply_ch, recver) = mpsc::unbounded();
        let num_peers = peers.len();
        let inner = Arc::new(Mutex::new(Raft {
            peers: peers.clone(),
            me,
            apply_ch,
            role: Role::Follower,
            current_term: 0,
            received_valid_rpc: false,
            voted_for: None,
        }));
        let handle = RaftHandle {peers, inner, me, num_peers, num_half_vote: (num_peers + 1) / 2 };
        // initialize from state persisted before a crash
        handle.restore().await.expect("failed to restore");
        handle.start_rpc_server();

        let handle_clone = handle.clone();
        task::spawn( async move {
            background(handle_clone).await;
        }).detach();


        (handle, recver)
    }

    /// Start agreement on the next command to be appended to Raft's log.
    ///
    /// If this server isn't the leader, returns [`Error::NotLeader`].
    /// Otherwise start the agreement and return immediately.
    ///
    /// There is no guarantee that this command will ever be committed to the
    /// Raft log, since the leader may fail or lose an election.
    pub async fn start(&self, cmd: &[u8]) -> Result<Start> {
        let mut raft = self.inner.lock().unwrap();
        info!("{:?} start", *raft);
        raft.start(cmd)
    }

    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.inner.lock().unwrap().current_term
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.inner.lock().unwrap().role == Role::Leader
    }

    /// A service wants to switch to snapshot.  
    ///
    /// Only do so if Raft hasn't have more recent info since it communicate
    /// the snapshot on `apply_ch`.
    pub async fn cond_install_snapshot(
        &self,
        last_included_term: u64,
        last_included_index: u64,
        snapshot: &[u8],
    ) -> bool {
        todo!()
    }

    /// The service says it has created a snapshot that has all info up to and
    /// including index. This means the service no longer needs the log through
    /// (and including) that index. Raft should now trim its log as much as
    /// possible.
    pub async fn snapshot(&self, index: u64, snapshot: &[u8]) -> Result<()> {
        todo!()
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    async fn persist(&self) -> io::Result<()> {
        let persist: Persist = todo!("persist state");
        let snapshot: Vec<u8> = todo!("persist snapshot");
        let state = bincode::serialize(&persist).unwrap();

        // you need to store persistent state in file "state"
        // and store snapshot in file "snapshot".
        // DO NOT change the file names.
        let file = fs::File::create("state").await?;
        file.write_all_at(&state, 0).await?;
        // make sure data is flushed to the disk,
        // otherwise data will be lost on power fail.
        file.sync_all().await?;

        let file = fs::File::create("snapshot").await?;
        file.write_all_at(&snapshot, 0).await?;
        file.sync_all().await?;
        Ok(())
    }

    /// Restore previously persisted state.
    async fn restore(&self) -> io::Result<()> {
        match fs::read("snapshot").await {
            Ok(snapshot) => {
                todo!("restore snapshot");
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        match fs::read("state").await {
            Ok(state) => {
                let persist: Persist = bincode::deserialize(&state).unwrap();
                todo!("restore state");
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        Ok(())
    }

    fn start_rpc_server(&self) {
        let net = net::NetLocalHandle::current();

        let this = self.clone();
        net.add_rpc_handler(move |args: RequestVoteArgs| {
            let this = this.clone();
            async move { this.request_vote(args).await.unwrap() }
        });
        // add more RPC handlers here
        let this = self.clone();
        net.add_rpc_handler(move |args: AppendEntriesArgs| {
            let this = this.clone();
            async move { this.append_entries(args).await.unwrap() }
        })
    }

    async fn request_vote(&self, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.request_vote(args)
        };
        // if you need to persist or call async functions here,
        // make sure the lock is scoped and dropped.
        // self.persist().await.expect("failed to persist");
        Ok(reply)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.append_entries(args)
        };
        Ok(reply)
    }
}

async fn background(rf_handle: RaftHandle) {
    loop {
        let role= {
            rf_handle.inner.lock().unwrap().role
        };

        match role {
            Role::Follower => {follower_task(rf_handle.clone(), Role::Follower).await},
            Role::Candidate => {follower_task(rf_handle.clone(), Role::Candidate).await},
            Role::Leader => {leader_task(rf_handle.clone()).await},
        }
    }
}

async fn leader_task(rf_handle: RaftHandle) {
    sleep(Duration::from_millis(150)).await;
    let mut rf = rf_handle.inner.lock().unwrap();
    if rf.received_valid_rpc {
        rf.received_valid_rpc = false;
        rf.role = Role::Follower;
        return;
    }
    if rf.role != Role::Leader {
        return;
    }
    send_heartbeat(rf_handle.clone(), rf.current_term);
}

async fn follower_task(rf_handle: RaftHandle, initial_role: Role) {
    sleep(Duration::from_millis(rand::rng().gen_range(450..900))).await;
    let mut rx = {
        let mut rf = rf_handle.inner.lock().unwrap();
        if rf.received_valid_rpc {
            rf.received_valid_rpc = false;
            rf.role = Role::Follower;
            return;
        }
        if rf.role != initial_role {
            return;
        }
        rf.role = Role::Candidate;
        rf.current_term += 1;
        rf.voted_for = Some(rf.me);
        let term = rf.current_term;
        info!("[{term}] peer {0} begin election", rf_handle.me);
        begin_election(rf_handle.clone(), term, 0, 0)
    };

    let mut num_granted = 1;
    let mut num_voted = 1;
    while let Some(result) = rx.next().await {
        let mut rf = rf_handle.inner.lock().unwrap();
        match result {
            VoteResult::HigherTerm(term) => {
                num_granted = 0;
                num_voted = rf_handle.num_peers;
                rf.role = Role::Follower;
                if term > rf.current_term {
                    rf.current_term = term;
                    rf.voted_for = None;
                }
            }
            VoteResult::Granted => {
                num_granted += 1;
                num_voted += 1;
            }
            VoteResult::Denied => {
                num_voted += 1;
            }
        }
        if rf.received_valid_rpc {
            rf.received_valid_rpc = false;
            rf.role = Role::Follower;
            return;
        }
        if rf.role != Role::Candidate {
            return;
        }
        if num_granted >= rf_handle.num_half_vote {
            info!("[{}] peer {} becomes leader", rf.current_term, rf.me);
            rf.role = Role::Leader;
            send_heartbeat(rf_handle.clone(), rf.current_term);
            return;
        }
        if num_voted >= rf_handle.num_peers {
            return
        }
    }
}

fn begin_election(rf_handle: RaftHandle, term: u64, last_log_index: u64, last_log_term: u64)
    -> mpsc::Receiver<VoteResult> {
    let (tx, rx) = mpsc::channel(rf_handle.num_peers);

    let args: RequestVoteArgs = RequestVoteArgs {
        term,
        candidate_id: rf_handle.me,
        last_log_index,
        last_log_term,
    };
    let timeout = Raft::generate_election_timeout();
    let net = net::NetLocalHandle::current();

    let mut rpcs = FuturesUnordered::new();
    for (i, &peer) in rf_handle.peers.iter().enumerate() {
        if i == rf_handle.me {
            continue;
        }
        // NOTE: `call` function takes ownerships
        let net = net.clone();
        let args = args.clone();
        rpcs.push(async move {
            net.call_timeout::<RequestVoteArgs, RequestVoteReply>(peer, args, timeout)
                .await
        });
    }

    // spawn a concurrent task
    let mut tx = tx.clone();
    task::spawn(async move {
        // handle RPC tasks in completion order
        while let Some(res) = rpcs.next().await {
            let rf = rf_handle.inner.lock().unwrap();
            match res {
                Ok(reply) => {
                    if reply.term > rf.current_term {
                        let _ = tx.try_send(VoteResult::HigherTerm(reply.term));
                    } else if term == rf.current_term && reply.vote_granted {
                        let _ = tx.try_send(VoteResult::Granted);
                    } else {
                        let _ = tx.try_send(VoteResult::Denied);
                    }
                }
                Err(e) => {
                    if term == rf.current_term {
                        let _ = tx.try_send(VoteResult::Denied);
                    }
                }
            }
        }
    }).detach(); // NOTE: you need to detach a task explicitly, or it will be cancelled on drop

    rx
}

fn send_heartbeat(rf_handle: RaftHandle, term: u64) {
    let args: AppendEntriesArgs = AppendEntriesArgs {
        term,
        leader_id: rf_handle.me,
    };
    let timeout = Raft::generate_election_timeout();
    let net = net::NetLocalHandle::current();
    let mut rpcs = FuturesUnordered::new();
    for (i, &peer) in rf_handle.peers.iter().enumerate() {
        if i == rf_handle.me {
            continue;
        }
        let net = net.clone();
        let args = args.clone();
        rpcs.push(async move {
            net.call_timeout::<AppendEntriesArgs, AppendEntriesReply>(peer, args, timeout).await
        });
    }

    task::spawn(async move {
        while let Some(res) = rpcs.next().await {
            let mut rf = rf_handle.inner.lock().unwrap();
            match res {
                Ok(reply) => {
                    if reply.term > rf.current_term {
                        rf.current_term = reply.term;
                        rf.role = Role::Follower;
                        rf.voted_for = None;
                    }
                }
                Err(e) => {}
            }
        }
    }).detach();
}


// HINT: put mutable non-async functions here
impl Raft {
    fn start(&mut self, data: &[u8]) -> Result<Start> {
        let leader = (self.me + 1) % self.peers.len();
        Err(Error::NotLeader(leader))
    }

    // Here is an example to apply committed message.
    fn apply(&self) {
        let msg = ApplyMsg::Command {
            data: todo!("apply msg"),
            index: todo!("apply msg"),
        };
        self.apply_ch.unbounded_send(msg).unwrap();
    }

    fn request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        if args.term > self.current_term {
            self.current_term = args.term;
            self.role = Role::Follower;
            self.voted_for = None;
        }
        if args.term < self.current_term {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            }
        }
        let mut vote_granted = false;
        if self.voted_for.is_none() || self.voted_for == Some(args.candidate_id) {
            self.voted_for = Some(args.candidate_id);
            vote_granted = true;
        }
        if vote_granted {
            self.received_valid_rpc = true;
        }
        RequestVoteReply {
            term: self.current_term,
            vote_granted
        }
    }

    fn append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        if args.term > self.current_term {
            self.current_term = args.term;
            self.role = Role::Follower;
            self.voted_for = None;
        }
        if args.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term, success: false
            }
        }
        self.received_valid_rpc = true;
        AppendEntriesReply {
            term: self.current_term, success: true
        }
    }

    // Here is an example to generate random number.
    fn generate_election_timeout() -> Duration {
        // see rand crate for more details
        Duration::from_millis(rand::rng().gen_range(150..300))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestVoteArgs {
    // Your data here.
    term: u64,
    candidate_id: usize,
    last_log_index: u64,
    last_log_term: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RequestVoteReply {
    // Your data here.
    term: u64,
    vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppendEntriesArgs {
    // Your data here.
    term: u64,
    leader_id: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppendEntriesReply {
    // Your data here.
    term: u64,
    success: bool,
}
