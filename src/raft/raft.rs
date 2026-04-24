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
use std::fmt::{Debug, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MyEntry {
    pub x: u64,
}


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

    // persistent state on all servers
    current_term: u64,
    voted_for: Option<usize>,
    log: Vec<LogEntry>,

    // volatile state on all servers
    commit_index: usize,
    last_applied: usize,
    role: Role,
    received_valid_rpc: bool,

    // volatile state on leaders
    next_index: Vec<usize>,
    match_index: Vec<usize>,
}

#[derive(Clone, Serialize, Deserialize)]
struct LogEntry {
    term: u64,
    command: Vec<u8>,
}

impl Debug for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.term)
    }
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
    current_term: u64,
    voted_for: Option<usize>,
    log: Vec<LogEntry>,
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
        let mut log: Vec<LogEntry> = Vec::new();
        log.push(LogEntry {
            term: 0,
            command: Vec::new(),
        });
        let inner = Arc::new(Mutex::new(Raft {
            peers: peers.clone(),
            me,
            apply_ch,
            current_term: 0,
            voted_for: None,
            log,
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower,
            received_valid_rpc: false,
            next_index: Vec::new(),
            match_index: Vec::new(),
        }));
        let handle = RaftHandle {peers, inner, me, num_peers, num_half_vote: (num_peers + 1) / 2 };
        // initialize from state persisted before a crash
        handle.restore().await.expect("failed to restore");
        handle.start_rpc_server();

        let handle_clone = handle.clone();
        task::spawn( async move {
            background(handle_clone).await;
        }).detach();

        let handle_clone = handle.clone();
        task::spawn(async move {
            commit_entries(handle_clone).await;
        }).detach();

        {
            let rf = handle.inner.lock().unwrap();
            info!("[{}] peer {} new {:?} {:?}", rf.current_term, rf.me, rf.voted_for, rf.log);
        }

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
        let result = {
            let mut raft = self.inner.lock().unwrap();
            // info!("{:?} start", *raft);
            raft.start(cmd)
        };
        if result.is_ok() {
            self.persist().await.expect("failed to persist");
        }
        result
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
        let persist: Persist = {
            let rf = self.inner.lock().unwrap();
            Persist {
            current_term: rf.current_term,
            voted_for: rf.voted_for,
            log: rf.log.clone(),
        }};
        // let snapshot: Vec<u8> = todo!("persist snapshot");
        let state = bincode::serialize(&persist).unwrap();

        // you need to store persistent state in file "state"
        // and store snapshot in file "snapshot".
        // DO NOT change the file names.
        let file = fs::File::create("state").await?;
        file.write_all_at(&state, 0).await?;
        // make sure data is flushed to the disk,
        // otherwise data will be lost on power fail.
        file.sync_all().await?;

        // let file = fs::File::create("snapshot").await?;
        // file.write_all_at(&snapshot, 0).await?;
        // file.sync_all().await?;
        Ok(())
    }

    /// Restore previously persisted state.
    async fn restore(&self) -> io::Result<()> {
        // match fs::read("snapshot").await {
        //     Ok(snapshot) => {
        //         todo!("restore snapshot");
        //     }
        //     Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        //     Err(e) => return Err(e),
        // }
        match fs::read("state").await {
            Ok(state) => {
                let persist: Persist = bincode::deserialize(&state).unwrap();
                let mut rf = self.inner.lock().unwrap();
                rf.current_term = persist.current_term;
                rf.voted_for = persist.voted_for;
                rf.log = persist.log.clone();
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
        self.persist().await.expect("failed to persist");
        Ok(reply)
    }

    async fn append_entries(&self, args: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        let reply = {
            let mut this = self.inner.lock().unwrap();
            this.append_entries(args)
        };
        self.persist().await.expect("failed to persist");
        Ok(reply)
    }
}

async fn commit_entries(rf_handle: RaftHandle) {
    loop {
        sleep(Duration::from_millis(300)).await;
        let mut rf = rf_handle.inner.lock().unwrap();
        loop {
            if rf.commit_index > rf.last_applied {
                rf.last_applied += 1;
                rf.apply(rf.log[rf.last_applied].command.clone(), rf.last_applied as u64);
            } else {
                break;
            }
        }
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
    let mut new_commit_index = rf.commit_index;
    for n in rf.commit_index + 1..rf.log.len() {
        if rf.log[n].term != rf.current_term {
            continue; // term mismatch, but a later entry might still qualify
        }
        if rf.match_index.iter().filter(|&&m| m >= n).count() >= rf_handle.num_half_vote {
            new_commit_index = n; // keep going, there may be a higher valid N
        }
    }
    rf.commit_index = new_commit_index;
    for i in 0..rf_handle.num_peers {
        if i == rf_handle.me {
            continue;
        }
        let prev_log_index = rf.next_index[i] - 1;
        if rf.log.len() - 1 >= rf.next_index[i] {
            let entries_copy = rf.log[rf.next_index[i]..].to_vec();
            send_append_entries(rf_handle.clone(), i, rf.current_term, prev_log_index,
                                rf.log[prev_log_index].term, rf.commit_index, entries_copy);
        } else {
            send_append_entries(rf_handle.clone(), i, rf.current_term, prev_log_index,
                                rf.log[prev_log_index].term, rf.commit_index, Vec::new());
        }
    }
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
        // info!("[{term}] peer {0} begin election", rf_handle.me);
        begin_election(rf_handle.clone(), term, rf.log.len()-1, rf.log[rf.log.len()-1].term)
    };

    let mut num_granted = 1;
    let mut num_voted = 1;
    let mut break_flag = false;
    while let Some(result) = rx.next().await {
        {
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
            if !break_flag && rf.received_valid_rpc {
                rf.received_valid_rpc = false;
                rf.role = Role::Follower;
                break_flag = true;
            }
            if !break_flag && rf.role != Role::Candidate {
                break_flag = true;
            }
            if !break_flag && num_granted >= rf_handle.num_half_vote {
                info!("[{}] peer {} becomes leader", rf.current_term, rf.me);
                rf.role = Role::Leader;
                rf.next_index = vec![rf.log.len(); rf_handle.num_peers];
                rf.match_index = vec![0; rf_handle.num_peers];
                rf.match_index[rf_handle.me] = rf.log.len() - 1;
                for i in 0..rf_handle.num_peers {
                    if i == rf_handle.me {
                        continue;
                    }
                    send_append_entries(rf_handle.clone(), i, rf.current_term,
                                        rf.log.len() - 1, rf.log[rf.log.len() - 1].term, rf.commit_index, Vec::new());
                }
                break_flag = true;
            }
            if !break_flag && num_voted >= rf_handle.num_peers {
                break_flag = true;
            }
        }
        if break_flag {
            break;
        }
    }
    rf_handle.persist().await.expect("failed to persist");
}

fn begin_election(rf_handle: RaftHandle, term: u64, last_log_index: usize, last_log_term: u64)
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
            // info!("[{}] peer {} received reply {:?}", rf.current_term, rf.me, res);
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

fn send_append_entries(rf_handle: RaftHandle, peer_index: usize, term: u64, prev_log_index: usize,
                       prev_log_term: u64, leader_commit: usize, entries: Vec<LogEntry>) {
    let args: AppendEntriesArgs = AppendEntriesArgs {
        term,
        leader_id: rf_handle.me,
        prev_log_index,
        prev_log_term,
        entries: entries.clone(),
        leader_commit,
    };
    info!("[{}] peer {} send to peer {} {:?}", term, rf_handle.me, peer_index, args);
    let timeout = Raft::generate_election_timeout();
    let net = net::NetLocalHandle::current();
    let mut rpcs = FuturesUnordered::new();
    let peer = rf_handle.peers[peer_index].clone();
    let net = net.clone();
    let args_clone = args.clone();
    rpcs.push(async move {
        net.call_timeout::<AppendEntriesArgs, AppendEntriesReply>(peer, args_clone, timeout).await
    });

    task::spawn(async move {
        while let Some(res) = rpcs.next().await {
            {
                let mut rf = rf_handle.inner.lock().unwrap();
                match res {
                    Ok(reply) => {
                        if reply.term > rf.current_term {
                            rf.current_term = reply.term;
                            rf.role = Role::Follower;
                            rf.voted_for = None;
                        }
                        if reply.success {
                            // info!("[{}] peer {} received success from peer {}, {:?}", rf.current_term,
                            // rf.me, peer_index, args);
                            let new_match_index = prev_log_index + entries.len();
                            if new_match_index > rf.match_index[peer_index] {
                                rf.next_index[peer_index] = new_match_index + 1;
                                rf.match_index[peer_index] = new_match_index;
                            }
                            // info!("[{}] peer {} update match_index[{}] to {}", rf.current_term, rf.me, peer_index, new_match_index);
                        } else if reply.term == term {
                            if reply.xterm.is_none() {
                                // Follower log too short — jump directly to its length
                                rf.next_index[peer_index] = reply.xindex.expect("xindex must be set when xterm is None");
                            } else {
                                // Find last entry in leader's log with xterm
                                let x_term = reply.xterm.unwrap();
                                let found = (1..rf.log.len()).rev().find(|&j| rf.log[j].term == x_term);
                                rf.next_index[peer_index] = match found {
                                    // Leader has xterm: start after its last entry
                                    Some(j) => j + 1,
                                    // Leader doesn't have xterm: jump to first conflicting index
                                    None => reply.xindex.expect("xindex must be set when xterm is set"),
                                };
                            }
                            rf.next_index[peer_index] = rf.next_index[peer_index].max(1);
                            assert!(
                                rf.next_index[peer_index] >= 1,
                                "next_index[{}] = {} < 1 after backtrack",
                                peer_index, rf.next_index[peer_index]
                            );
                        }
                    }
                    Err(e) => {}
                }
            }
            rf_handle.persist().await.expect("failed to persist");
        }
    }).detach();
}


// HINT: put mutable non-async functions here
impl Raft {
    fn start(&mut self, data: &[u8]) -> Result<Start> {
        if self.role != Role::Leader {
            let leader = (self.me + 1) % self.peers.len();
            return Err(Error::NotLeader(leader));
        }
        self.log.push(LogEntry{
            term: self.current_term,
            command: data.to_vec(),
        });
        self.next_index[self.me] += 1;
        self.match_index[self.me] = self.log.len() - 1;

        Ok(Start {
            index: (self.log.len() - 1) as u64,
            term: self.current_term,
        })
    }

    // Here is an example to apply committed message.
    fn apply(&self, data: Vec<u8>, index: u64) {
        let entry: MyEntry = bincode::deserialize(&data).unwrap();
        info!("[{}] peer {} apply {} with index {}", self.current_term, self.me, entry.x , index);
        let msg = ApplyMsg::Command {
            data,
            index,
        };
        self.apply_ch.unbounded_send(msg).unwrap();
    }

    fn request_vote(&mut self, args: RequestVoteArgs) -> RequestVoteReply {
        // info!("[{}] peer {} received {:?}, current vote for {:?}",
        // self.current_term, self.me, args, self.voted_for);
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
        let candidate_up_to_date = {
            if args.last_log_term != self.log[self.log.len() - 1].term {
                args.last_log_term >= self.log[self.log.len() - 1].term
            } else {
                args.last_log_index >= (self.log.len() - 1)
            }
        };
        if (self.voted_for.is_none() || self.voted_for == Some(args.candidate_id))
            && candidate_up_to_date {
            self.voted_for = Some(args.candidate_id);
            self.received_valid_rpc = true;
            return RequestVoteReply {
                    term: self.current_term,
                    vote_granted: true,
            }
        }
        RequestVoteReply {
            term: self.current_term,
            vote_granted: false,
        }
    }

    fn append_entries(&mut self, args: AppendEntriesArgs) -> AppendEntriesReply {
        // info!("[{}] peer {} received {:?}", self.current_term, self.me, args);
        // info!("[{}] peer {} log {:?}", self.current_term, self.me, self.log);
        if args.term > self.current_term {
            self.current_term = args.term;
            self.role = Role::Follower;
            self.voted_for = None;
        }

        if args.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                xindex: None,
                xterm: None,
            };
        }

        // Reset election timer for any valid-term AppendEntries, even if log check fails.
        // Without this, a follower won't reset its timer while the leader backtracks,
        // causing spurious elections that keep resetting next_index and preventing convergence.
        self.received_valid_rpc = true;

        if args.prev_log_index >= self.log.len() {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                xindex: Some(self.log.len()),
                xterm: None,
            };
        }

        if self.log[args.prev_log_index].term != args.prev_log_term {
            let x_term = self.log[args.prev_log_index].term;
            let mut x_index = args.prev_log_index;
            while x_index > 0 && self.log[x_index - 1].term == x_term {
                x_index -= 1;
            }
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                xterm: Some(x_term),
                xindex: Some(x_index),
            };
        }

        // Steps 3 & 4: reconcile incoming entries with existing log
        for (i, entry) in args.entries.iter().enumerate() {
            let log_index = args.prev_log_index + 1 + i;
            if log_index < self.log.len() {
                if self.log[log_index].term != entry.term {
                    assert!(
                        log_index > self.commit_index,
                        "truncating log at index {} which is <= commit_index {} — would destroy committed entry",
                        log_index, self.commit_index
                    );
                    self.log.truncate(log_index);
                    self.log.extend_from_slice(&args.entries[i..]);
                    break;
                }
                // Entry already matches, keep scanning
            } else {
                // Past the end of our log — append remaining entries all at once
                self.log.extend_from_slice(&args.entries[i..]);
                break;
            }
        }

        // Step 5: advance commitIndex (must never go backwards)
        if args.leader_commit > self.commit_index {
            let prev_commit_index = self.commit_index;
            let last_new_entry_index = args.prev_log_index + args.entries.len();
            self.commit_index = args.leader_commit.min(last_new_entry_index);
            assert!(
                self.commit_index >= prev_commit_index,
                "commit_index went backwards {} -> {}",
                prev_commit_index, self.commit_index
            );
            assert!(
                self.commit_index < self.log.len(),
                "commit_index {} >= log.len() {} after AE update",
                self.commit_index, self.log.len()
            );
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
            xindex: None,
            xterm: None,
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
    last_log_index: usize,
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
    prev_log_index: usize,
    prev_log_term: u64,
    entries: Vec<LogEntry>,
    leader_commit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AppendEntriesReply {
    // Your data here.
    term: u64,
    success: bool,
    xterm: Option<u64>,
    xindex: Option<usize>,
}
