package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int32

const (
	LEADER    State = 0
	CANDIDATE State = 1
	FOLLOWER  State = 2
)

type LogEntry struct {
	Command interface{} // content of this entry
	Term    int
}

// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastTimeHeartBeat time.Time // last time received heartbeat or granted vote.
	elapseTimeOut     time.Duration
	currentState      State

	currentTerm  int
	votedFor     int
	currentVotes int // votes gained in current term.
	log          []LogEntry
	commitIndex  int
	lastApplied  int
	nextIndex    []int
	matchIndex   []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	isleader = rf.currentState == LEADER
	term = rf.currentTerm
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	NewEntries   []LogEntry
	LeaderCommit int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return true when candidate's log is more or equally up-to-date than self's.
func compareLog(self []LogEntry, candidateLastTerm int, candidateLastIndex int) bool {
	if len(self) == 0 {
		return true
	} else if candidateLastIndex == -1 {
		return false
	}

	selfLastTerm := self[len(self)-1].Term
	if candidateLastTerm > selfLastTerm {
		return true
	} else if candidateLastTerm == selfLastTerm && candidateLastIndex >= len(self)-1 {
		return true
	}
	return false
}

func (rf *Raft) resetTimer() {
	// lock should be held.
	rf.lastTimeHeartBeat = time.Now()
	rf.elapseTimeOut = time.Duration(rand.Intn(200)+200) * time.Millisecond
}

func (rf *Raft) handleFutureTerm(term int) bool {
	// lock should be held.
	if term > rf.currentTerm {
		rf.stepTerm(term)
		rf.currentState = FOLLOWER
		return true
	}
	return false
}

func (rf *Raft) stepTerm(newTerm int) {
	// lock should be held.
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.currentVotes = 0
}

func (rf *Raft) becomeLeader() {
	// lock should be held.
	rf.currentState = LEADER
	for i := 0; i < len(rf.peers); i += 1 {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}
	//fmt.Printf("%v is the leader of term %v, Leader log is:", rf.me, rf.currentTerm)
	//fmt.Println(rf.log)
	rf.sendAppendToAllPeers()
}

func (rf *Raft) becomeCandidate() {
	// lock should be held.
	rf.currentState = CANDIDATE
	rf.stepTerm(rf.currentTerm + 1)
}

func (rf *Raft) activateElection() {
	// lock should be held.
	rf.becomeCandidate()
	rf.votedFor = rf.me
	rf.currentVotes = 1
	rf.resetTimer()
	rf.sendRequestToAllPeers()
}

func (rf *Raft) checkApply() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		//fmt.Printf("%v has applied [index: %v, term: %v, command: %v]\n", rf.me, rf.lastApplied+1, rf.log[rf.lastApplied].Term, rf.log[rf.lastApplied].Command)
		rf.applyCh <- ApplyMsg{
			CommandValid:  true,
			Command:       rf.log[rf.lastApplied].Command,
			CommandIndex:  rf.lastApplied + 1,
			SnapshotValid: false,
			Snapshot:      nil,
			SnapshotTerm:  0,
			SnapshotIndex: 0,
		}
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// pre check.
	rf.handleFutureTerm(args.Term)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && compareLog(rf.log, args.LastLogTerm, args.LastLogIndex) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.resetTimer()
		return
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = true
	if args.Term < rf.currentTerm {
		return
	}
	rf.handleFutureTerm(args.Term)
	rf.resetTimer()
	//if args.PrevLogIndex == -1 {
	//	// empty
	//	return
	//}
	if args.PrevLogIndex >= len(rf.log) || (args.PrevLogIndex != -1 && args.PrevLogTerm != rf.log[args.PrevLogIndex].Term) {
		reply.Success = false
		//fmt.Printf("%v:return false, log=%v, args=%v, prevIndex=%v, prevTerm=%v\n", rf.me, rf.log, args.NewEntries, args.PrevLogIndex, args.PrevLogTerm)
		return
	}
	lastNewIndex := args.PrevLogIndex + len(args.NewEntries)
	appendEntries := args.NewEntries
	for i := args.PrevLogIndex + 1; i < len(rf.log) && i-args.PrevLogIndex-1 < len(args.NewEntries); i += 1 {
		if rf.log[i].Term != args.NewEntries[i-args.PrevLogIndex-1].Term {
			rf.log = rf.log[:i]
			appendEntries = args.NewEntries[i-args.PrevLogIndex-1:]
			break
		}
	}
	//fmt.Printf("%v:Before append, log=%v, appendEntries=%v, args=%v, prevIndex=%v, prevTerm=%v\n", rf.me, rf.log, appendEntries, args.NewEntries, args.PrevLogIndex, args.PrevLogTerm)
	rf.log = append(rf.log, appendEntries...)
	//fmt.Printf("%v:After append, log=%v\n", rf.me, rf.log)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		rf.handleFutureTerm(reply.Term)
		if reply.Term < rf.currentTerm {
			return // drop when reply is old.
		}
		// drop when I'm not candidate anymore.
		if rf.currentState == CANDIDATE {
			if reply.VoteGranted {
				rf.currentVotes += 1
			}
			if rf.currentVotes > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}
	}
	return
}

func (rf *Raft) sendRequestToAllPeers() {
	// lock held
	CopyPeers := make([]*labrpc.ClientEnd, len(rf.peers))
	copy(CopyPeers, rf.peers)
	copyTerm := rf.currentTerm
	copyLastLogIndex := len(rf.log) - 1
	copyLastLogTerm := -1
	if copyLastLogIndex != -1 {
		copyLastLogTerm = rf.log[copyLastLogIndex].Term
	}
	if len(rf.log) > 0 {
		copyLastLogTerm = rf.log[len(rf.log)-1].Term
	}

	for i := 0; i < len(CopyPeers); i += 1 {
		if i != rf.me {
			args := RequestVoteArgs{
				Term:         copyTerm,
				CandidateId:  rf.me,
				LastLogIndex: copyLastLogIndex,
				LastLogTerm:  copyLastLogTerm,
			}
			go rf.sendRequestVote(i, &args)
		}
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	oldTerm := args.Term
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if oldTerm < rf.currentTerm {
			return // drop when reply is old.
		}
		outDated := rf.handleFutureTerm(reply.Term)
		if outDated {
			return // return false because of outdated term, return immediately.
		}
		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.NewEntries)
			//fmt.Println(server, args)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			n := rf.matchIndex[server]
			if n > rf.commitIndex {
				count := 0
				for i := 0; i < len(rf.peers); i++ {
					if rf.matchIndex[i] >= n {
						count++
					}
				}
				if count >= len(rf.peers)/2 && rf.log[n].Term == rf.currentTerm {
					rf.commitIndex = n
				}
			}
		} else {
			rf.nextIndex[server] -= 1
		}
	}
	return
}

func (rf *Raft) sendAppendToAllPeers() {
	// lock held
	copyPeer := make([]*labrpc.ClientEnd, len(rf.peers))
	copy(copyPeer, rf.peers)
	copyTerm := rf.currentTerm
	copyEntries := make([]LogEntry, len(rf.log))
	copy(copyEntries, rf.log)
	copyLeaderCommit := rf.commitIndex
	copyNextIndex := make([]int, len(rf.nextIndex))
	copy(copyNextIndex, rf.nextIndex)
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)

	for i := 0; i < len(copyPeer); i += 1 {
		if i != rf.me {
			prevIndex := copyNextIndex[i] - 1
			prevLogTerm := -1
			if prevIndex != -1 {
				prevLogTerm = copyEntries[prevIndex].Term
			}
			args := AppendEntriesArgs{
				Term:         copyTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevLogTerm,
				NewEntries:   make([]LogEntry, 0),
				LeaderCommit: copyLeaderCommit,
			}
			if len(copyEntries) > copyNextIndex[i] {
				args.NewEntries = copyEntries[copyNextIndex[i]:]
			}
			go rf.sendAppendEntries(i, &args)
		}
	}
	return
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	isLeader := rf.currentState == LEADER
	index := -1
	term := -1
	//fmt.Printf("start: %v: term=%v, command=%v\n", rf.me, rf.currentTerm, command)
	if isLeader {
		rf.log = append(rf.log, LogEntry{command, rf.currentTerm})
		index = len(rf.log)
		term = rf.currentTerm
		isLeader = rf.currentState == LEADER
	}
	rf.mu.Unlock()

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if time.Now().Sub(rf.lastTimeHeartBeat) > rf.elapseTimeOut {
			switch rf.currentState {
			case FOLLOWER:
				rf.activateElection()
			case CANDIDATE:
				rf.activateElection()
			}
		}
		rf.checkApply()
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// send heartbeats to all peers periodically when I'm leader.
func (rf *Raft) beater() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.currentState == LEADER {
			rf.sendAppendToAllPeers()
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.votedFor = -1
	rf.currentState = FOLLOWER
	rf.resetTimer()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.beater()
	return rf
}
