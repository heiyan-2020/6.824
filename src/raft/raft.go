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
	"6.824/labgob"
	"bytes"
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
	Index   int
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

	snapShot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.acquire("GetState")
	defer rf.release("GetState")
	isleader = rf.currentState == LEADER
	term = rf.currentTerm
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
	// lock held.
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	Debug(dPersist, "S%v persists", rf.me)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapShot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapShot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.acquire("readPersist")
	defer rf.release("readPersist")
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var logs []LogEntry
	var lastIndex int
	var lastTerm int
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIndex) != nil ||
		d.Decode(&lastTerm) != nil {
		Debug(dWarn, "S%v Decode failed.", rf.me)
	} else {
		rf.currentTerm = term
		rf.votedFor = voteFor
		rf.log = logs
		rf.snapShot = snapShot
		rf.lastIncludedIndex = lastIndex
		rf.lastIncludedTerm = lastTerm
		Debug(dPersist, "S%v reads from persistent state, term=%v, voted=%v, log=%v.", rf.me, rf.currentTerm, rf.votedFor, rf.log)
	}
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
	rf.acquire("Snapshot")
	defer rf.release("Snapshot")

	// get actual index in advance to avoid overriding of lastIncludedIndex
	index -= 1
	actualIndex := rf.getActualIndex(index)

	// install new snapShot
	rf.snapShot = snapshot
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[actualIndex].Term
	Debug(dSnap, "S%v has installed snapShot of [lastIndex=%v lastTerm=%v]", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)

	// trim log
	if actualIndex == len(rf.log)-1 {
		rf.log = make([]LogEntry, 0)
	} else {
		rf.log = rf.log[actualIndex+1:]
	}

	// persist
	rf.persist()
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

type InstallSnapShotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
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
	XTerm   int
	XIndex  int
	XLen    int
}

type InstallSnapshotReply struct {
	Term int
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

func (rf *Raft) acquire(funcName string) {
	//Debug(dLock, "S%v acquire lock from %v", rf.me, funcName)
	rf.mu.Lock()
	//Debug(dLock, "S%v acquired", rf.me)
}

func (rf *Raft) release(funcName string) {
	//Debug(dInfo, "S%v release lock from %v", rf.me, funcName)
	rf.mu.Unlock()
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
	rf.persist()
}

func (rf *Raft) getCompleteLogLength() int {
	return rf.lastIncludedIndex + len(rf.log) + 1
}

func (rf *Raft) getActualIndex(logicIndex int) int {
	res := logicIndex - rf.lastIncludedIndex - 1
	if res < 0 {
		Debug(dWarn, "S%v logicIndex=%v, lastIndex=%v", rf.me, logicIndex, rf.lastIncludedIndex)
	}
	return res
}

func (rf *Raft) getTerm(logicIndex int) int {
	if logicIndex == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.log[rf.getActualIndex(logicIndex)].Term
}

func (rf *Raft) becomeLeader() {
	// lock should be held.
	rf.currentState = LEADER
	initialNextIndex := rf.getCompleteLogLength()
	for i := 0; i < len(rf.peers); i += 1 {
		rf.nextIndex[i] = initialNextIndex
		rf.matchIndex[i] = -1
	}
	Debug(dLeader, "S%v becomes leader of Term:%v", rf.me, rf.currentTerm)
	rf.sendAppendToAllPeers()
}

func (rf *Raft) becomeCandidate() {
	// lock should be held.
	rf.currentState = CANDIDATE
	rf.stepTerm(rf.currentTerm + 1)
}

func (rf *Raft) activateElection() {
	// lock should be held.
	Debug(dLeader, "S%v wants to become leader", rf.me)
	rf.becomeCandidate()
	rf.votedFor = rf.me
	rf.persist()
	rf.currentVotes = 1
	rf.resetTimer()
	rf.sendRequestToAllPeers()
}

//func (rf *Raft) checkApply() {
//	if rf.commitIndex > rf.lastApplied {
//		rf.lastApplied++
//		actualIndex := rf.getActualIndex(rf.lastApplied)
//		Debug(dClient, "S%v apply [%v] at [%v], log=%v", rf.me, rf.log[actualIndex].Command, rf.lastApplied+1, rf.log)
//		msg := ApplyMsg{
//			CommandValid:  true,
//			Command:       rf.log[actualIndex].Command,
//			CommandIndex:  rf.lastApplied + 1,
//			SnapshotValid: len(rf.snapShot) > 0,
//			Snapshot:      rf.snapShot,
//			SnapshotTerm:  rf.lastIncludedTerm,
//			SnapshotIndex: rf.lastIncludedIndex,
//		}
//		rf.release("checkApply")
//		rf.applyCh <- msg
//	}
//}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.acquire("RequestVote")
	defer rf.release("RequestVote")

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
		rf.persist()
		Debug(dPersist, "S%v persist when voting", rf.me)
		rf.resetTimer()
		Debug(dTimer, "S%v reset timer for granting vote", rf.me)
		return
	}
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.acquire("AppendEntries")
	defer rf.release("AppendEntries")

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = rf.getCompleteLogLength()
	if args.Term < rf.currentTerm {
		return
	}
	rf.handleFutureTerm(args.Term)
	rf.resetTimer()
	if args.PrevLogIndex >= rf.getCompleteLogLength() || (args.PrevLogIndex != -1 && args.PrevLogTerm != rf.getTerm(args.PrevLogIndex)) {
		reply.Success = false
		/*	return more information for fast backing up	*/
		if args.PrevLogIndex != -1 && args.PrevLogIndex < rf.getCompleteLogLength() {
			reply.XTerm = rf.getTerm(args.PrevLogIndex)
			i := args.PrevLogIndex
			for ; i >= 0 && rf.getTerm(i) == reply.XTerm; i-- {
			}
			reply.XIndex = i
			Debug(dInfo, "S%v <- S%v Append failed.\n", rf.me, args.LeaderId)
		}
		return
	}
	lastNewIndex := args.PrevLogIndex + len(args.NewEntries)
	appendEntries := args.NewEntries
	for i := args.PrevLogIndex + 1; i < rf.getCompleteLogLength() && i-args.PrevLogIndex-1 < len(args.NewEntries); i += 1 {
		if rf.getTerm(i) != args.NewEntries[i-args.PrevLogIndex-1].Term {
			rf.log = rf.log[:i]
			appendEntries = args.NewEntries[i-args.PrevLogIndex-1:]
			break
		}
		appendEntries = args.NewEntries[i-args.PrevLogIndex:]
	}
	if len(appendEntries) == 0 {
		Debug(dLog, "S%v <- S%v heartbeat", rf.me, args.LeaderId)
	} else {
		Debug(dLog, "S%v append log at %v from S%v: %v.", rf.me, args.PrevLogIndex+1, args.LeaderId, appendEntries)
	}
	rf.log = append(rf.log, appendEntries...)
	rf.persist()
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit <= lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
		Debug(dCommit, "S%v commitIndex = %v", rf.me, rf.commitIndex)
	}
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if ok {
		rf.acquire("SendQuest")
		defer rf.release("SendQuest")

		if args.Term < rf.currentTerm {
			return // drop when reply is old.
		}

		rf.handleFutureTerm(reply.Term)
		if reply.Term < rf.currentTerm {
			return // drop when reply is old.
		}
		// drop when I'm not candidate anymore.
		if rf.currentState == CANDIDATE {
			if reply.VoteGranted {
				Debug(dVote, "S%v <- S%v votes", rf.me, server)
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
	lastLogIndex := rf.getCompleteLogLength() - 1
	lastLogTerm := -1
	if lastLogIndex != -1 {
		lastLogTerm = rf.getTerm(lastLogIndex)
	}

	for i := 0; i < len(rf.peers); i += 1 {
		if i != rf.me {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			go rf.sendRequestVote(i, &args)
		}
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	if ok {
		rf.acquire("SendAppendEntries")
		defer rf.release("SendAppendEntries")

		if args.Term < rf.currentTerm {
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
				if count >= len(rf.peers)/2 && rf.getTerm(n) == rf.currentTerm {
					rf.commitIndex = n
				}
			}
		} else {
			/* take advantage of returned information */
			if reply.XTerm == -1 {
				rf.nextIndex[server] = reply.XLen
			} else {
				i := args.PrevLogIndex // i won't be -1 because PrevLogIndex == -1 will always return true.
				for ; i >= 0 && rf.getTerm(i) != reply.XTerm; i-- {
				}
				if i == -1 {
					// XTerm is not in log.
					rf.nextIndex[server] = reply.XIndex
				} else {
					rf.nextIndex[server] = i + 1
				}
			}
		}
	}
	return
}

func (rf *Raft) sendAppendToAllPeers() {
	// lock held
	for i := 0; i < len(rf.peers); i += 1 {
		if i != rf.me {
			prevIndex := rf.nextIndex[i] - 1
			prevLogTerm := -1
			if prevIndex != -1 {
				prevLogTerm = rf.getTerm(prevIndex)
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevLogTerm,
				NewEntries:   make([]LogEntry, 0),
				LeaderCommit: rf.commitIndex,
			}
			if rf.getCompleteLogLength() > rf.nextIndex[i] {
				if rf.lastIncludedIndex < rf.nextIndex[i] {
					// newEntries are all within existing log
					args.NewEntries = rf.log[rf.getActualIndex(rf.nextIndex[i]):]
					Debug(dLog2, "S%v -> S%v append %v.", rf.me, i, args.NewEntries)
					go rf.sendAppendEntries(i, &args)
				} else {
					// There are some newEntries laying within snapShot, call installSnapshot
				}
			} else {
				// heartbeat
				Debug(dLog2, "S%v -> S%v sends heartbeat", rf.me, i)
				go rf.sendAppendEntries(i, &args)
			}
		}
	}
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapShotArgs) {
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
	if ok {
		rf.acquire("SendInstallSnapshot")
		defer rf.release("SendInstallSnapshot")

		if args.Term < rf.currentTerm {
			return
		}

		rf.handleFutureTerm(reply.Term)
		
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapShotArgs, reply *InstallSnapshotReply) {
	rf.acquire("InstallSnapshot")
	defer rf.release("InstallSnapshot")

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.handleFutureTerm(args.Term)
	rf.resetTimer() // install snapShot is kinda heartbeat.

	rf.snapShot = args.Data

	if args.LastIncludedIndex > rf.lastIncludedIndex && args.LastIncludedIndex < rf.getCompleteLogLength()-1 {
		rf.log = rf.log[rf.getActualIndex(args.LastIncludedIndex+1):]
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.lastIncludedIndex = args.LastIncludedIndex
		return
	}

	rf.log = make([]LogEntry, 0)
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex

	// Send Snapshot to application.

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.acquire("Start")
	defer rf.release("Start")
	isLeader := rf.currentState == LEADER
	index := -1
	term := -1
	//fmt.Printf("start: %v: term=%v, command=%v\n", rf.me, rf.currentTerm, command)
	if isLeader {
		index = rf.getCompleteLogLength()
		Debug(dClient, "S%v receive [%v] at %v", rf.me, command, index)
		rf.log = append(rf.log, LogEntry{command, rf.currentTerm, index})
		rf.persist()
		term = rf.currentTerm
	}

	// Your code here (2B).

	return index + 1, term, isLeader
}

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
		rf.acquire("ticker")
		if time.Now().Sub(rf.lastTimeHeartBeat) > rf.elapseTimeOut {
			switch rf.currentState {
			case FOLLOWER:
				rf.activateElection()
			case CANDIDATE:
				rf.activateElection()
			}
		}
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			actualIndex := rf.getActualIndex(rf.lastApplied)
			Debug(dClient, "S%v apply [%v] at [%v], log=%v", rf.me, rf.log[actualIndex].Command, rf.lastApplied+1, rf.log)
			msg := ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[actualIndex].Command,
				CommandIndex:  rf.lastApplied + 1,
				SnapshotValid: false,
				Snapshot:      rf.snapShot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.release("ticker")
			rf.applyCh <- msg // blocking
		} else {
			rf.release("ticker")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// send heartbeats to all peers periodically when I'm leader.
func (rf *Raft) beater() {
	for rf.killed() == false {
		rf.acquire("beater")
		if rf.currentState == LEADER {
			rf.sendAppendToAllPeers()
		}
		rf.release("beater")
		time.Sleep(100 * time.Millisecond)
	}
}

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
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1
	rf.applyCh = applyCh

	Debug(dClient, "S%v starts", rf.me)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.beater()
	return rf
}
