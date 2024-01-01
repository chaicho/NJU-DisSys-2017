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

import "sync"
import "labrpc"

import "math/rand"
import "time"
import "fmt"

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int        // latest term server has seen
	votedFor    int        // candidateId that received vote in current term (or null if none)
	log         []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader

	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	// Volatile state on leaders:
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

    state     int // State of the server (Follower, Candidate, Leader)
	votesReceived int // Number of votes received by the server in the current election
		// ... (you may need more fields depending on the algorithm
		
}

type LogEntry struct {
	Term    int         // term when entry was received by leader
	Command interface{} // command for state machine
}

const (
    Follower  = 0
    Candidate = 1
    Leader    = 2
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateID  int // candidate requesting the vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry

}


//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
	// Add any additional fields needed here
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

    // Your initialization code here.
    rf.currentTerm = 0
    rf.votedFor = -1 // Initially, no vote is given to any candidate
    rf.state = Follower // Start as a Follower
    // Initialize other fields...

    // initialize from state persisted before a crash
    rf.readPersist(persister.ReadRaftState())

    // Start a background goroutine for election handling
    go rf.startElection()

    return rf
}

func (rf *Raft) startElection() {
    for {
        // Reset the election timer with a randomized timeout
        timeout := getRandomTimeout()
        time.Sleep(timeout)

        rf.mu.Lock()
        if rf.state != Leader && rf.state != Candidate {
            rf.convertToCandidate()
            rf.currentTerm++
            rf.votedFor = rf.me
            rf.mu.Unlock()

            // Send RequestVote RPCs to other servers in parallel
            for server := range rf.peers {
                if server != rf.me {
					rf.mu.Lock()
					defer rf.mu.Unlock()
				
					// Prepare args for RequestVote RPC
					args := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateID:  rf.me,
						LastLogIndex: 0,
						LastLogTerm:  0,
						// LastLogIndex: len(rf.log) - 1,
						// LastLogTerm:  rf.log[len(rf.log)-1].Term,
					}
				
					var reply RequestVoteReply
				
					// Send RequestVote RPC to the server
					if rf.state != Candidate {
						return
					}
				
					ok := rf.sendRequestVote(server, args, &reply)
					if !ok {
						fmt.Printf("Server %v: RequestVote RPC to server %v failed\n", rf.me, server)
						return
					}
				
					rf.handleRequestVoteReply(reply)
                }
            }
        } else {
            rf.mu.Unlock()
        }
    }
}


func (rf *Raft) handleRequestVoteReply(reply RequestVoteReply) {
    rf.mu.Lock()
    defer rf.mu.Unlock()

    // Process RequestVote RPC reply
    if reply.Term > rf.currentTerm {
        rf.currentTerm = reply.Term
        rf.state = Follower
        rf.votedFor = -1
        // Additional steps to handle term update and transition to Follower
    } else {
        // Check if vote was granted and handle accordingly
        if reply.VoteGranted {
            rf.votesReceived++ // Increment vote count
            if rf.state == Candidate && rf.votesReceived > len(rf.peers)/2 {
                // If the server receives votes from the majority, become Leader
                rf.convertToLeader()
            }
        }
    }
}

func (rf *Raft) convertToLeader() {
    rf.state = Leader
    // Additional steps for transitioning to Leader state
    // Initialize nextIndex[] and matchIndex[] for log replication
    // Send initial empty AppendEntries RPCs to followers for heartbeats
}
func getRandomTimeout() time.Duration {
    rand.Seed(time.Now().UnixNano())
    return time.Duration(rand.Intn(300-150)+150) * time.Millisecond
}

func (rf *Raft) convertToCandidate() {
    rf.state = Candidate
    rf.votedFor = rf.me // Vote for self
    rf.currentTerm++
    // Reset election timer for the next round
}
