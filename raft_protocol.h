#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"

enum raft_rpc_opcodes
{
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status
{
    OK,
    RETRY,
    RPCERR,
    NOENT,
    IOERR
};

class request_vote_args
{
public:
    // Lab3: Your code here
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;
};

marshall &operator<<(marshall &m, const request_vote_args &args);
unmarshall &operator>>(unmarshall &u, request_vote_args &args);

class request_vote_reply
{
public:
    // Lab3: Your code here
    int term;
    bool voteGranted;
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);
unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

template <typename command>
class log_entry
{
public:
    // Lab3: Your code here
    int index;
    command cmd;
    int term;
    log_entry():index(0){}
};

template <typename command>
marshall &operator<<(marshall &m, const log_entry<command> &entry)
{
    // Lab3: Your code here
    m << entry.index << entry.cmd << entry.term;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, log_entry<command> &entry)
{
    // Lab3: Your code here
    u >> entry.index >> entry.cmd >> entry.term;
    return u;
}

template <typename command>
class append_entries_args
{
public:
    // Your code here
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<log_entry<command>> entries;
    int leaderCommit;
    append_entries_args() {}
    append_entries_args(int _term, int _leaderId, int _prevIndex, int _prevTerm, int prev, std::vector<log_entry<command>> _entry, int _leader_commit) : term(_term), leaderId(_leader_commit),
                                                                                                                                                         prevLogIndex(_prevIndex), prevLogTerm(_prevTerm),
                                                                                                                                                         entries(_entry), leaderCommit(_leader_commit) {}
};

template <typename command>
marshall &operator<<(marshall &m, const append_entries_args<command> &args)
{
    // Lab3: Your code here
    m << args.term << args.leaderId << args.prevLogIndex << args.prevLogTerm << args.entries << args.leaderCommit;
    return m;
}

template <typename command>
unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args)
{
    // Lab3: Your code here
    u >> args.term >> args.leaderId >> args.prevLogIndex >> args.prevLogTerm >> args.entries >> args.leaderCommit;
    return u;
}

class append_entries_reply
{
public:
    // Lab3: Your code here
    int term;
    bool success;
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);
unmarshall &operator>>(unmarshall &m, append_entries_reply &reply);

class install_snapshot_args
{
public:
    // Lab3: Your code here
    int term;
    int leaderId;
    int lastIncludedIndex;
    int lastIncludedTerm;
    std::vector<char> data;
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);
unmarshall &operator>>(unmarshall &m, install_snapshot_args &args);

class install_snapshot_reply
{
public:
    // Lab3: Your code here
    int term;
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);
unmarshall &operator>>(unmarshall &m, install_snapshot_reply &reply);

#endif // raft_protocol_h