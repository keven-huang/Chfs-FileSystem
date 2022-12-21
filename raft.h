#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <set>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

template <typename state_machine, typename command>
class raft
{

    static_assert(std::is_base_of<raft_state_machine, state_machine>(), "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

#define DEBUG 0
#if !DEBUG
#define RAFT_LOG(fmt, args...) \
    do                         \
    {                          \
    } while (0);
#else
#define RAFT_LOG(fmt, args...)                                                                                   \
    do                                                                                                           \
    {                                                                                                            \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
    } while (0);
#endif

public:
    raft(
        rpcs *rpc_server,
        std::vector<rpcc *> rpc_clients,
        int idx,
        raft_storage<command> *storage,
        state_machine *state);
    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role
    {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:
    // last heartbeat time
    uint64_t last_recv;
    std::vector<char> snapshot;

    int commitIndex;
    int lastApplied;

    std::set<int> voter;

    std::vector<int> nextIndex;
    std::vector<int> matchIndex;

private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);
    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);
    void handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    void send_install_snapshot(int target, install_snapshot_args arg);
    void handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();
    int num_nodes() { return rpc_clients.size(); }

    // background workers

    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();

    // Your code here:
    void send_request_vote_to_all();
    void send_append_entries_to_all(bool empty);
    void to_candidate();
    void to_follower();
    void to_leader();

    void begin_new_term(int new_term);
    int get_last_log_index();
    int get_front_log_index();
    int get_log_term(int index);
    bool match(int prev_log_index, int prev_log_term);

    int get_commitIndex();
};

template <typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage, state_machine *state) : storage(storage),
                                                                                                                                               state(state),
                                                                                                                                               rpc_server(server),
                                                                                                                                               rpc_clients(clients),
                                                                                                                                               my_id(idx),
                                                                                                                                               stopped(false),
                                                                                                                                               role(follower),
                                                                                                                                               current_term(0),
                                                                                                                                               background_election(nullptr),
                                                                                                                                               background_ping(nullptr),
                                                                                                                                               background_commit(nullptr),
                                                                                                                                               background_apply(nullptr)
{
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    last_recv = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

    voter.clear();

    srand(time(0));

    commitIndex = 0;
    lastApplied = 0;
    RAFT_LOG("restore");
    storage->restore(snapshot);
    // if (!snapshot.empty())
    // {
    //     state->apply_snapshot(snapshot);
    // }
    current_term = storage->current_term;
}

template <typename state_machine, typename command>
raft<state_machine, command>::~raft()
{
    if (background_ping)
    {
        delete background_ping;
    }
    if (background_election)
    {
        delete background_election;
    }
    if (background_commit)
    {
        delete background_commit;
    }
    if (background_apply)
    {
        delete background_apply;
    }
    delete thread_pool;
}

/******************************************************************
                        Public Interfaces
*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::stop()
{
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped()
{
    return stopped.load();
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term)
{
    term = current_term;
    return role == leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::start()
{
    // Your code here:

    RAFT_LOG("start");

    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index)
{
    // Your code here:
    std::unique_lock<std::mutex> lck(mtx);
    if (role == leader)
    {
        log_entry<command> new_log;
        new_log.index = get_last_log_index() + 1;
        new_log.cmd = cmd;
        new_log.term = current_term;
        RAFT_LOG("GET COMMAND index = %d", new_log.index);
        storage->log.emplace_back(new_log);
        storage->updateLog();

        term = current_term;
        index = get_last_log_index();

        return true;
    }

    return false;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot()
{
    // Your code here:
    std::unique_lock<std::mutex> lck(mtx);
    // get snapshot of state
    snapshot = state->snapshot();
    // delete log of applied
    if (lastApplied <= get_last_log_index())
    {
        RAFT_LOG("log size = %ld lastapplied = %d,firstindex = %d", storage->log.size(), lastApplied, get_front_log_index());
        auto front = get_front_log_index();
        storage->log.erase(storage->log.begin(), storage->log.begin() + lastApplied - front);
        RAFT_LOG("erase success");
    }
    else
    {
        storage->log.clear();
    }
    // update storage snapshot
    storage->updateSnapshot(snapshot);
    storage->updateLog();
    return true;
}

/******************************************************************
                         RPC Related
*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply)
{
    // Your code here:
    std::unique_lock<std::mutex> lck(mtx);
    RAFT_LOG("get request vote from %d,term = %d,index = %d", args.candidateId, args.term, args.lastLogIndex);
    if (args.term >= current_term)
    {
        if (args.term > current_term)
        {
            to_follower();
            begin_new_term(args.term);
        }

        if (storage->vote_for == -1 || storage->vote_for == args.candidateId)
        {
            int last_log_index = get_last_log_index();
            int last_log_term = get_log_term(last_log_index);
            if (last_log_term < args.lastLogTerm || (last_log_term == args.lastLogTerm && last_log_index <= args.lastLogIndex))
            {
                reply.voteGranted = true;
                storage->vote_for = args.candidateId;
                storage->updateMetaData();

                RAFT_LOG("grant vote for all");
                return 0;
            }
        }
    }
    reply.term = current_term;
    reply.voteGranted = false;
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply)
{
    // Your code here:
    std::unique_lock<std::mutex> lck(mtx);
    if (reply.term > current_term)
    {
        to_follower();
        begin_new_term(reply.term);
        return;
    }
    if (role != candidate)
    {
        return;
    }
    if (reply.voteGranted)
    {
        voter.emplace(target);
    }
    if ((int)voter.size() > num_nodes() / 2)
    {
        to_leader();
        RAFT_LOG("I'm LEADER");
    }
    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply)
{
    // Your code here:
    std::unique_lock<std::mutex> lck(mtx);
    if (arg.term < current_term)
    {
        reply.success = false;
        reply.term = current_term;
        return 0;
    }

    if (arg.leaderId == my_id)
    {
        reply.success = true;
        return 0;
    }

    // if nessary update current term
    if (current_term < arg.term)
    {
        to_follower();
        begin_new_term(arg.term);
    }

    // heartbeat
    if (arg.entries.empty())
    {
        last_recv = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        // if is candidate back to follower
        if (role == candidate)
        {
            to_follower();
        }

        if (match(arg.prevLogIndex, arg.prevLogTerm) && arg.prevLogIndex == get_last_log_index())
        {
            if (arg.leaderCommit > commitIndex)
            {
                commitIndex = std::min(arg.leaderCommit, get_last_log_index());
            }
        }
        return 0;
    }
    // send entry is not empty
    if (!match(arg.prevLogIndex, arg.prevLogTerm))
    {
        reply.success = false;
        reply.term = current_term;
        return 0;
    }
    else
    {
        RAFT_LOG("match and erase log,prevlogIndex = %d", arg.prevLogIndex);
        storage->log.erase(storage->log.begin() + arg.prevLogIndex, storage->log.end());
        storage->log.insert(storage->log.end(), arg.entries.begin(), arg.entries.end());

        storage->updateLog();

        reply.success = true;

        if (arg.leaderCommit > commitIndex)
        {
            commitIndex = std::min(arg.leaderCommit, get_last_log_index());
        }
    }
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply)
{
    // Your code here:

    if (role != leader)
    {
        return;
    }

    // heartbeat
    if (arg.entries.empty())
    {
        return;
    }
    std::unique_lock<std::mutex> lck(mtx);
    // not heartbeat
    if (!reply.success)
    {
        if (reply.term > current_term)
        {
            to_follower();
            begin_new_term(reply.term);
        }
        else
        {
            if (nextIndex[target] > 1)
            {
                --nextIndex[target];
            }
        }
        return;
    }

    matchIndex[target] = arg.prevLogIndex + arg.entries.size();
    nextIndex[target] = matchIndex[target] + 1;

    commitIndex = get_commitIndex();

    return;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply)
{
    // Your code here:
    std::unique_lock<std::mutex> lck(mtx);
    // TO DO
    last_recv = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    reply.term = current_term;
    if (current_term > args.term)
    {
        return 0;
    }

    if (role == candidate)
    {
        to_follower();
        begin_new_term(args.term);
        return 0;
    }

    if (args.term > current_term)
    {
        to_follower();
        begin_new_term(args.term);
        return 0;
    }
    int last_log_index = get_last_log_index();
    // write data into snapshot file according to the offset
    if (args.lastIncludedIndex <= last_log_index && args.lastIncludedTerm == get_log_term(last_log_index))
    {
        int end_index = args.lastIncludedIndex;

        if (end_index <= last_log_index)
        {
            storage->log.erase(storage->log.begin(), storage->log.begin() + end_index - get_front_log_index());
        }
        else
        {
            storage->log.clear();
        }
    }
    else
    {
        log_entry<command> cmd;
        cmd.term = args.term = args.lastIncludedTerm;
        cmd.index = args.lastIncludedIndex;
        storage->log.assign(1, cmd);
    }
    // reset state machine's snapshot

    snapshot = args.data;
    state->apply_snapshot(snapshot);
    lastApplied = args.lastIncludedIndex;
    commitIndex = std::max(commitIndex, args.lastIncludedIndex);
    storage->updateLog();
    storage->updateSnapshot(snapshot);
    return 0;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply)
{
    // Your code here:

    // if reply.term is greater than current_term
    if (reply.term > current_term)
    {
        to_follower();
        begin_new_term(reply.term);
    }
    if (role != leader)
    {
        return;
    }
    matchIndex[target] = std::max(matchIndex[target], arg.lastIncludedIndex);
    nextIndex[target] = matchIndex[target] + 1;
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg)
{
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0)
    {
        handle_request_vote_reply(target, arg, reply);
    }
    else
    {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg)
{
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0)
    {
        handle_append_entries_reply(target, arg, reply);
    }
    else
    {
        // RPC fails
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg)
{
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0)
    {
        handle_install_snapshot_reply(target, arg, reply);
    }
    else
    {
        // RPC fails
    }
}

/******************************************************************
                        Background Workers
*******************************************************************/

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_election()
{
    while (true)
    {
        if (is_stopped())
            return;
        // Your code here:
        {
            std::unique_lock<std::mutex> lck(mtx);
            uint64_t now_time = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            uint64_t diff = (now_time - last_recv);
            switch (role)
            {
            case follower:
            {
                // election_time_out range from 150–300ms
                uint64_t election_time_out = rand() % 150 + 150;
                if (diff > election_time_out)
                {
                    to_candidate();
                    begin_new_term(current_term + 1);
                    send_request_vote_to_all();
                }
            }
            break;
            case candidate:
            {
                uint64_t vote_time_out = 600;
                if (diff > vote_time_out)
                {
                    begin_new_term(current_term + 1);
                    RAFT_LOG("last vote is timeout, begin a new vote");
                    send_request_vote_to_all();
                }
            }
            break;
            case leader:
                // do nothing and break
                break;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit()
{
    // Periodly send logs to the follower.

    // Only work for the leader.

    while (true)
    {
        if (is_stopped())
            return;
        // Your code here:
        {
            std::unique_lock<std::mutex> lck(mtx);
            if (role == leader)
            {
                int num_node = num_nodes();
                for (int i = 0; i < num_node; i++)
                {
                    if (nextIndex[i] <= get_last_log_index())
                    {
                        // if (nextIndex[i] >= get_front_log_index())
                        // {
                        int prev_log_index = nextIndex[i] - 1;
                        append_entries_args<command> args;
                        args.term = current_term;
                        args.leaderId = my_id;
                        args.prevLogIndex = prev_log_index;
                        args.prevLogTerm = get_log_term(prev_log_index);
                        args.leaderCommit = commitIndex;
                        // entries
                        args.entries = std::vector<log_entry<command>>(storage->log.begin() + prev_log_index, storage->log.end());
                        if (!thread_pool->addObjJob(this, &raft::send_append_entries, i, args))
                        {
                            std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        }
                        // }
                        // else
                        // {
                        //     install_snapshot_args args;
                        //     args.term = current_term;
                        //     args.leaderId = my_id;
                        //     args.lastIncludedIndex = get_front_log_index();
                        //     args.lastIncludedTerm = get_log_term(args.lastIncludedIndex);
                        //     args.data = snapshot;
                        //     if (!thread_pool->addObjJob(this, &raft::send_install_snapshot, i, args))
                        //     {
                        //         std::this_thread::sleep_for(std::chrono::milliseconds(10));
                        //     }
                        // }
                    }
                }
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply()
{
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    while (true)
    {
        if (is_stopped())
            return;
        // Your code here:
        {
            std::unique_lock<std::mutex> lck(mtx);
            while (commitIndex > lastApplied)
            {
                RAFT_LOG("apply log leader %d", lastApplied);
                state->apply_log(storage->log[lastApplied].cmd);
                ++lastApplied;
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping()
{
    // Periodly send empty append_entries RPC to the followers.
    uint64_t heartbeat_time_out = 150;
    // Only work for the leader.

    while (true)
    {
        if (is_stopped())
            return;
        // Your code here:
        {
            std::unique_lock<std::mutex> lck(mtx);
            if (role == leader)
            {
                send_append_entries_to_all(true);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat_time_out));
    }
    return;
}

/******************************************************************
                        Other functions
*******************************************************************/
template <typename state_machine, typename command>
int raft<state_machine, command>::get_commitIndex()
{
    std::vector<int> commit = matchIndex;
    std::sort(commit.begin(), commit.end());
    return commit[commit.size() / 2];
}

template <typename state_machine, typename command>
void raft<state_machine, command>::to_follower()
{
    voter.clear();
    role = follower;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::to_candidate()
{
    role = candidate;
    voter.clear();
}

template <typename state_machine, typename command>
void raft<state_machine, command>::to_leader()
{
    nextIndex = std::vector<int>(num_nodes(), get_last_log_index() + 1);
    matchIndex = std::vector<int>(num_nodes(), 0);
    role = leader;
}

template <typename state_machine, typename command>
void raft<state_machine, command>::begin_new_term(int term)
{
    // begin a new term
    voter.clear();
    current_term = term;
    last_recv = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    storage->current_term = term;
    storage->vote_for = -1;
    storage->updateMetaData();
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries_to_all(bool empty)
{
    if (!empty)
    {
        RAFT_LOG("send entry to all %d %d", get_last_log_index(), nextIndex[0]);
    }
    int num_node = num_nodes();
    for (int i = 0; i < num_node; i++)
    {
        if (!empty)
        {
            if (get_last_log_index() < nextIndex[i])
                continue;
        }
        int prev_log_index = nextIndex[i] - 1;
        append_entries_args<command> args;
        args.term = current_term;
        args.leaderId = my_id;
        args.prevLogIndex = prev_log_index;
        args.prevLogTerm = get_log_term(prev_log_index);
        args.leaderCommit = commitIndex;
        // entries
        if (!empty)
        {
            args.entries = std::vector<log_entry<command>>(storage->log.begin() + prev_log_index, storage->log.end());
        }
        else
        {
            args.entries.clear();
        }
        if (!thread_pool->addObjJob(this, &raft::send_append_entries, i, args))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

template <typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote_to_all()
{
    RAFT_LOG("send requestmake vote to all");
    auto lst_log_index = get_last_log_index();
    request_vote_args request{
        current_term,
        my_id,
        lst_log_index,
        get_log_term(lst_log_index)};

    int num_node = num_nodes();
    for (int i = 0; i < num_node; ++i)
    {
        if (!thread_pool->addObjJob(this, &raft::send_request_vote, i, request))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
}

template <typename state_machine, typename command>
int raft<state_machine, command>::get_last_log_index()
{
    if (storage->log.size() == 0)
    {
        return 0;
    }
    // auto index = storage->log.back().index;
    return storage->log.size();
}

template <typename state_machine, typename command>
int raft<state_machine, command>::get_front_log_index()
{
    if (storage->log.size() == 0)
    {
        return 0;
    }
    return storage->log.front().index;
}

template <typename state_machine, typename command>
bool raft<state_machine, command>::match(int prev_log_index, int prev_log_term)
{
    if (get_last_log_index() < prev_log_index)
    {
        return false;
    }

    return get_log_term(prev_log_index) == prev_log_term;
}

template <typename state_machine, typename command>
int raft<state_machine, command>::get_log_term(int index)
{
    if (index > get_last_log_index())
    {
        return -1;
    }
    if (index == 0)
    {
        return 0;
    }
    // auto real_index = index - get_front_log_index();
    // auto term = storage->log[real_index].term;
    return storage->log[index - 1].term;
}

#endif // raft_h