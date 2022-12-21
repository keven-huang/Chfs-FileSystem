#include "extent_server_dist.h"

chfs_raft *extent_server_dist::leader() const
{
    int leader = this->raft_group->check_exact_one_leader();
    if (leader < 0)
    {
        return this->raft_group->nodes[0];
    }
    else
    {
        return this->raft_group->nodes[leader];
    }
}

int extent_server_dist::create(uint32_t type, extent_protocol::extentid_t &id)
{
    // Lab3: your code here
    chfs_command_raft chfs_cmd;
    chfs_cmd.cmd_tp = chfs_command_raft::CMD_CRT;
    chfs_cmd.type = type;
    int log_index, term;
    ASSERT(leader()->new_command(chfs_cmd, term, log_index) == 1, "leader create command failed");
    {
        std::unique_lock<std::mutex> lck(chfs_cmd.res->mtx);
        while (!chfs_cmd.res->done)
        {
            chfs_cmd.res->cv.wait(lck);
        }
    }
    id = chfs_cmd.res->id;
    return extent_protocol::OK;
}

int extent_server_dist::put(extent_protocol::extentid_t id, std::string buf, int &)
{
    // Lab3: your code here
    printf("extent dist -put\n");
    chfs_command_raft chfs_cmd;
    chfs_cmd.cmd_tp = chfs_command_raft::CMD_PUT;
    chfs_cmd.id = id;
    chfs_cmd.buf = buf;
    int log_index, term;
    ASSERT(leader()->new_command(chfs_cmd, term, log_index) == 1, "leader send command failed");
    {
        std::unique_lock<std::mutex> lck(chfs_cmd.res->mtx);
        while (!chfs_cmd.res->done)
        {
            chfs_cmd.res->cv.wait(lck);
        }
    }
    printf("return success!\n");
    return extent_protocol::OK;
}

int extent_server_dist::get(extent_protocol::extentid_t id, std::string &buf)
{
    // Lab3: your code here
    chfs_command_raft chfs_cmd;
    chfs_cmd.cmd_tp = chfs_command_raft::CMD_GET;
    chfs_cmd.id = id;
    int log_index, term;
    ASSERT(leader()->new_command(chfs_cmd, term, log_index) == 1, "leader send command failed");
    {
        std::unique_lock<std::mutex> lck(chfs_cmd.res->mtx);
        while (!chfs_cmd.res->done)
        {
            chfs_cmd.res->cv.wait(lck);
        }
    }
    buf = chfs_cmd.res->buf;
    return extent_protocol::OK;
}

int extent_server_dist::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
    // Lab3: your code here
    chfs_command_raft chfs_cmd;
    chfs_cmd.cmd_tp = chfs_command_raft::CMD_GETA;
    chfs_cmd.id = id;
    int log_index, term;
    ASSERT(leader()->new_command(chfs_cmd, term, log_index) == 1, "leader send command failed");
    {
        std::unique_lock<std::mutex> lck(chfs_cmd.res->mtx);
        while (!chfs_cmd.res->done)
        {
            chfs_cmd.res->cv.wait(lck);
        }
    }
    a = chfs_cmd.res->attr;
    return extent_protocol::OK;
}

int extent_server_dist::remove(extent_protocol::extentid_t id, int &)
{
    // Lab3: your code here
    chfs_command_raft chfs_cmd;
    chfs_cmd.cmd_tp = chfs_command_raft::CMD_RMV;
    chfs_cmd.id = id;
    int log_index, term;
    ASSERT(leader()->new_command(chfs_cmd, term, log_index) == 1, "leader remove command failed");
    {
        std::unique_lock<std::mutex> lck(chfs_cmd.res->mtx);
        while (!chfs_cmd.res->done)
        {
            chfs_cmd.res->cv.wait(lck);
        }
    }
    return extent_protocol::OK;
}

extent_server_dist::~extent_server_dist()
{
    delete this->raft_group;
}