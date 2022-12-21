#include "chfs_state_machine.h"
#include <assert.h>

chfs_command_raft::chfs_command_raft()
{
    // Lab3: Your code here
    res = std::make_shared<result>();
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) : cmd_tp(cmd.cmd_tp), type(cmd.type), id(cmd.id), buf(cmd.buf), res(cmd.res)
{
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft()
{
    // Lab3: Your code here
}

int chfs_command_raft::size() const
{
    // Lab3: Your code here
    return (1 + 4 + 8 + 4 + buf.size() + 1);
}

void chfs_command_raft::serialize(char *buf_out, int size) const
{
    // Lab3: Your code here
    if (size < this->size())
    {
        return;
    }
    int bufsize = buf.size() + 1;
    // 1 + 4 + 8 + 4 + bufsize + 1
    memcpy(&buf_out[0], &this->cmd_tp, sizeof(uint32_t));
    buf_out[0] = (char)cmd_tp;
    // type
    for (int i = 0; i < 4; ++i)
    {
        buf_out[i + 1] = (type >> (8 * (3 - i))) & 0xff;
    }
    // id
    for (int i = 0; i < 8; ++i)
    {
        buf_out[i + 5] = (id >> (8 * (8 - i))) & 0xff;
    }
    // bufsize
    for (int i = 0; i < 4; ++i)
    {
        buf_out[i + 13] = (bufsize >> (8 * (3 - i))) & 0xff;
    }
    // string
    strcpy(&buf_out[17], buf.data());
    return;
}

void chfs_command_raft::deserialize(const char *buf_in, int size)
{
    // Lab3: Your code here
    int bufsize = 0;
    type = 0;
    id = 0;
    // 1 + 4 + 8 + 4 + bufsize + 1
    cmd_tp = (command_type)buf_in[0];
    //type
    for (int i = 0; i < 4; ++i) {
        type |= (buf_in[i + 1] & 0xff) << (8 * (3 - i));
    }
    //id
    for (int i = 0; i < 8; ++i) {
        id |= (buf_in[i + 5] & 0xff) << (8 * (8 - i));
    }
    //bufsize
    for (int i = 0; i < 4; ++i) {
        bufsize |= (buf_in[i + 13] & 0xff) << (8 * (3 - i));
    }
    buf.assign(&buf_in[17]);
    return;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd)
{
    // Lab3: Your code here
    m << (char)cmd.cmd_tp << cmd.type << cmd.id << cmd.buf;
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd)
{
    // Lab3: Your code here
    char tp;
    u >> tp >> cmd.type >> cmd.id >> cmd.buf;
    cmd.cmd_tp = (chfs_command_raft::command_type)tp;
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd)
{
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here
    {
        std::unique_lock<std::mutex> lck(mtx);
        switch (chfs_cmd.cmd_tp)
        {
        case chfs_command_raft::command_type::CMD_NONE:
            break;
        case chfs_command_raft::command_type::CMD_GET:
            es.get(chfs_cmd.id, chfs_cmd.res->buf);
            chfs_cmd.res->tp = chfs_command_raft::command_type::CMD_GET;
            break;
        case chfs_command_raft::command_type::CMD_GETA:
            es.getattr(chfs_cmd.id, chfs_cmd.res->attr);
            chfs_cmd.res->tp = chfs_command_raft::command_type::CMD_GETA;
            break;
        case chfs_command_raft::command_type::CMD_CRT:
            es.create(chfs_cmd.type, chfs_cmd.res->id);
            chfs_cmd.res->tp = chfs_command_raft::command_type::CMD_CRT;
            break;
        case chfs_command_raft::command_type::CMD_PUT:
        {
            int size;
            es.put(chfs_cmd.id, chfs_cmd.buf, size);
            chfs_cmd.res->tp = chfs_command_raft::command_type::CMD_PUT;
        }
        break;
        case chfs_command_raft::command_type::CMD_RMV:
        {
            int size;
            es.remove(chfs_cmd.id, size);
            chfs_cmd.res->tp = chfs_command_raft::command_type::CMD_RMV;
        }
        break;
        default:
            break;
        }
    }
    chfs_cmd.res->done = true;
    chfs_cmd.res->cv.notify_all();
    return;
}
