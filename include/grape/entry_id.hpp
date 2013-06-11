#ifndef __ENTRY_ID_HPP
#define __ENTRY_ID_HPP

#include <msgpack.hpp>
#include <elliptics/packet.h>

namespace ioremap { namespace grape {

struct entry_id {
    int32_t chunk;
    int32_t pos;

    static entry_id from_dnet_raw_id(const dnet_raw_id *id) {
        return *(entry_id *)(id->id + DNET_ID_SIZE - sizeof(entry_id));
    }

    MSGPACK_DEFINE(chunk, pos);
};

}}

#endif // __ENTRY_ID_HPP