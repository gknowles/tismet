// http.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;


/****************************************************************************
*
*   Tuning parameters
*
***/

const unsigned kDefaultBlockSize = 4096;
const unsigned kDefaultHeaderTableSize = 4096;


/****************************************************************************
*
*   Declarations
*
***/

namespace {

enum class FrameType : uint8_t {
    kData = 0,
    kHeaders = 1,
    kPriority = 2,
    kRstStream = 3,
    kSettings = 4,
    kPushPromise = 5,
    kPing = 6,
    kGoAway = 7,
    kWindowUpdate = 8,
    kContinuation = 9,
};

enum FrameParam : uint16_t {
    kSettingsHeaderTableSize = 1,
    kSettingsEnablePush = 2,
    kSettingsMaxConcurrentStreams = 3,
    kSettingsInitialWindowSize = 4,
    kSettingsMaxFrameSize = 5,
    kSettingsMaxHeaderListSize = 6,
};

enum FrameFlag : uint8_t {
    kNone = 0x00,
    kAck = 0x01,
    kEndStream = 0x01,
    kEndHeaders = 0x04,
    kPadded = 0x08,
    kPriority = 0x20,
};

enum class FrameError {
    kNoError = 0,
    kProtocolError = 1,
    kInternalError = 2,
    kFlowControlError = 3,
    kSettingsTimeout = 4,
    kStreamClosed = 5,
    kFrameSizeError = 6,
    kRefusedStream = 7,
    kCancel = 8,
    kCompressionError = 9,
    kConnectError = 10,
    kEnhanceYourCalm = 11,
    kInadequateSecurity = 12,
    kHttp11Required = 13,
};

struct PriorityData {
    int stream;
    int weight;
    bool exclusive;
};
struct UnpaddedData {
    const char * hdr;
    const char * data;
    int dataLen;
    int padLen;
};

} // namespace


/****************************************************************************
*
*   Context initialization
*
***/

enum class DimHttpConn::ByteMode {
    kInvalid,
    kPreface,
    kHeader,
    kPayload,
};

enum class DimHttpConn::FrameMode {
    kSettings,
    kNormal,
    kContinuation,
};

//===========================================================================
DimHttpConn::DimHttpConn () 
    : m_byteMode{ByteMode::kPreface}
    , m_frameMode{FrameMode::kSettings}
{}


/****************************************************************************
*
*   Receiving data
*
***/

const int kFrameHeaderLen = 9;

const char kPrefaceData[] = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

//===========================================================================
static bool Skip (
    const char *& ptr, 
    const char * eptr, 
    const char literal[]
) {
    while (ptr != eptr || *literal) {
        if (*ptr++ != *literal++)
            return false;
    }
    return true;
}

//===========================================================================
static void StartFrame (
    CharBuf * out,
    unsigned stream,
    FrameType type,
    unsigned length,
    unsigned flags
) {
    uint8_t buf[kFrameHeaderLen];
    buf[0] = (uint8_t) (length >> 16);
    buf[1] = (uint8_t) (length >> 8);
    buf[2] = (uint8_t) length;
    buf[3] = (uint8_t) type;
    buf[4] = (uint8_t) flags;
    buf[5] = (uint8_t) (stream >> 24);
    buf[6] = (uint8_t) (stream >> 16);
    buf[7] = (uint8_t) (stream >> 8);
    buf[8] = (uint8_t) stream;
    out->Append((char *) &buf, sizeof(buf));
}

////===========================================================================
//static void GetFrame (FrameHeader * out, const char buf[kFrameHeaderLen]) {
//    out->length = (uint8_t) (buf[0] << 16) 
//        + (uint8_t) (buf[1] << 8)
//        + (uint8_t) buf[2];
//    out->type = (FrameType) buf[3];
//    out->flags = buf[4];
//    out->stream = (uint8_t) (buf[5] << 24)
//        + (uint8_t) (buf[6] << 16)
//        + (uint8_t) (buf[7] << 8)
//        + (uint8_t) buf[8];
//}

//===========================================================================
static int ntoh16 (const char frame[]) {
    return (uint8_t) (frame[0] << 8)
        + (uint8_t) frame[1];
}

//===========================================================================
static int ntoh24 (const char frame[]) {
    return (uint8_t) (frame[0] << 16)
        + (uint8_t) (frame[1] << 8)
        + (uint8_t) frame[2];
}

//===========================================================================
static int ntoh32 (const char frame[]) {
    return (uint8_t) (frame[0] << 24)
        + (uint8_t) (frame[1] << 16)
        + (uint8_t) (frame[2] << 8)
        + (uint8_t) frame[3];
}

//===========================================================================
static int ntoh31 (const char frame[]) {
    return ntoh32(frame) & 0x7fffffff;
}

//===========================================================================
static int GetFrameLen (const char frame[kFrameHeaderLen]) {
    return ntoh24(frame);
}

//===========================================================================
static FrameType GetFrameType (const char frame[kFrameHeaderLen]) {
    return (FrameType) frame[3];
}

//===========================================================================
static int GetFrameFlags (const char frame[kFrameHeaderLen]) {
    return frame[4];
}

//===========================================================================
static int GetFrameStream (const char frame[kFrameHeaderLen]) {
    return ntoh31(frame + 5);
}

//===========================================================================
static void ReplyGoAway (
    CharBuf * reply,
    int lastStream,
    FrameError error
) {
}

//===========================================================================
static void ReplyRstStream (
    CharBuf * reply,
    int stream,
    FrameError error
) {
}

//===========================================================================
static bool RemovePadding (
    UnpaddedData * out,
    const char src[],
    int frameLen,
    int hdrLen,
    int flags
) {
    out->hdr = src + kFrameHeaderLen;
    out->data = out->hdr + hdrLen;
    out->dataLen = frameLen - hdrLen;
    if (~flags & kPadded) {
        out->padLen = 0;
        return true;
    }

    if (out->dataLen) {
        out->padLen = *out->data;
        out->dataLen -= out->padLen + 1;
    }
    if (out->dataLen <= 0) 
        return false;
    out->hdr += 1;
    out->data += 1;

    // verify that the padding is zero filled
    const char * ptr = out->data + out->dataLen;
    const char * term = ptr + out->padLen;
    for (; ptr != term; ++ptr) {
        if (*ptr) 
            return false;
    }

    return true;
}

//===========================================================================
static bool RemovePriority (
    PriorityData * out, 
    const char hdr[],
    int hdrLen
) {
    if (hdrLen < 5)
        return false;
    unsigned tmp = ntoh32(hdr);
    out->exclusive = (tmp & 0x80000000) != 0;
    out->stream = tmp & 0x7fffffff;
    if (!out->stream)
        return false;
    out->weight = (unsigned) hdr[4] + 1;
    return true;
}

//===========================================================================
static void UpdatePriority () {
}

//===========================================================================
bool DimHttpConn::Recv (
    std::list<std::unique_ptr<DimHttpMsg>> * msg, 
    CharBuf * reply,
    const void * src, 
    int srcLen
) {
    const char * ptr = static_cast<const char *>(src);
    const char * eptr = ptr + srcLen;
    size_t avail;
    switch (m_byteMode) {
    case ByteMode::kPreface:
        if (!Skip(ptr, eptr, kPrefaceData + size(m_input))) 
            return false;
        if (ptr == eptr) {
            m_input.insert(m_input.end(), ptr - srcLen, eptr);
            return true;
        }
        StartFrame(reply, 0, FrameType::kSettings, 0, 0); 
        m_input.clear();
        m_byteMode = ByteMode::kHeader;
        // fall through

    case ByteMode::kHeader:
    next_frame:
        avail = eptr - ptr;
        if (!avail)
            return true;
        if (size_t used = size(m_input)) {
            size_t need = kFrameHeaderLen - used;
            if (avail < need) {
                m_input.insert(m_input.end(), ptr, eptr);
                return true;
            }
            m_input.insert(m_input.end(), ptr, ptr + need);
            ptr += need;
            avail -= need;
            m_inputFrameLen = GetFrameLen(data(m_input));
            if (avail < m_inputFrameLen) {
                if (m_inputFrameLen > m_maxInputFrame)
                    return OnFrame(msg, reply, data(m_input));
                m_input.insert(m_input.end(), ptr, eptr);
                m_byteMode = ByteMode::kPayload;
                return true;
            }
            m_input.insert(m_input.end(), ptr, ptr + m_inputFrameLen);
            ptr += m_inputFrameLen;
            if (!OnFrame(msg, reply, data(m_input)))
                return false;
            m_input.clear();
            goto next_frame;
        }

        if (avail < kFrameHeaderLen) {
            m_input.assign(ptr, eptr);
            return true;
        }
        avail -= kFrameHeaderLen;
        m_inputFrameLen = GetFrameLen(ptr);
        if (avail < m_inputFrameLen && m_inputFrameLen <= m_maxInputFrame) {
            m_input.assign(ptr, eptr);
            return true;
        }
        if (!OnFrame(msg, reply, ptr))
            return false;
        ptr += kFrameHeaderLen + m_inputFrameLen;
        goto next_frame;

    default:
        assert(m_byteMode == ByteMode::kPayload);
        // fall through

    case ByteMode::kPayload:
        avail = eptr - ptr;
        size_t used = size(m_input);
        size_t need = kFrameHeaderLen + m_inputFrameLen - used;
        if (avail < need) {
            m_input.insert(m_input.end(), ptr, eptr);
            return true;
        }
        m_input.insert(m_input.end(), ptr, ptr + need);
        ptr += need;
        if (!OnFrame(msg, reply, data(m_input)))
            return false;
        m_input.clear();
        m_byteMode = ByteMode::kHeader;
        goto next_frame;
    };
}

//===========================================================================
bool DimHttpConn::OnFrame (
    std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
    CharBuf * reply,
    const char src[]
) {
    // Frame header
    //  length : 24
    //  type : 8
    //  flags : 8
    //  reserved : 1
    //  stream : 31

    auto & hdr = (const char (&)[kFrameHeaderLen]) src;
    FrameType type = GetFrameType(hdr);
    int flags = GetFrameFlags(hdr);
    int stream = GetFrameStream(hdr);
    m_inputFrameLen = GetFrameLen(hdr);
    if (m_inputFrameLen > m_maxInputFrame) {
        ReplyGoAway(reply, m_lastInputStream, FrameError::kFrameSizeError);
        return false;
    }

    switch (type) {
    case FrameType::kContinuation: 
        return OnContinuation(msgs, reply, src, stream, flags);
    case FrameType::kData: 
        return OnData(msgs, reply, src, stream, flags);
    case FrameType::kGoAway: 
        return OnGoAway(msgs, reply, src, stream, flags);
    case FrameType::kHeaders: 
        return OnHeaders(msgs, reply, src, stream, flags);
    case FrameType::kPing: 
        return OnPing(msgs, reply, src, stream, flags);
    case FrameType::kPriority: 
        return OnPriority(msgs, reply, src, stream, flags);
    case FrameType::kPushPromise: 
        return OnPushPromise(msgs, reply, src, stream, flags);
    case FrameType::kRstStream: 
        return OnRstStream(msgs, reply, src, stream, flags);
    case FrameType::kSettings: 
        return OnSettings(msgs, reply, src, stream, flags);
    case FrameType::kWindowUpdate: 
        return OnWindowUpdate(msgs, reply, src, stream, flags);
    };

    // ignore unknown frames unless a specific frame type is required
    if (m_frameMode == FrameMode::kNormal) 
        return true;
    ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
    return false;
}

//===========================================================================
bool DimHttpConn::OnData (
    std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
    CharBuf * reply,
    const char src[],
    int stream,
    int flags
) {
    // Data frame
    //  if PADDED
    //      padLen : 8
    //  data[]
    //  padding[]

    if (m_frameMode != FrameMode::kNormal
        || !stream
    ) {
        // data frames aren't allowed on stream 0
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    auto it = m_streams.find(stream);
    if (it == m_streams.end()) {
        // data frame on non-open stream
        if (m_closedStreams.insert(stream).second)
            ReplyRstStream(reply, stream, FrameError::kStreamClosed);
        return true;
    }

    if (!it->second.m_msg) {
        // data frame on half closed stream, force to closed
        m_streams.erase(it);
        m_closedStreams.insert(stream);
        ReplyRstStream(reply, stream, FrameError::kStreamClosed);
        return true;
    }

    // adjust for any included padding
    UnpaddedData data;
    if (!RemovePadding(&data, src, m_inputFrameLen, 0, flags)) {
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    // TODO: flow control

    // TODO: check total buffer size

    CharBuf * buf = it->second.m_msg->Data();
    buf->Append(data.data, data.dataLen);
    if (flags & kEndStream) {
        msgs->push_back(move(it->second.m_msg));
    }
    return true;
}

//===========================================================================
bool DimHttpConn::OnHeaders (
    std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
    CharBuf * reply,
    const char src[],
    int stream,
    int flags
) {
    // Headers frame
    //  if PADDED flag
    //      padLen : 8
    //  if PRIORITY flag
    //      exclusive dependency : 1
    //      stream dependency : 31
    //      weight : 8
    //  headerBlock[]
    //  padding[]

    if (m_frameMode != FrameMode::kNormal
        || !stream
    ) {
        // header frames aren't allowed on stream 0
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    // adjust for any included padding
    UnpaddedData data;
    int hdrLen = (flags & kPriority) ? 5 : 0;
    if (!RemovePadding(&data, src, m_inputFrameLen, hdrLen, flags)) {
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    // parse priority
    if (flags & kPriority) {
        PriorityData pri;
        if (!RemovePriority(&pri, data.hdr, hdrLen)) {
            ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
            return false;
        }
        UpdatePriority();
    }

    // if (!m_hpack.Decode(ud.data, ud.dataLen)) {
    //     ReplyGoAway
    // }
    return true;
}

//===========================================================================
bool DimHttpConn::OnPriority (
    std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
    CharBuf * reply,
    const char src[],
    int stream,
    int flags
) {
    // Priority frame
    //  exclusive dependency : 1
    //  stream dependency : 31
    //  weight : 8

    if (m_frameMode != FrameMode::kNormal
        || !stream
    ) {
        // priority frames aren't allowed on stream 0
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    if (m_inputFrameLen != 5) {
        ReplyRstStream(reply, stream, FrameError::kFrameSizeError);
        return true;
    }

    PriorityData pri;
    if (!RemovePriority(&pri, src + kFrameHeaderLen, m_inputFrameLen)) {
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    return true;
}

//===========================================================================
bool DimHttpConn::OnRstStream (
    std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
    CharBuf * reply,
    const char src[],
    int stream,
    int flags
) {
    // RstStream frame
    //  errorCode : 32

    if (m_frameMode != FrameMode::kNormal
        || !stream
    ) {
        // priority frames aren't allowed on stream 0
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    if (m_inputFrameLen != 4) {
        ReplyGoAway(reply, m_lastInputStream, FrameError::kFrameSizeError);
        return false;
    }

    // TODO: actually close the stream...

    return true;
}

//===========================================================================
bool DimHttpConn::OnSettings (
    std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
    CharBuf * reply,
    const char src[],
    int stream,
    int flags
) {
    // Settings frame
    //  array of 0 or more
    //      identifier : 16
    //      value : 32

    if (m_frameMode != FrameMode::kNormal 
        && m_frameMode != FrameMode::kSettings
        || stream
    ) {
        // settings frames MUST be on stream 0
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    if (flags & kAck) {
        if (m_inputFrameLen) {
            ReplyGoAway(reply, m_lastInputStream, FrameError::kFrameSizeError);
            return false;
        }
        return true;
    }

    // must be an even multiple of identifier/value pairs
    if (m_inputFrameLen % 6 != 0) {
        ReplyGoAway(reply, m_lastInputStream, FrameError::kFrameSizeError);
        return false;
    }

    for (int pos = 0; pos < m_inputFrameLen; pos += 6) {
        int identifier = ntoh16(src + kFrameHeaderLen + pos);
        int value = ntoh32(src + kFrameHeaderLen + pos + 2);
        (void) identifier;
        (void) value;
    }

    return false;
}

//===========================================================================
bool DimHttpConn::OnPushPromise (
    std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
    CharBuf * reply,
    const char src[],
    int stream,
    int flags
) {
    // PushPromise frame
    //  if PADDED flag
    //      padLen : 8
    //  reserved : 1
    //  stream : 31
    //  headerBlock[]
    //  padding[]

    return false;
}

//===========================================================================
bool DimHttpConn::OnPing (
    std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
    CharBuf * reply,
    const char src[],
    int stream,
    int flags
) {
    // Ping frame
    //  data[8]

    return false;
}

//===========================================================================
bool DimHttpConn::OnGoAway (
    std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
    CharBuf * reply,
    const char src[],
    int stream,
    int flags
) {
    // GoAway frame
    //  reserved : 1
    //  lastStreamId : 31
    //  errorCode : 32
    //  data[]

    if (!stream) {
        // goaway frames aren't allowed on stream 0
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }
    
    int lastStreamId = ntoh31(src + kFrameHeaderLen);
    FrameError errorCode = (FrameError) ntoh32(src + kFrameHeaderLen + 4);

    if (lastStreamId > m_lastOutputStream) {
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    m_lastOutputStream = lastStreamId;
    if (errorCode == FrameError::kNoError) {
        if (m_lastOutputStream > m_nextOutputStream) {
            // TODO: report shutdown requested
            return true;
        }
    }

    ReplyGoAway(reply, m_lastInputStream, FrameError::kNoError);
    return false;
}

//===========================================================================
bool DimHttpConn::OnWindowUpdate (
    std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
    CharBuf * reply,
    const char src[],
    int stream,
    int flags
) {
    // WindowUpdate frame
    //  reserved : 1
    //  increment : 31

    if (m_frameMode != FrameMode::kNormal) {
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    auto it = m_streams.find(stream);
    if (it == m_streams.end()) {
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    return false;
}

//===========================================================================
bool DimHttpConn::OnContinuation (
    std::list<std::unique_ptr<DimHttpMsg>> * msgs, 
    CharBuf * reply,
    const char src[],
    int stream,
    int flags
) {
    // Continuation frame
    //  headerBlock[]

    if (m_frameMode != FrameMode::kContinuation 
        || stream != m_continueStream
    ) {
        ReplyGoAway(reply, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    
    return false;
}


/****************************************************************************
*
*   Sending data
*
***/

//===========================================================================
// Serializes a request and returns the stream id used
int DimHttpConn::Request (
    CharBuf * out,
    std::unique_ptr<DimHttpMsg> msg
) {
    return 0;
}

//===========================================================================
// Serializes a push promise
void DimHttpConn::PushPromise (
    CharBuf * out,
    std::unique_ptr<DimHttpMsg> msg
) {
}

//===========================================================================
// Serializes a reply on the specified stream
void DimHttpConn::Reply (
    CharBuf * out,
    int stream,
    std::unique_ptr<DimHttpMsg> msg
) {
}

//===========================================================================
void DimHttpConn::ResetStream (CharBuf * out, int stream) {
}
