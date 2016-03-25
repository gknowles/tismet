// httpconn.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Tuning parameters
*
***/

const unsigned kDefaultBlockSize = 4096;
const unsigned kDefaultHeaderTableSize = 4096;


/****************************************************************************
*
*   Private declarations
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

struct ResetStream {
    TimePoint closed;
    HttpConnHandle hc;
    int stream;
    shared_ptr<HttpStream> sm;
};

} // namespace


/****************************************************************************
*
*   Variables
*
***/

static HandleMap<HttpConnHandle, HttpConn> s_conns;


/****************************************************************************
*
*   Reset streams
*
***/

const Duration kMaxResetStreamAge = 10s;
const Duration kMinResetStreamCheckInterval = 2s;

class ResetStreamTimer : public ITimerNotify {
    Duration onTimer (TimePoint now) override;
};

static deque<ResetStream> s_resetStreams;

//===========================================================================
Duration ResetStreamTimer::onTimer (TimePoint now) {
    TimePoint expire = now - kMaxResetStreamAge;
    Duration sleep;
    while (!empty(s_resetStreams)) {
        auto & rs = s_resetStreams.front();
        sleep = rs.closed - expire;
        if (sleep > 0s) 
            return min(sleep, kMinResetStreamCheckInterval);

        if (rs.sm->m_state == HttpStream::kClosed) {
            HttpConn * conn = s_conns.find(rs.hc);
            conn->deleteStream(rs.stream, rs.sm.get());
        } else {
            assert(rs.sm->m_state == HttpStream::kDeleted);
        }

        s_resetStreams.pop_front();
    }

    return kTimerInfinite;
}


/****************************************************************************
*
*   HPACK message decoder
*
***/

class MsgDecoder : public IHpackDecodeNotify {
public:
    MsgDecoder (HttpMsg & msg);

    explicit operator bool () const;
    FrameError Error () const;

private:
    void onHpackHeader (
        HttpHdr id,
        const char name[],
        const char value[],
        int flags // Flags::*
    ) override;

    HttpMsg & m_msg;
    FrameError m_error{FrameError::kNoError};
};

//===========================================================================
MsgDecoder::MsgDecoder (HttpMsg & msg) 
    : m_msg(msg)
{}

//===========================================================================
void MsgDecoder::onHpackHeader (
    HttpHdr id,
    const char name[],
    const char value[],
    int flags // Flags::*
) {
    if (id) {
        m_msg.addHeaderRef(id, value);
    } else {
        m_msg.addHeaderRef(name, value);
    }
    // m_error = FrameError::kInternalError;
}

//===========================================================================
MsgDecoder::operator bool () const {
    return m_error == FrameError::kNoError;
}

//===========================================================================
FrameError MsgDecoder::Error () const {
    return m_error;
}


/****************************************************************************
*
*   Connection initialization
*
***/

enum class HttpConn::ByteMode {
    kInvalid,
    kPreface,
    kHeader,
    kPayload,
};

enum class HttpConn::FrameMode {
    kSettings,
    kNormal,
    kContinuation,
};

//===========================================================================
HttpConn::HttpConn () 
    : m_byteMode{ByteMode::kPreface}
    , m_frameMode{FrameMode::kSettings}
    , m_encoder{kDefaultHeaderTableSize}
    , m_decoder{kDefaultHeaderTableSize}
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
    while (ptr != eptr && *literal) {
        if (*ptr++ != *literal++)
            false;
    }
    return true;
}

//===========================================================================
static void SetFrameHeader (
    uint8_t out[kFrameHeaderLen],
    unsigned stream,
    FrameType type,
    unsigned length,
    unsigned flags
) {
    out[0] = (uint8_t) (length >> 16);
    out[1] = (uint8_t) (length >> 8);
    out[2] = (uint8_t) length;
    out[3] = (uint8_t) type;
    out[4] = (uint8_t) flags;
    out[5] = (uint8_t) (stream >> 24);
    out[6] = (uint8_t) (stream >> 16);
    out[7] = (uint8_t) (stream >> 8);
    out[8] = (uint8_t) stream;
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
    SetFrameHeader(buf, stream, type, length, flags);
    out->append((char *) &buf, sizeof(buf));
}

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
    CharBuf * out,
    int lastStream,
    FrameError error
) {
}

//===========================================================================
static void ReplyRstStream (
    CharBuf * out,
    int stream,
    HttpStream * sm,
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
bool HttpConn::recv (
    CharBuf * out,
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    const void * src, 
    size_t srcLen
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
        StartFrame(out, 0, FrameType::kSettings, 0, 0); 
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
                    return onFrame(msgs, out, data(m_input));
                m_input.insert(m_input.end(), ptr, eptr);
                m_byteMode = ByteMode::kPayload;
                return true;
            }
            m_input.insert(m_input.end(), ptr, ptr + m_inputFrameLen);
            ptr += m_inputFrameLen;
            if (!onFrame(msgs, out, data(m_input)))
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
        if (!onFrame(msgs, out, ptr))
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
        if (!onFrame(msgs, out, data(m_input)))
            return false;
        m_input.clear();
        m_byteMode = ByteMode::kHeader;
        goto next_frame;
    };
}

//===========================================================================
HttpStream * HttpConn::findAlways (CharBuf * out, int stream) {
    auto ib = m_streams.emplace(stream, nullptr);
    if (!ib.second)
        return ib.first->second.get();
    if (stream % 2 == 0) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return nullptr;
    }
    if (stream < m_lastInputStream) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return nullptr;
    }

    m_lastInputStream = stream;
    ib.first->second = make_shared<HttpStream>();
    return ib.first->second.get();
}

//===========================================================================
bool HttpConn::onFrame (
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    CharBuf * out,
    const char src[]
) {
    // Frame header
    //  length : 24
    //  type : 8
    //  flags : 8
    //  reserved : 1
    //  stream : 31

    auto * hdr = src;
    FrameType type = GetFrameType(hdr);
    int flags = GetFrameFlags(hdr);
    int stream = GetFrameStream(hdr);
    m_inputFrameLen = GetFrameLen(hdr);
    if (m_inputFrameLen > m_maxInputFrame) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kFrameSizeError);
        return false;
    }

    switch (type) {
    case FrameType::kContinuation: 
        return onContinuation(msgs, out, src, stream, flags);
    case FrameType::kData: 
        return onData(msgs, out, src, stream, flags);
    case FrameType::kGoAway: 
        return onGoAway(msgs, out, src, stream, flags);
    case FrameType::kHeaders: 
        return onHeaders(msgs, out, src, stream, flags);
    case FrameType::kPing: 
        return onPing(msgs, out, src, stream, flags);
    case FrameType::kPriority: 
        return onPriority(msgs, out, src, stream, flags);
    case FrameType::kPushPromise: 
        return onPushPromise(msgs, out, src, stream, flags);
    case FrameType::kRstStream: 
        return onRstStream(msgs, out, src, stream, flags);
    case FrameType::kSettings: 
        return onSettings(msgs, out, src, stream, flags);
    case FrameType::kWindowUpdate: 
        return onWindowUpdate(msgs, out, src, stream, flags);
    };

    // ignore unknown frames unless a specific frame type is required
    if (m_frameMode == FrameMode::kNormal) 
        return true;
    ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
    return false;
}

//===========================================================================
bool HttpConn::onData (
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    CharBuf * out,
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
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    auto it = m_streams.find(stream);
    auto * sm = (it == m_streams.end()) ? it->second.get() : nullptr;
    if (!sm 
        || sm->m_state != HttpStream::kOpen
           && sm->m_state != HttpStream::kLocalClosed
    ) {
        // data frame on non-open stream
        ReplyRstStream(out, stream, sm, FrameError::kStreamClosed);
        return true;
    }

    // adjust for any included padding
    UnpaddedData data;
    if (!RemovePadding(&data, src, m_inputFrameLen, 0, flags)) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    // TODO: flow control

    // TODO: check total buffer size

    CharBuf & buf = sm->m_msg->body();
    buf.append(data.data, data.dataLen);
    if (flags & kEndStream) {
        msgs->push_back(move(sm->m_msg));
    }
    return true;
}

//===========================================================================
bool HttpConn::onHeaders (
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    CharBuf * out,
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
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    // adjust for any included padding
    UnpaddedData ud;
    int hdrLen = (flags & kPriority) ? 5 : 0;
    if (!RemovePadding(&ud, src, m_inputFrameLen, hdrLen, flags)) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    // parse priority
    if (flags & kPriority) {
        PriorityData pri;
        if (!RemovePriority(&pri, ud.hdr, hdrLen)) {
            ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
            return false;
        }
        UpdatePriority();
    }

    // find stream
    HttpStream * sm = findAlways(out, stream);
    if (!sm)
        return false;

    switch (sm->m_state) {
        case HttpStream::kIdle:
            if (~flags & kEndStream) {
                sm->m_state = HttpStream::kOpen;
            } else {
                sm->m_state = HttpStream::kRemoteClosed;
            }
            sm->m_msg = make_unique<HttpRequest>();
            break;
        case HttpStream::kRemoteReserved:
            if (~flags & kEndStream) {
                sm->m_state = HttpStream::kLocalClosed;
            } else {
                sm->m_state = HttpStream::kClosed;
            }
            sm->m_msg = make_unique<HttpRequest>();
            break;
        case HttpStream::kOpen:
        case HttpStream::kLocalClosed:
            if (flags & kEndStream) {
                // trailing headers not supported
                // !!! should probably send a stream error and process
                //     the headers (to maintain the connection decompression 
                //     context) instead of dropping the connection.
                ReplyGoAway(out, stream, FrameError::kProtocolError);
                return false;
            }
            // fall through
        default:
            // !!! should probably send a stream error and process
            //     the headers (to maintain the connection decompression 
            //     context) instead of dropping the connection.
            ReplyGoAway(out, stream, FrameError::kProtocolError);
            return false;
    }

    if (~flags & kEndHeaders)
        m_frameMode = FrameMode::kContinuation;

    auto * msg = sm->m_msg.get();
    MsgDecoder notify(*msg);
    if (!m_decoder.parse(&notify, &msg->heap(), ud.data, ud.dataLen)) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kCompressionError);
        return false;
    }
    if (!notify) {
        ReplyRstStream(out, stream, sm, notify.Error());
    } else {
        if (sm->m_state == HttpStream::kRemoteClosed)
            msgs->push_back(move(sm->m_msg));
    }
    return true;
}

//===========================================================================
bool HttpConn::onPriority (
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    CharBuf * out,
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
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    if (m_inputFrameLen != 5) {
        ReplyRstStream(out, stream, nullptr, FrameError::kFrameSizeError);
        return true;
    }

    PriorityData pri;
    if (!RemovePriority(&pri, src + kFrameHeaderLen, m_inputFrameLen)) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    return true;
}

//===========================================================================
bool HttpConn::onRstStream (
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    CharBuf * out,
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
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    if (m_inputFrameLen != 4) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kFrameSizeError);
        return false;
    }

    // TODO: actually close the stream...

    return true;
}

//===========================================================================
bool HttpConn::onSettings (
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    CharBuf * out,
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
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }
    m_frameMode = FrameMode::kNormal;

    if (flags & kAck) {
        if (m_inputFrameLen) {
            ReplyGoAway(out, m_lastInputStream, FrameError::kFrameSizeError);
            return false;
        }
        return true;
    }

    // must be an even multiple of identifier/value pairs
    if (m_inputFrameLen % 6 != 0) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kFrameSizeError);
        return false;
    }

    for (int pos = 0; pos < m_inputFrameLen; pos += 6) {
        int identifier = ntoh16(src + kFrameHeaderLen + pos);
        int value = ntoh32(src + kFrameHeaderLen + pos + 2);
        (void) identifier;
        (void) value;
    }

    StartFrame(out, 0, FrameType::kSettings, 0, kAck);
    return true;
}

//===========================================================================
bool HttpConn::onPushPromise (
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    CharBuf * out,
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
bool HttpConn::onPing (
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    CharBuf * out,
    const char src[],
    int stream,
    int flags
) {
    // Ping frame
    //  data[8]

    return false;
}

//===========================================================================
bool HttpConn::onGoAway (
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    CharBuf * out,
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
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }
    
    int lastStreamId = ntoh31(src + kFrameHeaderLen);
    FrameError errorCode = (FrameError) ntoh32(src + kFrameHeaderLen + 4);

    if (lastStreamId > m_lastOutputStream) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    m_lastOutputStream = lastStreamId;
    if (errorCode == FrameError::kNoError) {
        if (m_lastOutputStream > m_nextOutputStream) {
            // TODO: report shutdown requested
            return true;
        }
    }

    ReplyGoAway(out, m_lastInputStream, FrameError::kNoError);
    return false;
}

//===========================================================================
bool HttpConn::onWindowUpdate (
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    CharBuf * out,
    const char src[],
    int stream,
    int flags
) {
    // WindowUpdate frame
    //  reserved : 1
    //  increment : 31

    if (m_frameMode != FrameMode::kNormal) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    auto it = m_streams.find(stream);
    if (it == m_streams.end()) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
        return false;
    }

    return false;
}

//===========================================================================
bool HttpConn::onContinuation (
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    CharBuf * out,
    const char src[],
    int stream,
    int flags
) {
    // Continuation frame
    //  headerBlock[]

    if (m_frameMode != FrameMode::kContinuation 
        || stream != m_continueStream
    ) {
        ReplyGoAway(out, m_lastInputStream, FrameError::kProtocolError);
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
int HttpConn::request (
    CharBuf * out,
    const HttpMsg & msg
) {
    return 0;
}

//===========================================================================
// Serializes a push promise
void HttpConn::pushPromise (
    CharBuf * out,
    const HttpMsg & msg
) {
}

//===========================================================================
// Serializes a reply on the specified stream
void HttpConn::reply (
    CharBuf * out,
    int stream,
    const HttpMsg & msg
) {
    auto it = m_streams.find(stream);
    if (it == m_streams.end())
        return;

    HttpStream * strm = it->second.get();
    switch (strm->m_state) {
        case HttpStream::kOpen:
            strm->m_state = HttpStream::kLocalClosed;
            break;
        case HttpStream::kLocalReserved:
        case HttpStream::kRemoteClosed:
            strm->m_state = HttpStream::kClosed;
            break;
        default:
            logMsgCrash() << "httpReply invalid state, {" 
                << strm->m_state << "}";
            return;
    }

    const CharBuf & body = msg.body();
    size_t framePos = out->size();
    size_t maxEndPos = framePos + m_maxOutputFrame + kFrameHeaderLen;
    FrameType ftype = FrameType::kHeaders;
    int flags = size(body) ? 0 : FrameFlag::kEndStream;
    uint8_t frameHdr[kFrameHeaderLen];
    SetFrameHeader(frameHdr, stream, ftype, 0, 0);
    out->append((char *) frameHdr, size(frameHdr));
    m_encoder.startBlock(out);
    for (auto&& hdr : msg) {
        for (auto&& hv : hdr) {
            if (hdr.m_id) {
                m_encoder.header(hdr.m_id, hv.m_value);
            } else {
                m_encoder.header(hdr.m_name, hv.m_value);
            }
            if (size(*out) > maxEndPos) {
                SetFrameHeader(
                    frameHdr, 
                    stream, 
                    ftype,
                    m_maxOutputFrame,
                    flags
                );
                out->replace(framePos, size(frameHdr), (char *) frameHdr);
                ftype = FrameType::kContinuation;
                flags = 0;
                framePos = maxEndPos;
                maxEndPos += m_maxOutputFrame + kFrameHeaderLen;
                out->insert(framePos, (char *) frameHdr, size(frameHdr));
            }
        }
    }
    SetFrameHeader(
        frameHdr,
        stream, 
        ftype,
        int(size(*out) - framePos),
        flags | FrameFlag::kEndHeaders
    );
    out->replace(framePos, size(frameHdr), (char *) frameHdr);
    
    size_t bodyLen = size(body);
    if (!bodyLen)
        return;

    size_t bodyPos = 0;
    while (bodyPos + m_maxOutputFrame < bodyLen) {
        StartFrame(out, stream, FrameType::kData, m_maxOutputFrame, 0);
        out->append(body, bodyPos, m_maxOutputFrame);
        bodyPos += m_maxOutputFrame;
    }
    StartFrame(
        out, 
        stream, 
        FrameType::kData, 
        int(bodyLen - bodyPos), 
        FrameFlag::kEndStream
    );
    out->append(body, bodyPos);
}

//===========================================================================
void HttpConn::resetStream (CharBuf * out, int stream) {
}

//===========================================================================
void HttpConn::deleteStream (int stream, HttpStream * sm) {
    auto it = m_streams.find(stream);
    if (it != m_streams.end() && sm == it->second.get()) {
        sm->m_state = sm->kDeleted;
        m_streams.erase(it);
    }
}


/****************************************************************************
*
*   Public API
*
***/

//===========================================================================
HttpConnHandle httpListen () {
    auto * conn = new HttpConn;
    return s_conns.insert(conn);
}

//===========================================================================
void httpClose (HttpConnHandle hc) {
    s_conns.erase(hc);
}

//===========================================================================
bool httpRecv (
    HttpConnHandle hc,
    CharBuf * out,
    std::vector<std::unique_ptr<HttpMsg>> * msgs, 
    const void * src, 
    size_t srcLen
) {
    if (auto * conn = s_conns.find(hc))
        return conn->recv(out, msgs, src, srcLen);
    return false;
}

//===========================================================================
void httpReply (
    HttpConnHandle hc,
    CharBuf * out,
    int stream,
    const HttpMsg & msg
) {
    auto * conn = s_conns.find(hc);
    conn->reply(out, stream, msg);
}

} // namespace
