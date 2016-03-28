// tlsrecord.cpp - dim services
#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Private
*
***/

namespace {

const unsigned kMaxPlaintext = 16'384;
const unsigned kMaxCiphertext = kMaxPlaintext + 256;

} // namespace


/****************************************************************************
*
*   Variables
*
***/


/****************************************************************************
*
*   TlsRecordEncrypt
*
***/

//===========================================================================
void TlsRecordEncrypt::setCipher (CharBuf * out, TlsCipher * cipher) {
    assert(!m_cipher && cipher);
    flush(out);
    m_cipher = cipher;
}

//===========================================================================
void TlsRecordEncrypt::writePlaintext (CharBuf * out) {
    size_t plainLen = m_plaintext.size();
    assert(plainLen <= kMaxPlaintext);
    char hdr[] = { 
        char(m_plainType), 
        3, 
        1, 
        char(plainLen / 256), 
        char(plainLen % 256) 
    };
    out->append(hdr, size(hdr));
    out->append((char *) m_plaintext.data(), plainLen);
    m_plaintext.clear();
}

//===========================================================================
void TlsRecordEncrypt::writeCiphertext (CharBuf * out) {
    char hdr[] = { kContentAppData, 3, 1, 0, 0 };
    size_t pos = out->size();
    out->append(hdr, size(hdr));

    m_plaintext.push_back((TlsContentType) m_plainType);
    m_cipher->add(out, m_plaintext.data(), m_plaintext.size());
    m_plaintext.clear();
    size_t cipherLen = size(*out) - pos - size(hdr);
    assert(cipherLen <= kMaxCiphertext);
    hdr[3] = uint8_t(cipherLen / 256);
    hdr[4] = uint8_t(cipherLen % 256);
    out->replace(pos + 3, 2, hdr + 3);
}

//===========================================================================
void TlsRecordEncrypt::add (
    CharBuf * out, 
    TlsContentType ct, 
    const void * vptr, 
    size_t count
) {
    // MUST NOT send zero-length fragments of Handshake or Alert types
    assert(ct != kContentAlert && ct != kContentHandshake || count);
    assert(count <= kMaxPlaintext);

    auto ptr = static_cast<const char *>(vptr);

    if (m_plainType != ct || ct == kContentAlert) {
        m_plainType = ct;
        goto start_text;
    }
    for (;;) {
        size_t num = min(count, size(m_plaintext) - kMaxPlaintext);
        m_plaintext.insert(m_plaintext.end(), ptr, ptr + num);
        ptr += num;
        count -= num;
        if (!count)
            break;

    start_text:
        flush(out);
    }
}

//===========================================================================
void TlsRecordEncrypt::flush (CharBuf * out) {
    if (size(m_plaintext)) {
        if (!m_cipher) {
            writePlaintext(out);
        } else {
            writeCiphertext(out);
        }
    }
}


/****************************************************************************
*
*   TlsRecordDecrypt
*
***/

//===========================================================================
void TlsRecordDecrypt::setCipher (TlsCipher * cipher) {
    assert(!m_cipher && cipher);
    m_cipher = cipher; 
}

//===========================================================================
bool TlsRecordDecrypt::parse (
    CharBuf * data,
    ITlsRecordDecryptNotify * notify,
    const void * vsrc, 
    size_t srcLen
) {
    auto base = (const uint8_t *) vsrc;
    auto ptr = base;
    auto eptr = ptr + srcLen;

    for (;;) {
        switch (m_recPos) {
        case 0: // content type
            if (ptr == eptr)
                return true;
            m_curType = *ptr++;
            m_recPos += 1;

            switch (m_curType) {
            case kContentAlert:
            case kContentHandshake:
                // When there's a cipher, the outer opaque_type is always
                // application data. The actual type is in the ciphertext.
                if (m_cipher)
                    return parseError(kUnexpectedMessage);
            case kContentAppData:
                break;
            default:
                return parseError(kUnexpectedMessage);
            }
        case 1: // protocol version major - MUST ignore
            if (ptr == eptr)
                return true;
            ptr += 1;
            m_recPos += 1;
        case 2: // protocol version minor - MUST ignore
            if (ptr == eptr)
                return true;
            ptr += 1;
            m_recPos += 1;
        case 3: // text length MSB
            if (ptr == eptr)
                return true;
            m_textLen = *ptr++;
            m_recPos += 1;
        case 4: // text length LSB
            if (ptr == eptr)
                return true;
            m_textLen = (m_textLen << 8) + *ptr++;
            m_recPos += 1;
            if (m_textLen > (m_cipher ? kMaxCiphertext : kMaxPlaintext))
                return parseError(kRecordOverflow);
        }

        srcLen = eptr - ptr;
        size_t textNeeded = m_textLen - m_recPos - 5;

        if (m_cipher) {
            if (textNeeded > srcLen) {
                m_ciphertext.insert(m_ciphertext.end(), ptr, eptr);
                m_recPos += (unsigned) srcLen;
                return true;
            }

            m_ciphertext.insert(m_ciphertext.end(), ptr, ptr + textNeeded);
            m_cipher->add(&m_plaintext, m_ciphertext.data(), m_ciphertext.size());
            m_ciphertext.clear();
            m_plaintext.rtrim(0);
            // no content type treated as unexpected_message (5.2.3)
            if (!m_plaintext.size())
                return parseError(kUnexpectedMessage);
            m_curType = m_plaintext.back();
            m_plaintext.popBack();
            if (m_textLen > kMaxPlaintext)
                return parseError(kRecordOverflow);
        } else {
            if (textNeeded > srcLen) {
                m_plaintext.append((char *) ptr, srcLen);
                m_recPos += (unsigned) srcLen;
                return true;
            }

            m_plaintext.append((char *) ptr, textNeeded);
        }

        switch (m_curType) {
        case kContentAlert:
            if (!parseAlerts(notify))
                return false;
            break;
        case kContentHandshake:
            if (!parseHandshakes(notify))
                return false;
            break;
        case kContentAppData:
            data->append(m_plaintext);
            break;
        default:
            return parseError(kUnexpectedMessage);
        }

        m_plaintext.clear();
        ptr += textNeeded;
        m_recPos = 0;
    }
}

//===========================================================================
bool TlsRecordDecrypt::parseAlerts (ITlsRecordDecryptNotify * notify) {
    int num = (int) m_plaintext.size();
    if (!num)
        return parseError(kUnexpectedMessage);

    const char * ptr = m_plaintext.data();
    const char * eptr = ptr + num / 2 * 2;
    while (ptr != eptr) {
        auto level = TlsAlertLevel(*ptr++);
        auto desc = TlsAlertDesc(*ptr++);
        notify->onTlsAlert(desc, level);
    }
    if (num % 2 == 1)
        return parseError(kUnexpectedMessage);
    return true;
}

//===========================================================================
bool TlsRecordDecrypt::parseHandshakes (ITlsRecordDecryptNotify * notify) {
    int pos = 0;
    int epos = (int) m_plaintext.size();
    while (pos + 4 <= epos) {
        const char * ptr = m_plaintext.data(pos, 4);
        int recLen = (ptr[1] << 16) + (ptr[2] << 8) + ptr[3];
        if (pos + recLen > epos)
            break;
        notify->onTlsHandshake(
            TlsHandshakeType(*ptr),
            (uint8_t *) m_plaintext.data(pos + 4, recLen), 
            recLen
        );
        pos += recLen;
    }
    return true;
}

//===========================================================================
bool TlsRecordDecrypt::parseError (
    TlsAlertDesc desc, 
    TlsAlertLevel level
) {
    m_alertLevel = level;
    m_alertDesc = desc;
    return false;
}


/****************************************************************************
*
*   Public API
*
***/

} // namespace
