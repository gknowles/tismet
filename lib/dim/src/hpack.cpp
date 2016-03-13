// hpack.cpp - dim services
//
// TODO:
//  * padding strictly longer than 7 bits MUST be treated as a decoding 
//    error
//  * padding not corresponding to the most significant bits of the code for 
//    the EOS symbol MUST be treated as a decoding error

#include "pch.h"
#pragma hdrstop

using namespace std;

namespace Dim {


/****************************************************************************
*
*   Tuning parameters
*
***/


/****************************************************************************
*
*   Declarations
*
***/

struct HpackFieldView {
    const char * name;
    const char * value;
};

//===========================================================================
namespace {

struct EncodeItem {
    uint32_t code;
    int bits;
};

const int16_t kDecodeUnused = numeric_limits<int16_t>::max();
struct DecodeItem {
    int16_t zero{kDecodeUnused};
    int16_t one{kDecodeUnused};
};
class HuffDecoder {
public:
    HuffDecoder (const EncodeItem items[], size_t count);

    bool decode (
        const char ** out, 
        ITempHeap * heap, 
        size_t unusedBits,
        const char src[],
        size_t count
    );

private:
    int m_prefixBits{0};
    vector<DecodeItem> m_decodeTable;
};

} // namespace


/****************************************************************************
*
*   Contants
*
***/

// static table has the following 61 members (not counting 0) as defined by 
// rfc7541 appendix A
const HpackFieldView s_staticTable[] = {
    {},
    { ":authority", "" },
    { ":method", "GET" },
    { ":method", "POST" },
    { ":path", "/" },
    { ":path", "/index.html" },
    { ":scheme", "http" },
    { ":scheme", "https" },
    { ":status", "200" },
    { ":status", "204" },
    { ":status", "206" },
    { ":status", "304" },
    { ":status", "400" },
    { ":status", "404" },
    { ":status", "500" },
    { "accept-charset", "" },
    { "accept-encoding", "gzip, deflate" },
    { "accept-language", "" },
    { "accept-ranges", "" },
    { "accept", "" },
    { "access-control-allow-origin", "" },
    { "age", "" },
    { "allow", "" },
    { "authorization", "" },
    { "cache-control", "" },
    { "content-disposition", "" },
    { "content-encoding", "" },
    { "content-language", "" },
    { "content-length", "" },
    { "content-location", "" },
    { "content-range", "" },
    { "content-type", "" },
    { "cookie", "" },
    { "date", "" },
    { "etag", "" },
    { "expect", "" },
    { "expires", "" },
    { "from", "" },
    { "host", "" },
    { "if-match", "" },
    { "if-modified-since", "" },
    { "if-none-match", "" },
    { "if-range", "" },
    { "if-unmodified-since", "" },
    { "last-modified", "" },
    { "link", "" },
    { "location", "" },
    { "max-forwards", "" },
    { "proxy-authenticate", "" },
    { "proxy-authorization", "" },
    { "range", "" },
    { "referer", "" },
    { "refresh", "" },
    { "retry-after", "" },
    { "server", "" },
    { "set-cookie", "" },
    { "strict-transport-security", "" },
    { "transfer-encoding", "" },
    { "user-agent", "" },
    { "vary", "" },
    { "via", "" },
    { "www-authenticate", "" },
};
static_assert(size(s_staticTable) == 62, "");

// huffman encoding as defined by rfc7541 appendix B
const EncodeItem s_encodeTable[] = {
    {     0x1ff8, 13 }, //     (  0) |11111111|11000
    {   0x7fffd8, 23 }, //     (  1) |11111111|11111111|1011000
    {  0xfffffe2, 28 }, //     (  2) |11111111|11111111|11111110|0010
    {  0xfffffe3, 28 }, //     (  3) |11111111|11111111|11111110|0011
    {  0xfffffe4, 28 }, //     (  4) |11111111|11111111|11111110|0100
    {  0xfffffe5, 28 }, //     (  5) |11111111|11111111|11111110|0101
    {  0xfffffe6, 28 }, //     (  6) |11111111|11111111|11111110|0110
    {  0xfffffe7, 28 }, //     (  7) |11111111|11111111|11111110|0111
    {  0xfffffe8, 28 }, //     (  8) |11111111|11111111|11111110|1000
    {   0xffffea, 24 }, //     (  9) |11111111|11111111|11101010
    { 0x3ffffffc, 30 }, //     ( 10) |11111111|11111111|11111111|111100
    {  0xfffffe9, 28 }, //     ( 11) |11111111|11111111|11111110|1001
    {  0xfffffea, 28 }, //     ( 12) |11111111|11111111|11111110|1010
    { 0x3ffffffd, 30 }, //     ( 13) |11111111|11111111|11111111|111101
    {  0xfffffeb, 28 }, //     ( 14) |11111111|11111111|11111110|1011
    {  0xfffffec, 28 }, //     ( 15) |11111111|11111111|11111110|1100
    {  0xfffffed, 28 }, //     ( 16) |11111111|11111111|11111110|1101
    {  0xfffffee, 28 }, //     ( 17) |11111111|11111111|11111110|1110
    {  0xfffffef, 28 }, //     ( 18) |11111111|11111111|11111110|1111
    {  0xffffff0, 28 }, //     ( 19) |11111111|11111111|11111111|0000
    {  0xffffff1, 28 }, //     ( 20) |11111111|11111111|11111111|0001
    {  0xffffff2, 28 }, //     ( 21) |11111111|11111111|11111111|0010
    { 0x3ffffffe, 30 }, //     ( 22) |11111111|11111111|11111111|111110
    {  0xffffff3, 28 }, //     ( 23) |11111111|11111111|11111111|0011
    {  0xffffff4, 28 }, //     ( 24) |11111111|11111111|11111111|0100
    {  0xffffff5, 28 }, //     ( 25) |11111111|11111111|11111111|0101
    {  0xffffff6, 28 }, //     ( 26) |11111111|11111111|11111111|0110
    {  0xffffff7, 28 }, //     ( 27) |11111111|11111111|11111111|0111
    {  0xffffff8, 28 }, //     ( 28) |11111111|11111111|11111111|1000
    {  0xffffff9, 28 }, //     ( 29) |11111111|11111111|11111111|1001
    {  0xffffffa, 28 }, //     ( 30) |11111111|11111111|11111111|1010
    {  0xffffffb, 28 }, //     ( 31) |11111111|11111111|11111111|1011
    {       0x14,  6 }, // ' ' ( 32) |010100
    {      0x3f8, 10 }, // '!' ( 33) |11111110|00
    {      0x3f9, 10 }, // '"' ( 34) |11111110|01
    {      0xffa, 12 }, // '#' ( 35) |11111111|1010
    {     0x1ff9, 13 }, // '$' ( 36) |11111111|11001
    {       0x15,  6 }, // '%' ( 37) |010101
    {       0xf8,  8 }, // '&' ( 38) |11111000
    {      0x7fa, 11 }, // ''' ( 39) |11111111|010
    {      0x3fa, 10 }, // '(' ( 40) |11111110|10
    {      0x3fb, 10 }, // ')' ( 41) |11111110|11
    {       0xf9,  8 }, // '*' ( 42) |11111001
    {      0x7fb, 11 }, // '+' ( 43) |11111111|011
    {       0xfa,  8 }, // ',' ( 44) |11111010
    {       0x16,  6 }, // '-' ( 45) |010110
    {       0x17,  6 }, // '.' ( 46) |010111
    {       0x18,  6 }, // '/' ( 47) |011000
    {        0x0,  5 }, // '0' ( 48) |00000
    {        0x1,  5 }, // '1' ( 49) |00001
    {        0x2,  5 }, // '2' ( 50) |00010
    {       0x19,  6 }, // '3' ( 51) |011001
    {       0x1a,  6 }, // '4' ( 52) |011010
    {       0x1b,  6 }, // '5' ( 53) |011011
    {       0x1c,  6 }, // '6' ( 54) |011100
    {       0x1d,  6 }, // '7' ( 55) |011101
    {       0x1e,  6 }, // '8' ( 56) |011110
    {       0x1f,  6 }, // '9' ( 57) |011111
    {       0x5c,  7 }, // ':' ( 58) |1011100
    {       0xfb,  8 }, // ';' ( 59) |11111011
    {     0x7ffc, 15 }, // '<' ( 60) |11111111|1111100
    {       0x20,  6 }, // '=' ( 61) |100000
    {      0xffb, 12 }, // '>' ( 62) |11111111|1011
    {      0x3fc, 10 }, // '?' ( 63) |11111111|00
    {     0x1ffa, 13 }, // '@' ( 64) |11111111|11010
    {       0x21,  6 }, // 'A' ( 65) |100001
    {       0x5d,  7 }, // 'B' ( 66) |1011101
    {       0x5e,  7 }, // 'C' ( 67) |1011110
    {       0x5f,  7 }, // 'D' ( 68) |1011111
    {       0x60,  7 }, // 'E' ( 69) |1100000
    {       0x61,  7 }, // 'F' ( 70) |1100001
    {       0x62,  7 }, // 'G' ( 71) |1100010
    {       0x63,  7 }, // 'H' ( 72) |1100011
    {       0x64,  7 }, // 'I' ( 73) |1100100
    {       0x65,  7 }, // 'J' ( 74) |1100101
    {       0x66,  7 }, // 'K' ( 75) |1100110
    {       0x67,  7 }, // 'L' ( 76) |1100111
    {       0x68,  7 }, // 'M' ( 77) |1101000
    {       0x69,  7 }, // 'N' ( 78) |1101001
    {       0x6a,  7 }, // 'O' ( 79) |1101010
    {       0x6b,  7 }, // 'P' ( 80) |1101011
    {       0x6c,  7 }, // 'Q' ( 81) |1101100
    {       0x6d,  7 }, // 'R' ( 82) |1101101
    {       0x6e,  7 }, // 'S' ( 83) |1101110
    {       0x6f,  7 }, // 'T' ( 84) |1101111
    {       0x70,  7 }, // 'U' ( 85) |1110000
    {       0x71,  7 }, // 'V' ( 86) |1110001
    {       0x72,  7 }, // 'W' ( 87) |1110010
    {       0xfc,  8 }, // 'X' ( 88) |11111100
    {       0x73,  7 }, // 'Y' ( 89) |1110011
    {       0xfd,  8 }, // 'Z' ( 90) |11111101
    {     0x1ffb, 13 }, // '[' ( 91) |11111111|11011
    {    0x7fff0, 19 }, // '\' ( 92) |11111111|11111110|000
    {     0x1ffc, 13 }, // ']' ( 93) |11111111|11100
    {     0x3ffc, 14 }, // '^' ( 94) |11111111|111100
    {       0x22,  6 }, // '_' ( 95) |100010
    {     0x7ffd, 15 }, // '`' ( 96) |11111111|1111101
    {        0x3,  5 }, // 'a' ( 97) |00011
    {       0x23,  6 }, // 'b' ( 98) |100011
    {        0x4,  5 }, // 'c' ( 99) |00100
    {       0x24,  6 }, // 'd' (100) |100100
    {        0x5,  5 }, // 'e' (101) |00101
    {       0x25,  6 }, // 'f' (102) |100101
    {       0x26,  6 }, // 'g' (103) |100110
    {       0x27,  6 }, // 'h' (104) |100111
    {        0x6,  5 }, // 'i' (105) |00110
    {       0x74,  7 }, // 'j' (106) |1110100
    {       0x75,  7 }, // 'k' (107) |1110101
    {       0x28,  6 }, // 'l' (108) |101000
    {       0x29,  6 }, // 'm' (109) |101001
    {       0x2a,  6 }, // 'n' (110) |101010
    {        0x7,  5 }, // 'o' (111) |00111
    {       0x2b,  6 }, // 'p' (112) |101011
    {       0x76,  7 }, // 'q' (113) |1110110
    {       0x2c,  6 }, // 'r' (114) |101100
    {        0x8,  5 }, // 's' (115) |01000
    {        0x9,  5 }, // 't' (116) |01001
    {       0x2d,  6 }, // 'u' (117) |101101
    {       0x77,  7 }, // 'v' (118) |1110111
    {       0x78,  7 }, // 'w' (119) |1111000
    {       0x79,  7 }, // 'x' (120) |1111001
    {       0x7a,  7 }, // 'y' (121) |1111010
    {       0x7b,  7 }, // 'z' (122) |1111011
    {     0x7ffe, 15 }, // '{' (123) |11111111|1111110
    {      0x7fc, 11 }, // '|' (124) |11111111|100
    {     0x3ffd, 14 }, // '}' (125) |11111111|111101
    {     0x1ffd, 13 }, // '~' (126) |11111111|11101
    {  0xffffffc, 28 }, //     (127) |11111111|11111111|11111111|1100
    {    0xfffe6, 20 }, //     (128) |11111111|11111110|0110
    {   0x3fffd2, 22 }, //     (129) |11111111|11111111|010010
    {    0xfffe7, 20 }, //     (130) |11111111|11111110|0111
    {    0xfffe8, 20 }, //     (131) |11111111|11111110|1000
    {   0x3fffd3, 22 }, //     (132) |11111111|11111111|010011
    {   0x3fffd4, 22 }, //     (133) |11111111|11111111|010100
    {   0x3fffd5, 22 }, //     (134) |11111111|11111111|010101
    {   0x7fffd9, 23 }, //     (135) |11111111|11111111|1011001
    {   0x3fffd6, 22 }, //     (136) |11111111|11111111|010110
    {   0x7fffda, 23 }, //     (137) |11111111|11111111|1011010
    {   0x7fffdb, 23 }, //     (138) |11111111|11111111|1011011
    {   0x7fffdc, 23 }, //     (139) |11111111|11111111|1011100
    {   0x7fffdd, 23 }, //     (140) |11111111|11111111|1011101
    {   0x7fffde, 23 }, //     (141) |11111111|11111111|1011110
    {   0xffffeb, 24 }, //     (142) |11111111|11111111|11101011
    {   0x7fffdf, 23 }, //     (143) |11111111|11111111|1011111
    {   0xffffec, 24 }, //     (144) |11111111|11111111|11101100
    {   0xffffed, 24 }, //     (145) |11111111|11111111|11101101
    {   0x3fffd7, 22 }, //     (146) |11111111|11111111|010111
    {   0x7fffe0, 23 }, //     (147) |11111111|11111111|1100000
    {   0xffffee, 24 }, //     (148) |11111111|11111111|11101110
    {   0x7fffe1, 23 }, //     (149) |11111111|11111111|1100001
    {   0x7fffe2, 23 }, //     (150) |11111111|11111111|1100010
    {   0x7fffe3, 23 }, //     (151) |11111111|11111111|1100011
    {   0x7fffe4, 23 }, //     (152) |11111111|11111111|1100100
    {   0x1fffdc, 21 }, //     (153) |11111111|11111110|11100
    {   0x3fffd8, 22 }, //     (154) |11111111|11111111|011000
    {   0x7fffe5, 23 }, //     (155) |11111111|11111111|1100101
    {   0x3fffd9, 22 }, //     (156) |11111111|11111111|011001
    {   0x7fffe6, 23 }, //     (157) |11111111|11111111|1100110
    {   0x7fffe7, 23 }, //     (158) |11111111|11111111|1100111
    {   0xffffef, 24 }, //     (159) |11111111|11111111|11101111
    {   0x3fffda, 22 }, //     (160) |11111111|11111111|011010
    {   0x1fffdd, 21 }, //     (161) |11111111|11111110|11101
    {    0xfffe9, 20 }, //     (162) |11111111|11111110|1001
    {   0x3fffdb, 22 }, //     (163) |11111111|11111111|011011
    {   0x3fffdc, 22 }, //     (164) |11111111|11111111|011100
    {   0x7fffe8, 23 }, //     (165) |11111111|11111111|1101000
    {   0x7fffe9, 23 }, //     (166) |11111111|11111111|1101001
    {   0x1fffde, 21 }, //     (167) |11111111|11111110|11110
    {   0x7fffea, 23 }, //     (168) |11111111|11111111|1101010
    {   0x3fffdd, 22 }, //     (169) |11111111|11111111|011101
    {   0x3fffde, 22 }, //     (170) |11111111|11111111|011110
    {   0xfffff0, 24 }, //     (171) |11111111|11111111|11110000
    {   0x1fffdf, 21 }, //     (172) |11111111|11111110|11111
    {   0x3fffdf, 22 }, //     (173) |11111111|11111111|011111
    {   0x7fffeb, 23 }, //     (174) |11111111|11111111|1101011
    {   0x7fffec, 23 }, //     (175) |11111111|11111111|1101100
    {   0x1fffe0, 21 }, //     (176) |11111111|11111111|00000
    {   0x1fffe1, 21 }, //     (177) |11111111|11111111|00001
    {   0x3fffe0, 22 }, //     (178) |11111111|11111111|100000
    {   0x1fffe2, 21 }, //     (179) |11111111|11111111|00010
    {   0x7fffed, 23 }, //     (180) |11111111|11111111|1101101
    {   0x3fffe1, 22 }, //     (181) |11111111|11111111|100001
    {   0x7fffee, 23 }, //     (182) |11111111|11111111|1101110
    {   0x7fffef, 23 }, //     (183) |11111111|11111111|1101111
    {    0xfffea, 20 }, //     (184) |11111111|11111110|1010
    {   0x3fffe2, 22 }, //     (185) |11111111|11111111|100010
    {   0x3fffe3, 22 }, //     (186) |11111111|11111111|100011
    {   0x3fffe4, 22 }, //     (187) |11111111|11111111|100100
    {   0x7ffff0, 23 }, //     (188) |11111111|11111111|1110000
    {   0x3fffe5, 22 }, //     (189) |11111111|11111111|100101
    {   0x3fffe6, 22 }, //     (190) |11111111|11111111|100110
    {   0x7ffff1, 23 }, //     (191) |11111111|11111111|1110001
    {  0x3ffffe0, 26 }, //     (192) |11111111|11111111|11111000|00
    {  0x3ffffe1, 26 }, //     (193) |11111111|11111111|11111000|01
    {    0xfffeb, 20 }, //     (194) |11111111|11111110|1011
    {    0x7fff1, 19 }, //     (195) |11111111|11111110|001
    {   0x3fffe7, 22 }, //     (196) |11111111|11111111|100111
    {   0x7ffff2, 23 }, //     (197) |11111111|11111111|1110010
    {   0x3fffe8, 22 }, //     (198) |11111111|11111111|101000
    {  0x1ffffec, 25 }, //     (199) |11111111|11111111|11110110|0
    {  0x3ffffe2, 26 }, //     (200) |11111111|11111111|11111000|10
    {  0x3ffffe3, 26 }, //     (201) |11111111|11111111|11111000|11
    {  0x3ffffe4, 26 }, //     (202) |11111111|11111111|11111001|00
    {  0x7ffffde, 27 }, //     (203) |11111111|11111111|11111011|110
    {  0x7ffffdf, 27 }, //     (204) |11111111|11111111|11111011|111
    {  0x3ffffe5, 26 }, //     (205) |11111111|11111111|11111001|01
    {   0xfffff1, 24 }, //     (206) |11111111|11111111|11110001
    {  0x1ffffed, 25 }, //     (207) |11111111|11111111|11110110|1
    {    0x7fff2, 19 }, //     (208) |11111111|11111110|010
    {   0x1fffe3, 21 }, //     (209) |11111111|11111111|00011
    {  0x3ffffe6, 26 }, //     (210) |11111111|11111111|11111001|10
    {  0x7ffffe0, 27 }, //     (211) |11111111|11111111|11111100|000
    {  0x7ffffe1, 27 }, //     (212) |11111111|11111111|11111100|001
    {  0x3ffffe7, 26 }, //     (213) |11111111|11111111|11111001|11
    {  0x7ffffe2, 27 }, //     (214) |11111111|11111111|11111100|010
    {   0xfffff2, 24 }, //     (215) |11111111|11111111|11110010
    {   0x1fffe4, 21 }, //     (216) |11111111|11111111|00100
    {   0x1fffe5, 21 }, //     (217) |11111111|11111111|00101
    {  0x3ffffe8, 26 }, //     (218) |11111111|11111111|11111010|00
    {  0x3ffffe9, 26 }, //     (219) |11111111|11111111|11111010|01
    {  0xffffffd, 28 }, //     (220) |11111111|11111111|11111111|1101
    {  0x7ffffe3, 27 }, //     (221) |11111111|11111111|11111100|011
    {  0x7ffffe4, 27 }, //     (222) |11111111|11111111|11111100|100
    {  0x7ffffe5, 27 }, //     (223) |11111111|11111111|11111100|101
    {    0xfffec, 20 }, //     (224) |11111111|11111110|1100
    {   0xfffff3, 24 }, //     (225) |11111111|11111111|11110011
    {    0xfffed, 20 }, //     (226) |11111111|11111110|1101
    {   0x1fffe6, 21 }, //     (227) |11111111|11111111|00110
    {   0x3fffe9, 22 }, //     (228) |11111111|11111111|101001
    {   0x1fffe7, 21 }, //     (229) |11111111|11111111|00111
    {   0x1fffe8, 21 }, //     (230) |11111111|11111111|01000
    {   0x7ffff3, 23 }, //     (231) |11111111|11111111|1110011
    {   0x3fffea, 22 }, //     (232) |11111111|11111111|101010
    {   0x3fffeb, 22 }, //     (233) |11111111|11111111|101011
    {  0x1ffffee, 25 }, //     (234) |11111111|11111111|11110111|0
    {  0x1ffffef, 25 }, //     (235) |11111111|11111111|11110111|1
    {   0xfffff4, 24 }, //     (236) |11111111|11111111|11110100
    {   0xfffff5, 24 }, //     (237) |11111111|11111111|11110101
    {  0x3ffffea, 26 }, //     (238) |11111111|11111111|11111010|10
    {   0x7ffff4, 23 }, //     (239) |11111111|11111111|1110100
    {  0x3ffffeb, 26 }, //     (240) |11111111|11111111|11111010|11
    {  0x7ffffe6, 27 }, //     (241) |11111111|11111111|11111100|110
    {  0x3ffffec, 26 }, //     (242) |11111111|11111111|11111011|00
    {  0x3ffffed, 26 }, //     (243) |11111111|11111111|11111011|01
    {  0x7ffffe7, 27 }, //     (244) |11111111|11111111|11111100|111
    {  0x7ffffe8, 27 }, //     (245) |11111111|11111111|11111101|000
    {  0x7ffffe9, 27 }, //     (246) |11111111|11111111|11111101|001
    {  0x7ffffea, 27 }, //     (247) |11111111|11111111|11111101|010
    {  0x7ffffeb, 27 }, //     (248) |11111111|11111111|11111101|011
    {  0xffffffe, 28 }, //     (249) |11111111|11111111|11111111|1110
    {  0x7ffffec, 27 }, //     (250) |11111111|11111111|11111101|100
    {  0x7ffffed, 27 }, //     (251) |11111111|11111111|11111101|101
    {  0x7ffffee, 27 }, //     (252) |11111111|11111111|11111101|110
    {  0x7ffffef, 27 }, //     (253) |11111111|11111111|11111101|111
    {  0x7fffff0, 27 }, //     (254) |11111111|11111111|11111110|000
    {  0x3ffffee, 26 }, //     (255) |11111111|11111111|11111011|10
    { 0x3fffffff, 30 }, // EOS (256) |11111111|11111111|11111111|111111      
};    
static_assert(size(s_encodeTable) == 257, "");

      
/****************************************************************************
*     
*   Variables
*
***/  

static HuffDecoder s_decode(s_encodeTable, size(s_encodeTable));


/****************************************************************************
*     
*   Helpers
*     
***/  

//===========================================================================
static size_t fieldSize (const HpackFieldView & fld) {
    return strlen(fld.name) + strlen(fld.value) + 32;
}

//===========================================================================
static size_t fieldSize (const HpackDynField & fld) {
    return size(fld.name) + size(fld.value) + 32;
}


/****************************************************************************
*     
*   Encoding data
*     
***/  

//===========================================================================
HpackEncode::HpackEncode (size_t tableSize) {
    setTableSize(tableSize);
}

//===========================================================================
void HpackEncode::setTableSize (size_t tableSize) {
    m_dynSize = tableSize;
}

//===========================================================================
void HpackEncode::startBlock (CharBuf * out) {
    m_out = out;
}

//===========================================================================
void HpackEncode::endBlock () {
}

//===========================================================================
void HpackEncode::header (
    const char name[], 
    const char value[], 
    int flags
) {
    // (0x00) - literal header field without indexing (new name)
    // (0x10) - literal header field never indexed (new name)
    m_out->append(1, (flags & kNeverIndexed) ? 0x10 : 0x00);
    write(name);
    write(value);
}

//===========================================================================
void HpackEncode::header (
    HttpHdr name, 
    const char value[], 
    int flags
) {
}

//===========================================================================
void HpackEncode::write (const char str[]) {
    size_t len = strlen(str);
    write(str, len);
}

//===========================================================================
void HpackEncode::write (const char str[], size_t len) {
    // high bit 0 - encode as raw bytes (not huffman)
    write(len, 0x00, 7);
    m_out->append(str, len);
}

//===========================================================================
void HpackEncode::write (size_t val, char prefix, int prefixBits) {
    assert(prefixBits >= 1 && prefixBits <= 8);
    int limit = (1 << prefixBits) - 1;
    if (val < limit) {
        prefix |= (char) val;
        m_out->append(1, prefix);
        return;
    }
    prefix |= limit;
    m_out->append(1, prefix);
    val -= limit;
    while (val >= 128) {
        m_out->append(1, char(val % 128) + 128);
        val /= 128;
    }
    m_out->append(1, (char) val);
}


/****************************************************************************
*     
*   Decoding data
*     
***/  

//===========================================================================
// HuffDecoder
//===========================================================================
HuffDecoder::HuffDecoder (const EncodeItem items[], size_t count) {
    int shortest = numeric_limits<int>::max();
    int longest = 0;
    const EncodeItem * ptr = items,
        * term = ptr + count;
    for (; ptr != term; ++ptr) {
        shortest = min(shortest, ptr->bits);
        longest = max(longest, ptr->bits);
    }
    assert(shortest > 0 && longest < 32);
    m_prefixBits = shortest - 1;
    m_decodeTable.resize(1ull << m_prefixBits);

    ptr = items;
    int16_t val = 0;
    for (; ptr != term; ++ptr, ++val) {
        int pos = ptr->code >> (ptr->bits - m_prefixBits);
        int mask = 1 << (ptr->bits - m_prefixBits - 1);

        for (;;) {
            assert(pos >= 0 && pos < size(m_decodeTable));
            DecodeItem & item = m_decodeTable[pos];
            int16_t & key = (~ptr->code & mask) ? item.zero : item.one;
            mask >>= 1;
            if (!mask) {
                assert(key == kDecodeUnused);
                key = -val;
                break;
            } else {
                if (key != kDecodeUnused) {
                    pos = key;
                } else {
                    pos = (int) size(m_decodeTable);
                    key = (int16_t) pos;
                    m_decodeTable.resize(pos + 1);
                }
            }
        }
    }
}

//===========================================================================
static bool read (
    int * out, 
    int bits,
    int & availBits, 
    const char *& ptr, 
    const char * eptr
) {
    if (ptr == eptr) {
        assert(availBits == 8);
        return false;
    }

    int val;

    while (bits > availBits) {
        val = *ptr;
        val &= (1 << availBits) - 1;
        *out = (*out << availBits) + val;
        if (++ptr == eptr)
            return false;
        bits -= availBits;
        availBits = 8;
    }

    availBits -= bits;
    if (!availBits) {
        val = *ptr;
        availBits = 8;
        ptr += 1;
    } else {
        val = *ptr >> availBits;
    }
    val &= (1 << bits) - 1;
    *out = (*out << bits) + val;
    return true;
}

//===========================================================================
bool HuffDecoder::decode (
    const char ** out, 
    ITempHeap * heap, 
    size_t unusedBits,
    const char src[],
    size_t count
) {
    assert(count);
    int avail = (int) unusedBits;
    const char * ptr = src;
    const char * eptr = src + count;
    char * optr = heap->alloc<char>(2 * count);
    *out = optr;
    int val;
    for (;;) {
        int key = 0;
        if (!read(&key, m_prefixBits, avail, ptr, eptr))
            goto done;
        do {
            DecodeItem & di = m_decodeTable[key];
            if (!read(&key, 1, avail, ptr, eptr))
                goto done;
            val = (key & 1) ? di.one : di.zero;
            key = val;
        } while (val > 0);
        if (val < -255)
            return false;
        *optr++ = (char) -val;
    }

done:
    *optr = 0;
    return true;
}

//===========================================================================
// HpackDecode
//===========================================================================
HpackDecode::HpackDecode (size_t tableSize) 
    : m_dynSize{tableSize}
{}

//===========================================================================
void HpackDecode::reset () {
    size_t size = m_dynSize;
    setTableSize(0);
    setTableSize(size);
}

//===========================================================================
void HpackDecode::setTableSize (size_t tableSize) {
    m_dynSize = tableSize;
    pruneDynTable();
}

//===========================================================================
bool HpackDecode::parse (
    IHpackDecodeNotify * notify,
    ITempHeap * heap,
    const char src[],
    size_t srcLen
) {
    while (srcLen) {
        if (!readInstruction(notify, heap, src, srcLen))
            return false;
    }
    return true;
}

//===========================================================================
bool HpackDecode::readInstruction (
    IHpackDecodeNotify * notify,
    ITempHeap * heap,
    const char *& src, 
    size_t & srcLen
) {
    enum {
        kNeverIndexed,
        kNotIndexed,
        kIndexed,
    } mode = kIndexed;
    HpackFieldView fld;
    int ch = (unsigned char) *src;
    if (ch & 0x80) {
        // indexed header field
        if (!readIndexedField(&fld, heap, 7, src, srcLen))
            return false;
        mode = kNotIndexed;
    } else if (ch & 0x40) {
        // literal header field with incremental indexing
        if (ch & 0x3f) {
            // indexed
            if (!readIndexedName(&fld, heap, 6, src, srcLen))
                return false;
        } else {
            // new name
            if (!read(&fld.name, heap, ++src, --srcLen)
                || !read(&fld.value, heap, src, srcLen)
            ) {
                return false;
            }
        }
    } else if (ch & 0x20) {
        // table size update
        size_t size;
        if (!read(&size, 5, src, srcLen))
            return false;
        setTableSize(size);
        return true;
    } else {
        // literal header field without indexing
        if (ch & 0x10) {
            mode = kNeverIndexed;
        } else {
            mode = kNotIndexed;
        }
        if (ch & 0x0f) {
            // indexed
            if (!readIndexedName(&fld, heap, 4, src, srcLen))
                return false;
        } else {
            // new name
            if (!read(&fld.name, heap, ++src, --srcLen)
                || !read(&fld.value, heap, src, srcLen)
            ) {
                return false;
            }
        }
    }

    if (mode == kIndexed) {
        m_dynTable.emplace_front();
        auto & front = m_dynTable.front();
        front.name = fld.name;
        front.value = fld.value;
        m_dynUsed += fieldSize(fld);
        pruneDynTable();
    }

    notify->onHpackHeader(
        kHttpInvalid, 
        fld.name, 
        fld.value, 
        mode == kNeverIndexed ? kNeverIndexed : 0
    );
    return true;
}

//===========================================================================
bool HpackDecode::readIndexedField (
    HpackFieldView * out,
    ITempHeap * heap,
    size_t prefixBits,
    const char *& src, 
    size_t & srcLen
) {
    size_t index;
    if (!read(&index, prefixBits, src, srcLen))
        return false;
    if (index < size(s_staticTable)) {
        if (!index)
            return false;
        *out = s_staticTable[index];
    } else {
        index -= size(s_staticTable);
        if (index >= size(m_dynTable))
            return false;
        HpackDynField & dfld = m_dynTable[index];
        out->name = heap->strDup(data(dfld.name), size(dfld.name));
        out->value = heap->strDup(data(dfld.value), size(dfld.value));
    }
    return true;
}

//===========================================================================
bool HpackDecode::readIndexedName (
    HpackFieldView * out,
    ITempHeap * heap,
    size_t prefixBits,
    const char *& src, 
    size_t & srcLen
) {
    size_t index;
    if (!read(&index, prefixBits, src, srcLen))
        return false;
    if (index < size(s_staticTable)) {
        if (!index)
            return false;
        out->name = s_staticTable[index].name;
    } else {
        index -= size(s_staticTable);
        if (index >= size(m_dynTable))
            return false;
        HpackDynField & dfld = m_dynTable[index];
        out->name = heap->strDup(data(dfld.name));
    }
    if (!read(&out->value, heap, src, srcLen))
        return false;
    return true;
}

//===========================================================================
void HpackDecode::pruneDynTable () {
    while (m_dynUsed > m_dynSize) {
        m_dynUsed -= fieldSize(m_dynTable.back());
        m_dynTable.pop_back();
    }
}

//===========================================================================
bool HpackDecode::read (
    size_t * out,
    size_t prefixBits, 
    const char *& src, 
    size_t & srcLen
) {
    assert(prefixBits >= 1 && prefixBits <= 8);
    if (!srcLen)
        return false;

    int limit = (1 << prefixBits) - 1;
    size_t val = (unsigned char) *src++ & limit;
    srcLen -= 1;
    if (val < limit) {
        *out = val;
        return true;
    }
    int m = 0;
    for (; srcLen && m < 32; m += 7) {
        size_t b = *src++;
        srcLen -= 1;
        val += (b & 0x7f) << m;
        if (~b & 0x80) {
            *out = val;
            return true;
        }
    }
    return false;
}

//===========================================================================
bool HpackDecode::read (
    const char ** out,
    ITempHeap * heap,
    const char *& src, 
    size_t & srcLen
) {
    size_t len;
    bool huffman = (*src & 0x80) != 0;
    if (!read(&len, 7, src, srcLen))
        return false;
    if (srcLen < len)
        return false;
    
    if (!huffman) {
        *out = heap->strDup(src, len);
    } else {
        if (!s_decode.decode(out, heap, 8, src, len))
            return false;
    }

    src += len;
    srcLen -= len;
    return true;
}

} // namespace
