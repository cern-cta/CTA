/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/CRC.hpp"

#include <stdint.h>

namespace cta {
  
//-----------------------------------------------------------------------------
// crcRS_sw
//-----------------------------------------------------------------------------
uint32_t crcRS_sw (const uint32_t crcInit, 
  const uint32_t cnt, const void *const start) {
  static const uint32_t crcTable[256] = {
    0x00000000, 0x38CF3801, 0x70837002, 0x484C4803, 0xE01BE004, 0xD8D4D805,
    0x90989006, 0xA857A807, 0xDD36DD08, 0xE5F9E509, 0xADB5AD0A, 0x957A950B,
    0x3D2D3D0C, 0x05E2050D, 0x4DAE4D0E, 0x7561750F, 0xA76CA710, 0x9FA39F11,
    0xD7EFD712, 0xEF20EF13, 0x47774714, 0x7FB87F15, 0x37F43716, 0x0F3B0F17,
    0x7A5A7A18, 0x42954219, 0x0AD90A1A, 0x3216321B, 0x9A419A1C, 0xA28EA21D,
    0xEAC2EA1E, 0xD20DD21F, 0x53D85320, 0x6B176B21, 0x235B2322, 0x1B941B23,
    0xB3C3B324, 0x8B0C8B25, 0xC340C326, 0xFB8FFB27, 0x8EEE8E28, 0xB621B629,
    0xFE6DFE2A, 0xC6A2C62B, 0x6EF56E2C, 0x563A562D, 0x1E761E2E, 0x26B9262F,
    0xF4B4F430, 0xCC7BCC31, 0x84378432, 0xBCF8BC33, 0x14AF1434, 0x2C602C35,
    0x642C6436, 0x5CE35C37, 0x29822938, 0x114D1139, 0x5901593A, 0x61CE613B,
    0xC999C93C, 0xF156F13D, 0xB91AB93E, 0x81D5813F, 0xA6ADA640, 0x9E629E41,
    0xD62ED642, 0xEEE1EE43, 0x46B64644, 0x7E797E45, 0x36353646, 0x0EFA0E47,
    0x7B9B7B48, 0x43544349, 0x0B180B4A, 0x33D7334B, 0x9B809B4C, 0xA34FA34D,
    0xEB03EB4E, 0xD3CCD34F, 0x01C10150, 0x390E3951, 0x71427152, 0x498D4953,
    0xE1DAE154, 0xD915D955, 0x91599156, 0xA996A957, 0xDCF7DC58, 0xE438E459,
    0xAC74AC5A, 0x94BB945B, 0x3CEC3C5C, 0x0423045D, 0x4C6F4C5E, 0x74A0745F,
    0xF575F560, 0xCDBACD61, 0x85F68562, 0xBD39BD63, 0x156E1564, 0x2DA12D65,
    0x65ED6566, 0x5D225D67, 0x28432868, 0x108C1069, 0x58C0586A, 0x600F606B,
    0xC858C86C, 0xF097F06D, 0xB8DBB86E, 0x8014806F, 0x52195270, 0x6AD66A71,
    0x229A2272, 0x1A551A73, 0xB202B274, 0x8ACD8A75, 0xC281C276, 0xFA4EFA77,
    0x8F2F8F78, 0xB7E0B779, 0xFFACFF7A, 0xC763C77B, 0x6F346F7C, 0x57FB577D,
    0x1FB71F7E, 0x2778277F, 0x51475180, 0x69886981, 0x21C42182, 0x190B1983,
    0xB15CB184, 0x89938985, 0xC1DFC186, 0xF910F987, 0x8C718C88, 0xB4BEB489,
    0xFCF2FC8A, 0xC43DC48B, 0x6C6A6C8C, 0x54A5548D, 0x1CE91C8E, 0x2426248F,
    0xF62BF690, 0xCEE4CE91, 0x86A88692, 0xBE67BE93, 0x16301694, 0x2EFF2E95,
    0x66B36696, 0x5E7C5E97, 0x2B1D2B98, 0x13D21399, 0x5B9E5B9A, 0x6351639B,
    0xCB06CB9C, 0xF3C9F39D, 0xBB85BB9E, 0x834A839F, 0x029F02A0, 0x3A503AA1,
    0x721C72A2, 0x4AD34AA3, 0xE284E2A4, 0xDA4BDAA5, 0x920792A6, 0xAAC8AAA7,
    0xDFA9DFA8, 0xE766E7A9, 0xAF2AAFAA, 0x97E597AB, 0x3FB23FAC, 0x077D07AD,
    0x4F314FAE, 0x77FE77AF, 0xA5F3A5B0, 0x9D3C9DB1, 0xD570D5B2, 0xEDBFEDB3,
    0x45E845B4, 0x7D277DB5, 0x356B35B6, 0x0DA40DB7, 0x78C578B8, 0x400A40B9,
    0x084608BA, 0x308930BB, 0x98DE98BC, 0xA011A0BD, 0xE85DE8BE, 0xD092D0BF,
    0xF7EAF7C0, 0xCF25CFC1, 0x876987C2, 0xBFA6BFC3, 0x17F117C4, 0x2F3E2FC5,
    0x677267C6, 0x5FBD5FC7, 0x2ADC2AC8, 0x121312C9, 0x5A5F5ACA, 0x629062CB,
    0xCAC7CACC, 0xF208F2CD, 0xBA44BACE, 0x828B82CF, 0x508650D0, 0x684968D1,
    0x200520D2, 0x18CA18D3, 0xB09DB0D4, 0x885288D5, 0xC01EC0D6, 0xF8D1F8D7,
    0x8DB08DD8, 0xB57FB5D9, 0xFD33FDDA, 0xC5FCC5DB, 0x6DAB6DDC, 0x556455DD,
    0x1D281DDE, 0x25E725DF, 0xA432A4E0, 0x9CFD9CE1, 0xD4B1D4E2, 0xEC7EECE3,
    0x442944E4, 0x7CE67CE5, 0x34AA34E6, 0x0C650CE7, 0x790479E8, 0x41CB41E9,
    0x098709EA, 0x314831EB, 0x991F99EC, 0xA1D0A1ED, 0xE99CE9EE, 0xD153D1EF,
    0x035E03F0, 0x3B913BF1, 0x73DD73F2, 0x4B124BF3, 0xE345E3F4, 0xDB8ADBF5,
    0x93C693F6, 0xAB09ABF7, 0xDE68DEF8, 0xE6A7E6F9, 0xAEEBAEFA, 0x962496FB,
    0x3E733EFC, 0x06BC06FD, 0x4EF04EFE, 0x763F76FF
  };

  uint32_t crc = crcInit;
  const uint8_t *d = (const uint8_t*)start;
  for (uint32_t i = 0; i < cnt; i++)
    {
      crc = (crc << 8) ^ crcTable[*d ^ (crc >> 24)];
      d++;
    }
  return crc;
}

//-----------------------------------------------------------------------------
// crc32c_sw
//-----------------------------------------------------------------------------
uint32_t crc32c_sw (const uint32_t crcInit, 
  const uint32_t cnt, const void *const start)
{
  static const uint32_t crcTable[256] = {
  0x00000000,0xF26B8303,0xE13B70F7,0x1350F3F4,0xC79A971F,0x35F1141C,
  0x26A1E7E8,0xD4CA64EB,0x8AD958CF,0x78B2DBCC,0x6BE22838,0x9989AB3B,
  0x4D43CFD0,0xBF284CD3,0xAC78BF27,0x5E133C24,0x105EC76F,0xE235446C,
  0xF165B798,0x030E349B,0xD7C45070,0x25AFD373,0x36FF2087,0xC494A384,
  0x9A879FA0,0x68EC1CA3,0x7BBCEF57,0x89D76C54,0x5D1D08BF,0xAF768BBC,
  0xBC267848,0x4E4DFB4B,0x20BD8EDE,0xD2D60DDD,0xC186FE29,0x33ED7D2A,
  0xE72719C1,0x154C9AC2,0x061C6936,0xF477EA35,0xAA64D611,0x580F5512,
  0x4B5FA6E6,0xB93425E5,0x6DFE410E,0x9F95C20D,0x8CC531F9,0x7EAEB2FA,
  0x30E349B1,0xC288CAB2,0xD1D83946,0x23B3BA45,0xF779DEAE,0x05125DAD,
  0x1642AE59,0xE4292D5A,0xBA3A117E,0x4851927D,0x5B016189,0xA96AE28A,
  0x7DA08661,0x8FCB0562,0x9C9BF696,0x6EF07595,0x417B1DBC,0xB3109EBF,
  0xA0406D4B,0x522BEE48,0x86E18AA3,0x748A09A0,0x67DAFA54,0x95B17957,
  0xCBA24573,0x39C9C670,0x2A993584,0xD8F2B687,0x0C38D26C,0xFE53516F,
  0xED03A29B,0x1F682198,0x5125DAD3,0xA34E59D0,0xB01EAA24,0x42752927,
  0x96BF4DCC,0x64D4CECF,0x77843D3B,0x85EFBE38,0xDBFC821C,0x2997011F,
  0x3AC7F2EB,0xC8AC71E8,0x1C661503,0xEE0D9600,0xFD5D65F4,0x0F36E6F7,
  0x61C69362,0x93AD1061,0x80FDE395,0x72966096,0xA65C047D,0x5437877E,
  0x4767748A,0xB50CF789,0xEB1FCBAD,0x197448AE,0x0A24BB5A,0xF84F3859,
  0x2C855CB2,0xDEEEDFB1,0xCDBE2C45,0x3FD5AF46,0x7198540D,0x83F3D70E,
  0x90A324FA,0x62C8A7F9,0xB602C312,0x44694011,0x5739B3E5,0xA55230E6,
  0xFB410CC2,0x092A8FC1,0x1A7A7C35,0xE811FF36,0x3CDB9BDD,0xCEB018DE,
  0xDDE0EB2A,0x2F8B6829,0x82F63B78,0x709DB87B,0x63CD4B8F,0x91A6C88C,
  0x456CAC67,0xB7072F64,0xA457DC90,0x563C5F93,0x082F63B7,0xFA44E0B4,
  0xE9141340,0x1B7F9043,0xCFB5F4A8,0x3DDE77AB,0x2E8E845F,0xDCE5075C,
  0x92A8FC17,0x60C37F14,0x73938CE0,0x81F80FE3,0x55326B08,0xA759E80B,
  0xB4091BFF,0x466298FC,0x1871A4D8,0xEA1A27DB,0xF94AD42F,0x0B21572C,
  0xDFEB33C7,0x2D80B0C4,0x3ED04330,0xCCBBC033,0xA24BB5A6,0x502036A5,
  0x4370C551,0xB11B4652,0x65D122B9,0x97BAA1BA,0x84EA524E,0x7681D14D,
  0x2892ED69,0xDAF96E6A,0xC9A99D9E,0x3BC21E9D,0xEF087A76,0x1D63F975,
  0x0E330A81,0xFC588982,0xB21572C9,0x407EF1CA,0x532E023E,0xA145813D,
  0x758FE5D6,0x87E466D5,0x94B49521,0x66DF1622,0x38CC2A06,0xCAA7A905,
  0xD9F75AF1,0x2B9CD9F2,0xFF56BD19,0x0D3D3E1A,0x1E6DCDEE,0xEC064EED,
  0xC38D26C4,0x31E6A5C7,0x22B65633,0xD0DDD530,0x0417B1DB,0xF67C32D8,
  0xE52CC12C,0x1747422F,0x49547E0B,0xBB3FFD08,0xA86F0EFC,0x5A048DFF,
  0x8ECEE914,0x7CA56A17,0x6FF599E3,0x9D9E1AE0,0xD3D3E1AB,0x21B862A8,
  0x32E8915C,0xC083125F,0x144976B4,0xE622F5B7,0xF5720643,0x07198540,
  0x590AB964,0xAB613A67,0xB831C993,0x4A5A4A90,0x9E902E7B,0x6CFBAD78,
  0x7FAB5E8C,0x8DC0DD8F,0xE330A81A,0x115B2B19,0x020BD8ED,0xF0605BEE,
  0x24AA3F05,0xD6C1BC06,0xC5914FF2,0x37FACCF1,0x69E9F0D5,0x9B8273D6,
  0x88D28022,0x7AB90321,0xAE7367CA,0x5C18E4C9,0x4F48173D,0xBD23943E,
  0xF36E6F75,0x0105EC76,0x12551F82,0xE03E9C81,0x34F4F86A,0xC69F7B69,
  0xD5CF889D,0x27A40B9E,0x79B737BA,0x8BDCB4B9,0x988C474D,0x6AE7C44E,
  0xBE2DA0A5,0x4C4623A6,0x5F16D052,0xAD7D5351 };
 
  uint32_t crc = crcInit;
  const uint8_t *d = (const uint8_t *) start;
  for (uint32_t i=0; i<cnt; i++ )
    {
      crc = (crc >> 8) ^ crcTable[*d ^ (crc & 0xFF)];
      d++;
    }
  return crc ^ 0XFFFFFFFF;
}

//-----------------------------------------------------------------------------
// Helper function crc32c_intel_le_hw_8b 
//-----------------------------------------------------------------------------
uint32_t crc32c_intel_le_hw_8b(const uint32_t crcInit, 
  const uint8_t *const data, uint32_t length) {
  uint32_t crc = crcInit;
  const uint8_t *ptr = data;

  while (length--) {
    __asm__ __volatile__(
        ".byte 0xF2, 0x0F, 0x38, 0xF0, 0xF1"
        :"=S"(crc)
        :"0"(crc), "c"(*ptr)
        );
    ptr++;
  }
  return crc;
}

//-----------------------------------------------------------------------------
// Helper function crc32c_intel_le_hw_64b
//-----------------------------------------------------------------------------
uint32_t crc32c_intel_le_hw_64b(const uint32_t crcInit, 
  const uint64_t *const data, uint32_t length) {
  uint32_t crc = crcInit;
  const uint64_t *ptr = data;
  
  
  #if defined(__x86_64__)
    #define REX_PRE "0x48, "
  #else
    #define REX_PRE
  #endif

  while (length--) {
    __asm__ __volatile__(
        ".byte 0xF2, " REX_PRE "0x0F, 0x38, 0xF1, 0xF1"
        :"=S"(crc)
        :"0"(crc), "c"(*ptr)
        );
    ptr++;
  }
  return crc;
}


//-----------------------------------------------------------------------------
// crc32c_hw
//-----------------------------------------------------------------------------
uint32_t crc32c_hw (const uint32_t crcInit, 
  const uint32_t cnt, const void *const start) {

  /* Do CPU 64 instruction */
  const uint8_t *blk_adr = (const uint8_t *) start;
  uint32_t iquotient = cnt / 8;
  uint32_t iremainder = cnt % 8;
  uint32_t crc = crcInit;
  while (iquotient--) {
    crc = crc32c_intel_le_hw_64b(crc, (const uint64_t *)blk_adr, 1);
    blk_adr += 8;
  }
  if (iremainder) {
    crc = crc32c_intel_le_hw_8b(crc, blk_adr, iremainder);
  }
  crc = crc ^ 0XFFFFFFFF;

  return crc;
}

//-----------------------------------------------------------------------------
// crc32c
//-----------------------------------------------------------------------------
uint32_t crc32c(const uint32_t crcInit, const uint32_t cnt,
  const void *const start) {
  int sse42;

  SSE42(sse42);
  return sse42 ? crc32c_hw(crcInit, cnt, start): crc32c_sw(crcInit, cnt, start);
}

//-----------------------------------------------------------------------------
// addCrc32cToMemoryBlock
//-----------------------------------------------------------------------------
uint32_t addCrc32cToMemoryBlock(const uint32_t crcInit,
  const uint32_t cnt, uint8_t *start ) {
  if (cnt == 0)
    return 0; //no such thing as a zero length block in SSC (write NOP)
  const uint32_t crc = crc32c(crcInit, cnt, start);

  //append CRC in proper byte order (regardless of system endian-ness)
  start[cnt+0] = (crc >>  0) & 0xFF;
  start[cnt+1] = (crc >>  8) & 0xFF;
  start[cnt+2] = (crc >> 16) & 0xFF;
  start[cnt+3] = (crc >> 24) & 0xFF;
  return (cnt+4); //size of block to be written includes CRC
}

//-----------------------------------------------------------------------------
// verifyCrc32cForMemoryBlockWithCrc32c
//-----------------------------------------------------------------------------
bool verifyCrc32cForMemoryBlockWithCrc32c(
  const uint32_t crcInit, const uint32_t cnt, const uint8_t *start) {
  if (cnt <= 4)
    return false; //block is too small to be valid, cannot check CRC

  const uint32_t crccmp = crc32c(crcInit, cnt-4, start);
  const uint32_t crcblk= (start[cnt-4] << 0) |
    (start[cnt-3] << 8) |
    (start[cnt-2] << 16) |
    (start[cnt-1] << 24);

  if (crccmp != crcblk)
    return false; //block CRC is incorrect
  return true;
}

} // namespace cta