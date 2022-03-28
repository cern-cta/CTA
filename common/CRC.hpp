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

#pragma once

#include <stdint.h>

namespace cta {

/**
 * Set of CRC functions.
 */ 
  
/**
 * Compute by software Reed Solomon CRC for the given block.
 *
 * @param crcInit  The initial crc (0 for fresh) (i.e., seed).
 * @param cnt      The number of data bytes to compute CRC for.
 * @param start    The starting address of the data bytes (e.g., data buffer).
 * @return The computed CRC in big endian (MSB is first byte).
 */

uint32_t crcRS_sw (const uint32_t crcInit, const uint32_t cnt, 
  const void *const start);
  
/**
 * Compute by software CRC32C (iSCSI) for the given block.
 *
 * @param crcInit  The initial crc (0xFFFFFFFF for fresh) (i.e., seed).
 * @param cnt      The number of data bytes to compute CRC for.
 * @param start    The starting address of the data bytes (e.g., data buffer).
 * @return The computed CRC in big endian (MSB is first byte). 
 */
uint32_t crc32c_sw (const uint32_t crcInit, const uint32_t cnt, 
  const void *const start);

/**
 * Compute by hardware CRC32C (iSCSI) for the given block.
 *
 * CRC32 Generator Polynomial:
 * 0x11EDC6F41
 * x^32+x^28+x^27+x^26+x^25+x^23+x^22+x^20+x^19+x^18+x^14+x^13+x^11+
 * x^10+x^9+x^8+x^6+x^0
 *
 * @param crcInit  The initial crc (0xFFFFFFFF for fresh) (i.e., seed).
 * @param cnt      The number of data bytes to compute CRC for.
 * @param start    The starting address of the data bytes (e.g., data buffer).
 * @return The computed CRC in big endian (MSB is first byte). 
 */
uint32_t crc32c_hw (const uint32_t crcInit, const uint32_t cnt, 
  const void *const start);

/**
 * Check for SSE 4.2.  SSE 4.2 was first supported in Nehalem processors
 * introduced in November, 2008.  This does not check for the existence of the
 * cpuid instruction itself, which was introduced on the 486SL in 1992, so this
 * will fail on earlier x86 processors.  cpuid works on all Pentium and later
 * processors. 
 */
#define SSE42(have) \
  do { \
      uint32_t eax, ecx; \
      eax = 1; \
      __asm__("cpuid" \
              : "=c"(ecx) \
              : "a"(eax) \
              : "%ebx", "%edx"); \
      (have) = (ecx >> 20) & 1; \
  } while (0)

/**
 * Compute the CRC32C (iSCSI). If the crc32 processor's instruction is
 * available than use the hardware version. Otherwise use the software version.
 *
 * @param crcInit  The initial crc (0xFFFFFFFF for fresh) (i.e., seed).
 * @param cnt      The number of data bytes to compute CRC for.
 * @param start    The starting address of the data bytes (e.g., data buffer).
 * @return The computed CRC in big endian (MSB is first byte). 
 */
uint32_t crc32c(const uint32_t crcInit, const uint32_t cnt, 
  const void *const start);

/**
 * Compute the CRC32C (iSCSI) and append it to the memory block.
 * If the crc32 processor's instruction is available
 * use the hardware version. Otherwise use the software version.
 *
 * @param crcInit  The initial crc (0xFFFFFFFF for fresh) (i.e., seed).
 * @param cnt      The number of data bytes in the memory block.
 * @param start    The starting address of the data bytes (e.g., data buffer).
 *                 The memory block must be big enough to save additional
 *                 4 bytes at the end.
 * @return The length of the memory block with crc32c added.
 */
uint32_t addCrc32cToMemoryBlock(const uint32_t crcInit,
  const uint32_t cnt, uint8_t *start);

/**
 * Compute the CRC32C (iSCSI) and check it for the memory block with CRC32C
 * addition. If the crc32 processor's instruction is available
 * use the hardware version. Otherwise use the software version.
 *
 * @param crcInit  The initial crc (0xFFFFFFFF for fresh) (i.e., seed).
 * @param cnt      The number of data bytes in the memory block.
 * @param start    The starting address of the data bytes (e.g., data buffer).
 * @return True if CRC32C is correct and False otherwise.
 */
bool verifyCrc32cForMemoryBlockWithCrc32c(
  const uint32_t crcInit, const uint32_t cnt, const uint8_t *start);


} // namespace cta
