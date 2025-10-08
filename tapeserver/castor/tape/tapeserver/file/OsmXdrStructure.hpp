/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <rpc/xdr.h> //!< xdrmem_create for decode_legato

namespace castor::tape::tapeFile::osm::xdr {

struct Chunk {
public:
  uint64_t m_ulSsid;
  uint64_t mc_ulLow;
  struct {
    uint32_t m_uiDataLen;
    char *m_pcDataVal = nullptr;
  } m_data;

  static bool_t xdr_chunk(XDR *xdrs, Chunk *objp) {
    if (!xdr_u_long(xdrs, &objp->m_ulSsid)) {
      return (FALSE);
    }
    if (!xdr_u_long(xdrs, &objp->mc_ulLow)) {
      return (FALSE);
    }
    // #define MAXMRECSIZE 32768
    if (!xdr_bytes(xdrs, reinterpret_cast<char**>(&objp->m_data.m_pcDataVal), reinterpret_cast<uint32_t*>(&objp->m_data.m_uiDataLen), 32768)) {
      return (FALSE);
    }
    return (TRUE);
  }
};

struct Record {
public:
  char m_tcHandler[128];
  uint64_t m_ulVolid;
  uint64_t m_ulFn;
  uint64_t m_ulRn;
  uint64_t m_ulLen;
  struct {
    uint32_t m_ulChunkLen;
    Chunk *m_pChunk = nullptr;
  } m_RChunk;

  ~Record() {
    bFree = true;
    xdr_free((xdrproc_t) Record::_decode, reinterpret_cast<char*>(this));
  }

  bool_t decode(XDR *xdrs) {
    return _decode(xdrs, this);
  }

private:
  bool bFree = false;

  static bool_t _decode(XDR *xdrs, Record *objp) {
    // #define MHNDLEN 128
    if(!xdr_opaque(xdrs, objp->m_tcHandler, 128)) {
      return (FALSE);
    }
    if(!xdr_u_long(xdrs, &objp->m_ulVolid)) {
      return (FALSE);
    }
    if(!xdr_u_long(xdrs, &objp->m_ulFn)) {
      return (FALSE);
    }
    if(!xdr_u_long(xdrs, &objp->m_ulRn)) {
      return (FALSE);
    }
    if(!xdr_u_long(xdrs, &objp->m_ulLen)) {
      return (FALSE);
    }
    // #define MMAXCHK 2048
    if((objp->bFree && objp->m_RChunk.m_pChunk == nullptr) || !xdr_array(xdrs, reinterpret_cast<char**>(&objp->m_RChunk.m_pChunk), reinterpret_cast<uint32_t*>(&objp->m_RChunk.m_ulChunkLen), 2048,
        sizeof(Chunk), (xdrproc_t) &Chunk::xdr_chunk)) {
      return (FALSE);
    }
    return (TRUE);
  }
};

struct VolLabel {
public:
  uint64_t m_ulMagic;
  uint64_t m_ulExpireTime;
  uint64_t m_ulCreateTime;
  uint64_t m_ulRecSize;
  uint64_t m_ulVolId;
  char *m_pcVolName = nullptr;

  ~VolLabel() {
    bFree = true;
    xdr_free((xdrproc_t) VolLabel::_decode, reinterpret_cast<char*>(this));
  }

  bool_t decode(XDR *xdrs) {
    return _decode(xdrs, this);
  }

  private:
  bool bFree = false;

  static bool_t _decode(XDR *xdrs, VolLabel *objp) {
    if(!xdr_u_long(xdrs, &objp->m_ulMagic)) {
      return (FALSE);
    }
    if(!xdr_u_long(xdrs, &objp->m_ulCreateTime)) {
      return (FALSE);
    }
    if(!xdr_u_long(xdrs, &objp->m_ulExpireTime)) {
      return (FALSE);
    }
    if(!xdr_u_long(xdrs, &objp->m_ulRecSize)) {
      return (FALSE);
    }
    if(!xdr_u_long(xdrs, &objp->m_ulVolId)) {
      return (FALSE);
    }
    // #define NSR_LENGTH = 64
    if((objp->bFree && objp->m_pcVolName == nullptr) || !xdr_string(xdrs, &objp->m_pcVolName, 64)) {
      return (FALSE);
    }
    return (TRUE);
  }
};

} // namespace castor::tape::tapeFile::osm::xdr
