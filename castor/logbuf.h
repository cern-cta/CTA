/******************************************************************************
 *                      logbuf.h
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: logbuf.h,v $ $Revision: 1.9 $ $Release$ $Date: 2005/02/17 10:56:44 $ $Author: sponcec3 $
 *
 * An abstract string buffer for the log that is able
 * to handle levels of output
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_LOGBUF_H
#define CASTOR_LOGBUF_H 1

// Include Files
#include <sstream>
#include "Cuuid.h"
#include "Cns_api.h"
#include "dlf_api.h"

namespace castor {

  class logbuf : public std::stringbuf {

  public:

    /**
     * The different possible levels of output
     */
    typedef int Level;

  public:

    /**
     * Constructor
     */
    logbuf() throw() : std::stringbuf(std::ios_base::out),
      m_curLevel(DLF_LVL_USAGE), m_fileId(0) {
      memset(&m_uuid, 0, sizeof(Cuuid_t));
    }

    /**
     * Sets current output level
     */
    void setLevel(logbuf::Level l) throw() { m_curLevel = l; }

    /**
     * Gets the current output level
     */
    logbuf::Level level() throw() { return m_curLevel; }

    /**
     * setUuid
     */
    void setUuid(Cuuid_t val) {
      m_uuid = val;
    }

    /**
     * setFileId
     */
    void setFileId(Cns_fileid* val) {
      if (0 == val) {
        if (0 != m_fileId) {
          free(m_fileId);
          m_fileId = 0;
        }
      } else {
        if (0 == m_fileId) {
          m_fileId = (Cns_fileid*) malloc(sizeof(Cns_fileid));
        }
        *m_fileId = *val;
      }
    }

  public:
    
    /**
     * Synchronizes the buffer
     */
    virtual int sync() throw() = 0;
    
  protected:

    /**
     * The current level of output for the stream.
     * Next calls to << will use this level
     */
    logbuf::Level m_curLevel;

    /**
     * Current uuid if any
     */
    Cuuid_t m_uuid;

    /**
     * Current fileId if any
     */
    Cns_fileid* m_fileId;

  };


} // End of namespace Castor

#endif // CASTOR_LOGBUF_H
