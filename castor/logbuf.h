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
 * @(#)$RCSfile: logbuf.h,v $ $Revision: 1.1 $ $Release$ $Date: 2004/05/28 09:40:26 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_LOGBUF_H 
#define CASTOR_LOGBUF_H 1

// Include Files
#include <iostream>
#include <fstream>
#include <string>

namespace castor {

  class logbuf : public std::filebuf {

  public:

    /**
     * Constructor
     */
    logbuf() : std::filebuf(), m_newline(true) {}

  public:

    /**
     * output of n characters in one go
     */
    virtual std::streamsize
      xsputn(const char* __s, std::streamsize __n) {
      if (m_newline) {
        std::string prefix = getTimeStamp();
        std::filebuf::xsputn(prefix.c_str(), prefix.size());
      }
      m_newline = (__s[__n-1] == '\n');
      std::filebuf::xsputn(__s, __n);
    }

    /**
     * set m_newline to true
     */
    void setNewLine() { m_newline = true; }

  private:

    /**
     * Build a prefix for logs containing the date, time
     * and thread number
     */
    std::string getTimeStamp();

  private:
    
    /**
     * remember whether we are at a new line
     */
    bool m_newline;

  };
  

} // End of namespace Castor

#endif // CASTOR_LOGBUF_H
