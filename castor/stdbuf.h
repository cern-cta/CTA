/******************************************************************************
 *                      stdbuf.h
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
 * @(#)$RCSfile: stdbuf.h,v $ $Revision: 1.2 $ $Release$ $Date: 2004/07/29 16:57:43 $ $Author: sponcec3 $
 *
 * A string buffer for logging into standard output
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_STDBUF_H
#define CASTOR_STDBUF_H 1

// Include Files
#include "castor/logbuf.h"
#include <stdio.h>
#include <Cthread_api.h>

namespace castor {

  class stdbuf : public castor::logbuf {
      
  public:

    /**
     * Synchronizes the buffer but writing it to stdout
     */
    virtual int sync() throw() {
      if (0 == str().size()) return 0;
      // Write current message to stdout
      Cthread_mutex_lock(&s_lock);
      printf("%s", str().c_str());
      fflush(stdout);
      Cthread_mutex_unlock(&s_lock);
      // Erase buffer
      str("");
      return 0;
    }

  private:

      /**
       * A lock to ensure that printf is never called
       * simultaneously by two threads
       */
      static int s_lock;

  };


} // End of namespace Castor

#endif // CASTOR_STDBUF_H
