/******************************************************************************
 *                      MsgSvc.hpp
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
 * @(#)$RCSfile: MsgSvc.hpp,v $ $Revision: 1.1.1.1 $ $Release$ $Date: 2004/05/12 12:13:34 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_MSGSVC_HPP 
#define CASTOR_MSGSVC_HPP 1

// Include Files
#include "castor/logstream.h"

// Local Includes
#include "BaseSvc.hpp"

namespace castor {

  /**
   * A message service to handle logs.
   * The default output is on cout
   */
  class MsgSvc : public BaseSvc {

  public:
    
    /**
     * Constructor
     */
    MsgSvc(const std::string name);
    
    /**
     * Destructor
     */
    virtual ~MsgSvc();

    /**
     * Get the service id
     */
    virtual const unsigned int id() const;

    /**
     * Get the service id, statically
     */
    static const unsigned int ID();

    /**
     * get the output stream associated to this service
     */
    castor::logstream& stream();


    /**
     * Returns the defaultname of the logfile
     */
    static std::string defaultLogfilename();

    /**
     * Sets the default log file name
     */
    static void setDefaultLogfilename(std::string lfn);



  private:

    /** the output stream */
    castor::logstream* m_stream;

    /** The name of the logfile */
    static std::string s_defaultLogfilename;

  };
  
} // end of namespace castor

#endif // CASTOR_MSGSVC_HPP
