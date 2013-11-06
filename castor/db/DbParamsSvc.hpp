/******************************************************************************
 *                      DbParamsSvc.hpp
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
 *
 * A service to provide parameters to access the db layer of a Castor application
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef CASTOR_DB_DBPARAMSSVC_HPP
#define CASTOR_DB_DBPARAMSSVC_HPP 1

// Include files
#include <string>
#include "castor/BaseSvc.hpp"
#include "castor/Constants.hpp"

namespace castor {
  
 namespace db {

  /**
   * The DbParams service
   */
  class DbParamsSvc : public castor::BaseSvc {

  public:

    /**
     * Default constructor
     */
    DbParamsSvc(const std::string name) throw();
    
    /**
     * virtual destructor
     */
     virtual ~DbParamsSvc() throw() {};

    /**
     * Get the service id
     */
    virtual unsigned int id() const;

    /**
     * Get the service id
     */
    static unsigned int ID();

    /**
     * Get the schema version
     * @return the value of m_schemaVersion
     */
    std::string getSchemaVersion() const {
      return m_schemaVersion;
    }

    /**
     * Set the schema version
     * @param the new value of m_schemaVersion
     */
    void setSchemaVersion(std::string value) {
      m_schemaVersion = value;
    }
    
    /**
     * Get the config file name
     * @return the value of m_confFile
     */
    std::string getDbAccessConfFile() const {
      return m_confFile;
    }

    /**
     * Set the config file name
     * @param the new value of m_confFile
     */
    void setDbAccessConfFile(std::string value) {
      m_confFile = value;
    }
    
  private:
  
    /// the config file containing db login info
    std::string m_confFile;
    
    /// the schema version against with the db will be checked    
    std::string m_schemaVersion;    

  };
  
 } // end of namespace db

} // end of namespace castor

#endif // CASTOR_DB_DBPARAMSSVC_HPP
