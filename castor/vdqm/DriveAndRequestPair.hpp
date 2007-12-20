/******************************************************************************
 *                      DriveAndRequestPair.hpp
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
 *
 * @author castor dev team
 *****************************************************************************/
 
#ifndef CASTOR_VDQM_DRIVEANDREQUESTPAIR_HPP
#define CASTOR_VDQM_DRIVEANDREQUESTPAIR_HPP 1

#include "castor/IObject.hpp"


namespace castor {

  namespace vdqm {

    // Forward declaration
    class TapeDrive;
    class TapeRequest;

    /**
     * A tape and a tape drive.
     */
    class DriveAndRequestPair : public castor::IObject {

    public:

      /**
       * Constructor.
       */
      DriveAndRequestPair() throw();

      /**
       * Sets the id of the object
       * @param id The new id
       */
      virtual void setId(u_signed64 id);

      /**
       * gets the id of the object
       */
      virtual u_signed64 id() const;

      /**
       * Gets the type of the object
       */
      virtual int type() const;

      /**
       * virtual method to clone any object
       */
      virtual IObject* clone();

      /**
       * Outputs this object in a human readable format
       * @param stream The stream where to print this object
       * @param indent The indentation to use
       * @param alreadyPrinted The set of objects already printed.
       * This is to avoid looping when printing circular dependencies
       */
      virtual void print(std::ostream& stream,
                         std::string indent,
                         castor::ObjectSet& alreadyPrinted) const;

      /**
       * Outputs this object in a human readable format
       */
      virtual void print() const;

      /**
       * Sets the pointer to the tape drive.
       */
      void setTapeDrive(castor::vdqm::TapeDrive *tapeDrive) throw();

      /**
       * Returns the pointer to the tape drive.
       */
      castor::vdqm::TapeDrive *tapeDrive() const throw();

      /**
       * Sets the pointer to the tape request.
       */
      void setTapeRequest(castor::vdqm::TapeRequest* tapeRequest) throw();

      /**
       * Returns the pointer to the tape request.
       */
      castor::vdqm::TapeRequest* tapeRequest() const throw();


    private:

      /**
       * The id of this object.
       */
      u_signed64 m_id;

      /**
       * Pointer to the tape drive.
       */
      castor::vdqm::TapeDrive* m_tapeDrive;

      /**
       * Pointer to the tape request.
       */
      castor::vdqm::TapeRequest* m_tapeRequest;

    }; // class DriveAndRequestPair

  } // namespace vdqm

} // namespace castor

#endif // CASTOR_VDQM_DRIVEANDREQUESTPAIR_HPP
