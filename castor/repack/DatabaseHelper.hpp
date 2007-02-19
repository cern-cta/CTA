
/******************************************************************************
 *                      castor/repack/DatabaseHelper.hpp
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
 * @(#)$RCSfile: DatabaseHelper.hpp,v $ $Revision: 1.16 $ $Release$ $Date: 2007/02/19 11:06:52 $ $Author: gtaur $
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef CASTOR_REPACK_DATABASEHELPER_HPP
#define CASTOR_REPACK_DATABASEHELPER_HPP

// Include Files
#include "castor/repack/RepackCommonHeader.hpp"
#include <vector>
#include "castor/BaseAddress.hpp"
#include "castor/Services.hpp"
#include "castor/db/DbBaseObj.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/exception/SQLError.hpp"
#include "stager_client_api.h"


namespace castor {

  namespace repack {

    // Forward declarations
    class RepackRequest;
    class IObject;
    class Services;


    /**
     * class DatabaseHelper
     *
     */
    class DatabaseHelper  : public castor::db::DbBaseObj{

    public:

      /**
       * Empty Constructor
       */
      DatabaseHelper() ;

      /**
       * Empty Destructor
       */
      virtual ~DatabaseHelper() throw() ;

      /**
       * Stores a RepackRequest in the Database
       * @param rreq The RepackRequest
       */
      void storeRequest(castor::repack::RepackRequest* rreq)
        throw(castor::exception::Exception);


      /**
       * Selects a RepackSubRequest as to be updated.
       * After calling this method, the unlock has to be executed to commit.
       * otherwise the table entry is locked forever.
       * @exception Exception if the RepackSubRequest was not found.
       */
      void lock(castor::repack::RepackSubRequest* tape) throw (castor::exception::Exception);
      /**
       * Executes an explicit commit on the db connection. Since the statements are all FOR UPDATE
       * (pessimistic CC), we have to do a commit, even if we didn't do anything with a table row.
       */
      void unlock() throw ();


      /**
       * Resets the converter. In particular any prepared
       * statements are destroyed.
       */
      virtual void reset() throw();

      /**
       * updates the RepackSubRequest. For logging purpose the cuuid is given as
       * parameter.
       * @param obj The RepackSubRequest, which is to be updated
       * @param cuuid The Cuuid for following the dlf messages
       * @throws castor::exception::exception in case of an error
       */
      void updateSubRequest(castor::repack::RepackSubRequest* obj, bool fill,Cuuid_t& cuuid)
        throw(castor::exception::Exception);

      /**
       * Checks the RepackDB for SubRequests in a certain status.
       * The returned Object is filled (all segments and the corresponding
       * RepackRequest This means that the caller has to free the
       * allocated memory.
       * Be aware : the affected table is locked! do an unlock() after use;
       * @param status The status to be queried
       * @return RepackSubRequest a full RepackSubRequest Object
       * @throws castor::exception::exception in case of an error
       */
      RepackSubRequest* checkSubRequestStatus(int status)
        throw(castor::exception::Exception);

      /**
       * Removes an RepackRequest,RepackSubRequest or even RepackSegment from DB
       * @param obj The object to be removed
       * @throws castor::exception::exception in case of an error
       */
      void remove(castor::IObject* obj) throw(castor::exception::Exception) ;


      /**
       * Retrieves and RepackSubRequest from DB by a given volume id
       * @param vid the volume name to search for
       * @param fill flag, if the segments should be added
       * @throws castor::exception::exception in case of an error
       */
      RepackSubRequest* getSubRequestByVid(std::string vid, bool fill)
        throw(castor::exception::Exception);

      /**
       * Checks,wether a Tape is already stored in the RepackSubRequest Table.
       * This is needed before a Tape is inserted as a new repackjob.
       * @throws castor::exception::exception in case of an error
       */
      bool is_stored(std::string vid) throw(castor::exception::Exception);

      /**
       * Gets all RepackSubRequests from the DB
       * @return an pointer to a vector of Repack SubRequests
       * @throws castor::exception::exception in case of an error
       */
      std::vector<castor::repack::RepackSubRequest*>*
      getAllSubRequests() throw (castor::exception::Exception);

      /**
       * Gets all RepackSubRequests in a certain status from the DB
       * Be aware : the affected table is locked! do an unlock() after use;
       * @return an pointer to a vector of Repack SubRequests
       * @throws castor::exception::exception in case of an error
       */
      std::vector<castor::repack::RepackSubRequest*>*
      getAllSubRequestsStatus(int status)
        throw (castor::exception::Exception);

      /**
       * Returns the already existing RepackSegment in the DB.
       * Note that even a file is multi segmented the file is being
       * repacked only for one segment.
       */
      RepackSegment* getTapeCopy(RepackSegment* lookup)
        throw (castor::exception::Exception);




      RepackRequest* getLastTapeInformation(std::string vidName)
        throw (castor::exception::Exception);




      /** Archives the finished RepackSubRequests.
       * It just updates the DB for Requests in SUBREQUEST_DONE
       * to SUBREQUEST_ARCHIVED.
       * @throw castor::exception::exception in case of an error
       */
      void archive() throw (castor::exception::Exception);



    private:

      /**
       * Little Helper to get a get a SubRequest from DB by the rowid.
       * @param sub_id the id of the RepackSubRequest
       * @throws castor::exception::exception in case of an error
       */
      RepackSubRequest* getSubRequest(u_signed64 sub_id)
        throw(castor::exception::Exception);

      /**
       * Gets a bunch of RepackSubRequest from DB. This method is only used
       * internally.
       * @param castor::db::IDbStatement The Statement to get the SubRequests
       * @throw castor::exception::exception in case of an error
       * @return pointer to vector of RepackSubRequests
       */
      std::vector<RepackSubRequest*>* internalgetSubRequests
      (castor::db::IDbStatement* statement)
        throw (castor::exception::Exception);


      /** The SQL Statement Objects and their strings */

      static const std::string s_selectCheckStatementString;
      castor::db::IDbStatement *m_selectCheckStatement;

      static const std::string s_selectCheckSubRequestStatementString;
      castor::db::IDbStatement *m_selectCheckSubRequestStatement;

      static const std::string s_selectAllSubRequestsStatementString;
      castor::db::IDbStatement *m_selectAllSubRequestsStatement;

      static const std::string s_selectExistingSegmentsStatementString;
      castor::db::IDbStatement *m_selectExistingSegmentsStatement;

      static const std::string s_isStoredStatementString;
      castor::db::IDbStatement *m_isStoredStatement;

      static const std::string s_selectAllSubRequestsStatusStatementString;
      castor::db::IDbStatement *m_selectAllSubRequestsStatusStatement;

      static const std::string s_archiveStatementString;
      castor::db::IDbStatement  *m_archiveStatement;

      static const std::string s_selectLockStatementString;
      castor::db::IDbStatement *m_selectLockStatement;

      static const std::string s_selectLastSegmentsSituationStatementString;
      castor::db::IDbStatement *m_selectLastSegmentsSituationStatement;

      castor::BaseAddress ad;

    }; // end of class DatabaseHelper

  }; // end of namespace repack

}; // end of namespace castor

#endif // CASTOR_REPACK_DATABASEHELPER_HPP
