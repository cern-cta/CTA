/******************************************************************************
 *                      NsTapeGatewayHelper.hpp
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
 * @(#)$RCSfile: NsTapeGatewayHelper.hpp,v $ $Revision: 1.8 $ $Release$ $Date: 2009/05/18 13:52:38 $ $Author: gtaur $
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#ifndef TAPEGATEWAY_NSTAPEGATEWAYHELPER_HPP
#define TAPEGATEWAY_NSTAPEGATEWAYHELPER_HPP 

// Include Files

#include "castor/exception/Exception.hpp"

#include "castor/tape/tapegateway/FileMigratedNotification.hpp"
#include "castor/tape/tapegateway/FileRecalledNotification.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"


namespace castor {

  namespace tape {
    
    namespace tapegateway {

      class NsTapeGatewayHelper {

        public:

	void updateMigratedFile(tape::tapegateway::FileMigratedNotification& file,
                                int copyNumber, std::string vid,
                                u_signed64 lastModificationTime)
          throw (castor::exception::Exception);

	void updateRepackedFile(tape::tapegateway::FileMigratedNotification& file,
                                int originalCopyNumber, std::string originalVid,
                                int copyNumber, std::string vid,
                                u_signed64 lastModificationTime)
          throw (castor::exception::Exception);

	void checkRecalledFile(castor::tape::tapegateway::FileRecalledNotification& file,
                               std::string vid, int copyNb)
          throw (castor::exception::Exception);

	void checkFileSize(castor::tape::tapegateway::FileRecalledNotification& file)
          throw (castor::exception::Exception);

	void getBlockIdToRecall(tape::tapegateway::FileToRecall& file, std::string vid)
          throw (castor::exception::Exception);

	void checkFseqForWrite (const std::string &vid, int side, int Fseq)
          throw (castor::exception::Exception);

	/* Ad-hoc exceptions */
	class NoSuchFileException: public castor::exception::Exception {
	  public:
	  NoSuchFileException():castor::exception::Exception(ENOENT){};
	};

	class FileMutatedException: public castor::exception::Exception {
        public:
	  FileMutatedException():castor::exception::Exception(ESTALE){};
	};

        class FileMutationUnconfirmedException: public castor::exception::Exception {
        public:
          FileMutationUnconfirmedException():castor::exception::Exception(ESTALE){};

        };
      };
    } // end of namespace tapegateway

  } // end of namespace tape

}  // end of namespace castor

#endif // TAPEGATEWAY_NSTAPEGATEWAYHELPER_HPP 
