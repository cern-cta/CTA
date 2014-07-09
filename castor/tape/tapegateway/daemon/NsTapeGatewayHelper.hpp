/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

// Include Files

#include "castor/exception/Exception.hpp"

namespace castor {

  namespace tape {
    
    namespace tapegateway {

      class NsTapeGatewayHelper {

        public:

	void checkFseqForWrite (const std::string &vid, int Fseq)
          ;

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

        class SuperfluousSegmentException: public castor::exception::Exception {
          public:
          SuperfluousSegmentException(int error_number):castor::exception::Exception(error_number){};
        };
    
      };
    
    } // end of namespace tapegateway

  } // end of namespace tape

}  // end of namespace castor

