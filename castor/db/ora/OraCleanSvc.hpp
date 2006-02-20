#ifndef ORA_ORACLEANSVC_HPP
#define ORA_ORACLEANSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/db/ora/OraCommonSvc.hpp"
//#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/cleaning/ICleanSvc.hpp"
#include "occi.h"
#include <vector>

namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the ICleanSvc for Oracle
       */
      class OraCleanSvc : public OraCommonSvc,
                         public virtual castor::cleaning::ICleanSvc {

      public:

        /**
         * default constructor
         */
        OraCleanSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraCleanSvc() throw();

        /**
         * Get the service id
         */
        virtual inline const unsigned int id() const;

        /**
         * Get the service id
         */
        static const unsigned int ID();

        /**
         * Reset the converter statements.
         */
        void reset() throw ();

      public:

         /*
         * @param 
         * @return 
         * @exception 
         */

        int removeOutOfDateRequests(int numDays)
        throw (castor::exception::Exception);
         
         /*
         * @param 
         * @return 
         * @exception 
         */

        int  removeArchivedRequests(int hours)
        throw (castor::exception::Exception);

         /*
         * @param 
         * @return 
         * @exception 
         */


      private:

        /// SQL statement for function removeOutOfDateRequests
        static const std::string s_removeOutOfDateRequestsString;

        /// SQL statement object for removeOutOfDateRequests 
        oracle::occi::Statement *m_removeOutOfDateRequestsStatement;

        /// SQL statement for removeArchivedRequests function
        static const std::string s_removeArchivedRequestsString;

        /// SQL statement object for removeArchivedRequests function
        oracle::occi::Statement *m_removeArchivedRequestsStatement;


      }; // end of class OraCleanSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORACLEANSVC_HPP
