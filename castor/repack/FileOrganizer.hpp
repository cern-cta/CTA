#ifndef _FILEORGANIZER_HPP_
#define _FILEORGANIZER_HPP_

#include "RepackCommonHeader.hpp"
 
namespace castor {
	
	namespace repack {

	/**
     * class FileOrganizer
     */
	class FileOrganizer : public castor::BaseObject
	{
		public : 
		
		/**
	     * Empty Constructor
	     */
		FileOrganizer();
		
		/**
	     * Destructor
	     */
		~FileOrganizer();
		
		/**
		 * Stages the files
		 */
		stage_files(RepackRequest* rreq) throw();
		
		private:

		
	};



	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR



#endif //_FILEORGANIZER_HPP_
