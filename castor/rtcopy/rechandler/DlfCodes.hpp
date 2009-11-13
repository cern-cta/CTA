/*********************************************************************************/
/* File including the constants to represent the DLF Messages for the new */
/*******************************************************************************/

#ifndef RECHANDLER_DLF_MESSAGES_HPP
#define RECHANDLER_DLF_MESSAGES_HPP 1


namespace castor {
  namespace rtcopy {
    namespace rechandler {

      enum DlfCodes{

	DAEMON_START=1,
	DAEMON_STOP = 2,
	NO_POLICY = 3,
	FATAL_ERROR = 4,
	TAPE_NOT_FOUND = 5,
	TAPE_FOUND = 6,
	POLICY_INPUT = 7,
	ALLOWED_WITHOUT_POLICY = 8,
	ALLOWED_BY_POLICY = 9,
	PRIORITY_SENT = 10,
	NOT_ALLOWED = 11,
	PYTHON_ERROR = 12,
	RESURRECT_TAPES = 14

      };
    }// end namespace mighunter
  }// end namespace rtcopy
}// end namespace castor

#endif

