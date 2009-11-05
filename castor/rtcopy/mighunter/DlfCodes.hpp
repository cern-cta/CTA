/*********************************************************************************/
/* File including the constants to represent the DLF Messages for the new */
/*******************************************************************************/

#ifndef MIGHUNTER_DLF_MESSAGES_HPP
#define MIGHUNTER_DLF_MESSAGES_HPP 1


namespace castor {
  namespace rtcopy {
    namespace mighunter{

      enum DlfCodes{

	DAEMON_START = 1,
	DAEMON_STOP = 2,
	NO_RETRY_POLICY_FOUND = 3,
	FATAL_ERROR  = 4, 
	NO_POLICY = 5,
	PARSING_OPTIONS = 6,
	NO_TAPECOPIES = 7,
	TAPECOPIES_FOUND = 8,
	NO_TAPEPOOLS = 9, 
	NOT_ENOUGH = 10,
	NO_DRIVES = 11, 
	POLICY_INPUT = 12, 
	ALLOWED_WITHOUT_POLICY = 13,
	ALLOWED_BY_POLICY = 14, 
	NOT_ALLOWED = 15,
	POLICY_RESULT = 16,
	ATTACHED_TAPECOPIES = 17,
	DB_ERROR = 18,
	RESURRECT_TAPECOPIES = 19,
	INVALIDATE_TAPECOPIES = 20,
	NS_ERROR = 21,
	NO_STREAM = 22,
	STREAMS_FOUND = 23,
	STREAM_INPUT = 24,
	START_WITHOUT_POLICY = 25,
	START_BY_POLICY = 26, 
	NOT_STARTED = 27,
	STREAM_POLICY_RESULT = 28,
	STARTED_STREAMS = 29,
	STOP_STREAMS = 30

      };
    }// end namespace mighunter
  }// end namespace rtcopy
}// end namespace castor

#endif

