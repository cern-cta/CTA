/*
 * $Id: msg.h,v 1.1 2000/08/21 14:52:01 baud Exp $
 */

/*
 * @(#)$RCSfile: msg.h,v $ $Revision: 1.1 $ $Date: 2000/08/21 14:52:01 $ CERN IT-PDP/DC Antoine Trannoy
 */

/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DC
 * All rights reserved
 */

/* msg.h        SHIFT message system header file.                       */

#ifndef __MSG_H__
#define __MSG_H__

/*
 * Key number for communications with message daemon.
 */
#define MSG_MAGIC	0x0333

/*
 * Maximun message size.
 */
#define MSGSIZ		256

/*
 * Possible requests, and acknowledgment, to message daemon.
 */
#define RQST_MSGR	0x1001	/* Sending a message which has to be  	*/
#define ACKN_MSGR	0x2001	/* kept until a reply is given.		*/

#define RQST_MSGI	0x1002	/* Sending informative message.		*/
#define ACKN_MSGI	0x2002

#define RQST_REPL	0x1003	/* Operator sending a reply.		*/
#define ACKN_REPL	0x2003

#define RQST_GETR	0x1004	/* Getting all messages waiting for an 	*/
#define ACKN_GETR	0x2004	/* answer.				*/

#define RQST_GETI	0x1005	/* Getting all informative messages.	*/
#define ACKN_GETI	0x2005

#define RQST_CANC	0x1006	/* Canceling a message.			*/
#define ACKN_CANC	0x2006

#define GIVE_REPL	0x1007	/* Sending the reply to the user.	*/

#endif 	/*	__MSG_H__	*/
