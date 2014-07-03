/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 *
 * @author Ben Couturier
 *****************************************************************************/

 * @version $Revision: 1.2 $
 * @date $Date: 2004/10/29 13:31:07 $
 */
/** @mainpage CASTOR Disk Mover Interface
 *
 * @section intro Introduction
 * In the new CASTOR stager, it is foreseen to use other movers than RFIO, that
 * was mandatory in the previous stager. For that purpose, a new API has been defined
 * to allow the integration of other movers, e.g ROOTD or XROOTD.
 *
 * @section overview Overview
 *
 * The mover client should can put/get files from the CASTOR stager using the standard
 * CASTOR client API (see stager_client_api.h). The API allows it to stage the file in, 
 * and to know on which diskserver the file has been staged.
 *
 * The mover API allows a data mover running on a disk server to interface with the CASTOR
 * stager. It is defined below, and allows to send signals to the stager, 
 * so as to indicate that:
 *
 * - a file must be opened, and doing a name translation in case the
 * the client does not have the physical pathname.
 * - a file is being written to. This is useful for the stager, so as to disable
 * other read-only copies it might have of the given file.
 * - a file is being closed (the stager might use this to see whether it should migrate 
 * the file)
 * - there is a problem with an I/O operation, for example that the disk server is full
 */

#pragma once

#include <osdep.h>


/** \addtogroup ServerAPI */
/*\@{*/


/**********************************************************/
/*    stage_mover_open                                    */
/**********************************************************/
		       

		       
/**
 * stage_mover_open
 * Allows the mover to translate a site filename into a
 * physical filename. Also checks with the stager that the user
 * is allowed to open that that file.
 *
 * \ingroup ServerAPI
 * 
 * @param sfn            The site filename of the file the mover wants to open
 * @param mode           The mode in which the user wants to open the file
 * @param userid         The identity of the user requesting access to the file 
 * @param pfn            The physical filemame of the file on the local disk
 * @param stagerContext  An opaque structure, used by the stager match the 
 *                       different calls for the same request.
 *
 * @returns 0 in case of success, -1 otherwise
 * @note the pfn is allocated by the call, and therefore
 *       should be freed by the mover.
 */
EXTERN_C int stage_mover_open (const char *sfn,
			       int mode,
			       const char *userId,
			       char **pfn,
			       void **stagerContext);



/**********************************************************/
/*    stage_mover_write                                   */
/**********************************************************/
		       

		       
/**
 * stage_mover_write
 * Using this call, the mover can indicate to the stager that
 * a file opened in O_RDWR or O_RDONLY is being modifed by the 
 * by the client.
 * 
 * \ingroup ServerAPI
 * 
 * @param stagerContext  The stager context
 *
 * @returns 0 in case of success, -1 otherwise
 */
EXTERN_C int stage_mover_write (void *stagerContext);



/**********************************************************/
/*    stage_mover_close                                   */
/**********************************************************/
		       

		       
/**
 * stage_mover_close
 * Indicates to the stager that the client has requested that the
 * file be closed.
 * 
 * \ingroup ServerAPI
 * 
 * @param stagerContext  The stager context
 *
 * @returns 0 in case of success, -1 otherwise
 */
EXTERN_C int stage_mover_close (void *stagerContext);


/**********************************************************/
/*    stage_mover_error                                   */
/**********************************************************/


		       
/**
 * stage_mover_error
 * Warns the stager that there has been a problem while accessing the file on disk.
 * This can be used by the stager to flag the file as inaccessible.
 * 
 * \ingroup ServerAPI
 * 
 * @param stagerContext  The stager context for this request
 * @param errorCode  The code for the errors
 * @param errorSting Description of the problem 
 *
 * @returns 0 in case of success, -1 otherwise
 */
EXTERN_C int stage_mover_error (void *stagerContext,
				const int  errorCode,
				const char *errorString);



/*\@}*/

