/*COPYRIGHT>
 *
 *   Copyright (c) International Business Machines  Corp., 2003
 *
 *   This program is free software;  you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY;  without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See
 *   the GNU General Public License for more details.
 *
 *   You should have received a copy of the GNU General Public License
 *   along with this program;  if not, write to the Free Software
 *   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 <COPYRIGHT*/

#ifndef H_STFSPERFMONLIB
#define H_STFSPERFMONLIB

/*
 *  Name: 
 *        stfsperfmonlib.h
 *
 *  Component:
 *        stfsperfmonlib
 *
 *  Description:
 *        STFS performance monitor library interface.
 *
 *  Environment:
 *        Linux
 *
 */

#include <stdint.h> /* Import uint64_t. */

#define STFS_PM_VERSION_MAJOR  0
#define STFS_PM_VERSION_MINOR  4

/*-----------------------------------------------------------------------------
*         Error Codes.
*----------------------------------------------------------------------------*/

#define STFS_PMRC_SUCCESS         0
#define STFS_PMRC_BAD_ADDRESS     1
#define STFS_PMRC_NOT_CONNECTED   2
#define STFS_PMRC_NOT_FOUND       3

/*-----------------------------------------------------------------------------
*         Exported Types.
*----------------------------------------------------------------------------*/

struct stfs_pm_iostats
{
    uint64_t     io_rd_count;        /* Cumulative read count (bytes).  */
    uint64_t     io_wr_count;        /* Cumulative write count (bytes). */

    uint32_t     io_interval;        /* Basic sample interval (seconds). */
    float        io_rd_rate;         /* Read Rate (MB/sec) over last
                                        basic sample interval. */
    float        io_wr_rate;         /* Write Rate (MB/sec) over last
                                        basic sample interval. */

    uint32_t     io_ltinterval;      /* Long-term sample interval (seconds). */
    float        io_rd_ltrate;       /* Read Rate (MB/sec) over last
                                        long-term sample interval. */
    float        io_wr_ltrate;       /* Write Rate (MB/sec) over last
                                        long-term sample interval. */

    float        io_rd_rating;       /* Read rating (MB/sec).      */
    float        io_wr_rating;       /* Write rating (MB/sec).     */
    float        io_rdwr_rating;     /* Read-write rating (MB/sec. */

    uint64_t     total_space;        /* Total space in bytes */
    uint64_t     free_space;         /* Free space in bytes */
};

/*-----------------------------------------------------------------------------
*         Exported Functions.
*----------------------------------------------------------------------------*/

#ifdef __cplusplus
#define CPREFIX "C"
#else
#define CPREFIX 
#endif

/*
 * Action: Resolve given address for stfsperfmon collector.
 *         Attempt to connect.
 * Return: STFS_PMRC_SUCCESS indicates success.
 *         STFS_PMRC_BAD_ADDRESS if address could not be resolved.
 *         STFS_PMRC_NOT_CONNECTED if connection could not be made at this time.
 */
extern CPREFIX int stfsperfmon_initialize ( const char *ip, const int portNo ) ;


/*
 * Action: Get the read/write SECTOR count for the given "file system".
 * Return: STFS_PMRC_SUCCESS indicates success. rd/wr contain the stats.
 *         STFS_PMRC_NOT_CONNECTED if connection could not be made at this time.
 *         STFS_PMRC_NOT_FOUND if fileset by the given name was not found.
 */
extern CPREFIX int stfsperfmon_get_fs_statistics( const char  *fsName,
                                                  uint64_t    *rd,
                                                  uint64_t    *wr,
						  uint64_t    *total,
						  uint64_t    *free );


/*
 * Action: Get the IO statistics for the given pool.
 * Return: STFS_PMRC_SUCCESS indicates success. *iostatsP contains the stats.
 *         STFS_PMRC_NOT_CONNECTED if connection could not be made at this time.
 *         STFS_PMRC_NOT_FOUND if pool by the given name was not found.
 */
#define STFS_PM_RDWR_MAX ((uint64_t) -1u)

extern CPREFIX int stfsperfmon_get_pool_stats( const char             *poolName,
                                               struct stfs_pm_iostats *statsP );

#endif /* H_STFSPERFMONLIB */

