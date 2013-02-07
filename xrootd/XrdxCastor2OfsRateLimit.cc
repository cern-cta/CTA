//          $Id: XrdxCastor2OfsRateLimit.cc,v 1.2 2009/03/06 13:45:04 apeters Exp $

#include "XrdxCastor2Ofs.hh"
#include "XrdSys/XrdSysTimer.hh"

#include <math.h>

/******************************************************************************/
/*                    T h r e a d 4 R a t e L i m i t a t i o n               */
/******************************************************************************/

#define RATEPERIOD 10
#define PROCUPDATEPERIOD 10

/******************************************************************************/
/*                           E x e c u t o r                                  */
/******************************************************************************/

void *XrdxCastor2OfsRateLimitStart(void *pp) {
  XrdxCastor2OfsRateLimit *gc = (XrdxCastor2OfsRateLimit*)pp;
  gc->Limit();
  return (void*)0;
}

/******************************************************************************/
/*                           I n i t                                          */
/******************************************************************************/
bool
XrdxCastor2OfsRateLimit::Init(XrdSysError &ep) {
  pthread_t tid;
  int rc;
  eDest = &ep;
  eDest->Say("###### " ,"ratelimiter: starting thread ","");

  if ((rc = XrdSysThread::Run(&tid, XrdxCastor2OfsRateLimitStart, static_cast<void *>(this),
			      0, "Garbage Collector"))) {
    ep.Emsg("ratelimiter",rc,"create rater limiter thread");
    return false;
  }
  return true;
}

/******************************************************************************/
/*                           L i m i t                                        */
/******************************************************************************/
void 
XrdxCastor2OfsRateLimit::Limit() {
  long long readstartbytes;
  long long writestartbytes;
  long long readstopbytes[RATEPERIOD];
  long long writestopbytes[RATEPERIOD];
  int sleeptime=500000;
  float readdesiredrate=100.0;
  float writedesiredrate=100.0;
  float readratemissing=0;
  float writeratemissing=0;
  float readsleeper=0;
  float writesleeper=0;
  int cnt=0;
  float lastreadsleeper=0;
  float lastwritesleeper=0;
  time_t procupdatetime;

  procupdatetime = time(NULL) + PROCUPDATEPERIOD;

  while(1) {
    char buff[1024];

    readdesiredrate  = XrdxCastor2OfsFS.ReadRateLimit;
    writedesiredrate = XrdxCastor2OfsFS.WriteRateLimit;
    readratemissing  = (float) XrdxCastor2OfsFS.ReadRateMissing;
    writeratemissing = (float) XrdxCastor2OfsFS.WriteRateMissing;
    if (XrdxCastor2OfsFS.ReadRateMissing) {
      readdesiredrate = (XrdxCastor2OfsFS.ReadRateLimit*1.0 - XrdxCastor2OfsFS.ReadRateMissing*1.0);
      //      printf("Missing Rate %d/%d -> %f\n",XrdxCastor2OfsFS.ReadRateLimit,XrdxCastor2OfsFS.ReadRateMissing, readdesiredrate);
      if (!(cnt % 4)) {
	XrdxCastor2OfsFS.RateMissingCnt++;
	XrdxCastor2OfsFS.ReadRateMissing=0;
      }
    } else {
      //      printf("No missing Read rate\n");
    }
    
    // we always reserve 10 Mb/s for all default users
    if (readdesiredrate <0) {
      readdesiredrate = 10.0;
    }

    if (XrdxCastor2OfsFS.WriteRateMissing) {
      writedesiredrate = (XrdxCastor2OfsFS.WriteRateLimit*1.0 - XrdxCastor2OfsFS.WriteRateMissing*1.0);
      //      printf("Missing Rate %d/%d -> %f\n",XrdxCastor2OfsFS.WriteRateLimit,XrdxCastor2OfsFS.WriteRateMissing, writedesiredrate);
      if (!(cnt % 4)) {
	XrdxCastor2OfsFS.RateMissingCnt++;
	XrdxCastor2OfsFS.WriteRateMissing=0;
      }
    } 
    
    // we always reserve 10 Mb/s for all default users
    if (writedesiredrate <0) {
      writedesiredrate = 10.0;
    }

    cnt++;

    readstartbytes = readstopbytes[cnt%RATEPERIOD];
    writestartbytes = writestopbytes[cnt%RATEPERIOD];

    XrdSysTimer::Wait(sleeptime/1000);

    readstopbytes[cnt%RATEPERIOD]  = XrdxCastor2OfsFS.TotalReadBytes;
    writestopbytes[cnt%RATEPERIOD]  = XrdxCastor2OfsFS.TotalWriteBytes;

    // keep the proc values more or less uptodate
    if ( time(NULL) > procupdatetime ) {
      if (!XrdxCastor2OfsFS.UpdateProc("*")) {
	sprintf(buff,"error updating /proc information");
	eDest->Say("*****> mainRateLimiter ",0, buff);
      }
      procupdatetime = (time(NULL) + PROCUPDATEPERIOD);
    }

    if (cnt>RATEPERIOD)
    {
      {
	// read
	float rate = (1.0 * (readstopbytes[cnt%RATEPERIOD]-readstartbytes)/(sleeptime*RATEPERIOD));
	float rateweight;
	float ratefraction;
	rateweight = fabs(readdesiredrate-rate)/readdesiredrate;
	ratefraction = 1.0*rate/readdesiredrate;
	if (ratefraction <0.9) {
	  ratefraction = 0.9;
	}
	if (rate> readdesiredrate) {
	  readsleeper += (int)(100000*rateweight) ;
	} else {
	  readsleeper *= (float)ratefraction;
	}
	if (readsleeper <1.0)
	  readsleeper = 0;
	
	if (rate < 0.5) {
	  // we just remove the delay for this very small rates
	  readsleeper = 0;
	}
	XrdxCastor2OfsFS.ReadDelay = (unsigned int)(readsleeper/1000.0); // we need ms
	
	if (readsleeper != lastreadsleeper) {
	  if (readsleeper>0) {
	    sprintf(buff,"limiter-enabled : delaying by %f ms - measured read rate %.02f Mb/s -> desired %.02f mb/s [missing %.02f mb/s]",readsleeper/1000,rate,readdesiredrate,readratemissing);
	    eDest->Say("*****> mainRateLimiter ",0, buff);
	  } else {
	    sprintf(buff,"limiter-disabled: no delay          - measured read rate %.02f Mb/s -> desired rate %.02f mb/s [missing %.02f mb/s]",rate,readdesiredrate, readratemissing);
	    eDest->Say("*****> mainRateLimiter ",0, buff);
	  }
	  lastreadsleeper = readsleeper;
	}
	XrdxCastor2OfsFS.MeasuredReadRate = (int)rate;
      }

      {
	// write
	float rate = (1.0 * (writestopbytes[cnt%RATEPERIOD]-writestartbytes)/(sleeptime*RATEPERIOD));
	float rateweight;
	float ratefraction;
	rateweight = fabs(writedesiredrate-rate)/writedesiredrate;
	ratefraction = 1.0*rate/writedesiredrate;
	if (ratefraction <0.9) {
	  ratefraction = 0.9;
	}
	if (rate> writedesiredrate) {
	  writesleeper += (int)(100000*rateweight) ;
	} else {
	  writesleeper *= (float)ratefraction;
	}
	if (writesleeper <1.0)
	  writesleeper = 0;
	
	if (rate < 0.5) {
	  // we just remove the delay for this very small rates
	  writesleeper = 0;
	}
	XrdxCastor2OfsFS.WriteDelay = (unsigned int)(writesleeper/1000.0); // we need ms
	
	if (writesleeper != lastwritesleeper) {
	  if (writesleeper>0) {
	    sprintf(buff,"limiter-enabled : delaying by %f ms - measured write rate %.02f Mb/s -> desired %.02f mb/s [missing %.02f mb/s]",writesleeper/1000,rate,writedesiredrate,writeratemissing);
	    eDest->Say("*****> mainRateLimiter ",0, buff);
	  } else {
	    sprintf(buff,"limiter-disabled: no delay          - measured write rate %.02f Mb/s -> desired rate %.02f mb/s [missing %.02f mb/s]",rate,readdesiredrate,writeratemissing);
	    eDest->Say("*****> mainRateLimiter ",0, buff);
	  }
	  lastwritesleeper = writesleeper;
	}
	XrdxCastor2OfsFS.MeasuredWriteRate = (int)rate;
      }
    }
  }
}
