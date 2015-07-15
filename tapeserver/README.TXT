The CTA/tapeserver/castor directory started as a partial is copy of a git
checkout of:

    https://:@git.cern.ch/kerberos/CASTOR

at the following commit:

    commit f4cb056d4dc487112892a236dc5bdb04251344b8
    Author: Sebastien Ponce <sebastien.ponce@cern.ch>
    Date:   Wed Jul 15 09:15:29 2015 +0200

        Close properly hbase connections in case of broken pipe, avoiding socket leak
