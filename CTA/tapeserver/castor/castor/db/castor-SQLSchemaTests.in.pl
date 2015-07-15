#!/usr/bin/perl -w
#
# This test script will install the SQL schema scripts of castor and return
# an error if any of them does not compile properly
#

use strict;
use IPC::Open2;
use IO::Select;
use Fcntl qw(:seek);

sub main();
my %parameters;
my $confFile = "/etc/castor/SQLSchemaTests.conf";

main;

sub connString ( $ ) {
  my $schema = shift;
  open(CONF, "< $confFile")
    or die "Could not open $confFile for reading: $!";
  while(<CONF>) {
    if ( /^$schema=(.+\/.+\@.+)$/ ) {
      close CONF;
      return $1;
    }
  }
  close CONF;
  die "Could not find connection string for schema $schema";
}

sub connParams ( $ ) {
  my $schema = shift;
  open(CONF, "< $confFile")
    or die "Could not open $confFile for reading: $!";
  while(<CONF>) {
    if ( /^$schema=(.+)\/(.+)\@(.+)$/ ) {
      close CONF;
      return ( $1, $2, $3 );
    }
  }
  close CONF;
  die "Could not find connection string for schema $schema";
}

sub getParamaters () {
  my @params = ( "stageUid", "stageGid", "adminList", "instanceName",
    "stagerNsHost" );
  open(CONF, "< $confFile")
    or die "Could not open $confFile for reading: $!";
  foreach my $param (@params) {
    my $found = 0;
    seek(CONF, 0, SEEK_SET);
    while (<CONF>) {
      if ( /^$param=(.*)$/ ) {
        $parameters{$param}=$1;
        $found = 1;
        last;
      }
    }
    if ( !$found ) {
      close CONF;
      die "Could not find parameter $param in $confFile";
    }
  }
  close (CONF);
}

sub filterSQL ( $$$$$ ) {
  my $line = shift;
  my ( $NsDbUser, $NsDbPasswd, $NsDbName ) =  ( shift, shift, shift );
  my ( $vmgrSchema ) = ( shift );
  $line=~ s/^ACCEPT/--ACCEPT/;
  $line=~ s/^PROMPT/--PROMPT/;
  $line=~ s/^UNDEF/--UNDEF/;
  $line=~ s/\&stageUid/$parameters{stageUid}/g;
  $line=~ s/\&stageGid/$parameters{stageGid}/g;
  $line=~ s/\&adminList/$parameters{adminList}/g;
  $line=~ s/\&instanceName/$parameters{instanceName}/g;
  $line=~ s/\&stagerNsHost/$parameters{stagerNsHost}/g;
  $line=~ s/\&cnsUser/$NsDbUser/g;
  $line=~ s/\&cnsPasswd/$NsDbPasswd/g;
  $line=~ s/\&cnsDbName/$NsDbName/g;
  $line=~ s/\&vmgrSchema\./$vmgrSchema/g;
  $line=~ s/\&cnsSchema/$NsDbUser/g;
  return $line;
}

sub printFiltered ( $ ) {
  my $allLines = shift;
  foreach my $line ( split ( /\n/, $allLines ) ) {
    if ( ! ( $line =~ /^$/ || 
             $line =~ /^(\d+ rows?|Table|Package(| body)|Type|Function|Procedure|Index|Sequence|View|Synonym|Commit|Grant|Trigger)\s+(created|updated|altered|complete|succeeded)\.\w?$/ ||
             $line =~ /PL\/SQL procedure successfully completed./ ) ) {
      print "    $line\n";
    }
  }
}

sub main () {
  my $check_compilation = "
  whenever sqlerror exit SQL.SQLCODE
  set serveroutput on;
  declare
    nerrors NUMBER;
    newnerrors NUMBER;
    e exception;
    pragma exception_init( e, -255 );
  begin
    select count(*)
      into nerrors
      from user_objects
     where object_type = 'PROCEDURE'
       and status = 'INVALID';
    while nerrors != 0
    loop
      for proc in (
        select object_name
         from user_objects
        where object_type = 'PROCEDURE'
          and status = 'INVALID')
      loop
        begin
          execute immediate 'ALTER PROCEDURE ' || proc.object_name || ' COMPILE';
        exception when others then
          null;
        end;
      end loop;
      select count(*)
       into newnerrors
       from user_objects
      where object_type = 'PROCEDURE'
        and status = 'INVALID';
      if newnerrors = nerrors then
        DBMS_OUTPUT.put_line('Failed to compile all objects');
        for obj in (
        select object_name, object_type
         from user_objects
        where  status = 'INVALID')
        loop
          DBMS_OUTPUT.put_line( obj.object_type||' '||obj.object_name|| ' is still invalid');
        end loop;
        raise e;
      else
        nerrors := newnerrors;
      end if;
    end loop;
  end;
  /
  exit 0
  ";
  my @SQLTests = ( "vmgr", "cns", "cupv", "vdqm", "stager");
  my ( $NsDbUser, $NsDbPasswd, $NsDbName ) =  connParams( "cns" );
  my ( $vmgrSchema, $vmgrPasswd, $vmgrDbName ) = connParams( "vmgr" );
  getParamaters();
  foreach my $st ( @SQLTests ) {
    my $scriptFile = `mktemp`;
    chomp $scriptFile;
    print ("Script file for $st: $scriptFile\n");
    open ( SCRIPT, "> $scriptFile" );
    print ( SCRIPT "connect " . ( connString($st) )."\n" );
    open ( DROP, "< /usr/share/castor-@CASTOR_VERSION@-@CASTOR_RELEASE@/sql/drop_oracle_schema.sql" ) 
      or die "Failed to open  for reading: $!";
    while (<DROP>) {
      print ( SCRIPT $_ );
    }
    close (DROP);
    my $sqlScript = "/usr/share/castor-@CASTOR_VERSION@-@CASTOR_RELEASE@/sql/".$st."_oracle_create.sql";
    open ( SQL, "< ".$sqlScript ) or die "Failed to open $sqlScript for reading: $!";
    while (<SQL>) {
      print ( SCRIPT filterSQL ( $_, $NsDbUser, $NsDbPasswd, $NsDbName, $vmgrSchema ) ) ;
    }
    close ( SQL );
    print ( SCRIPT "".$check_compilation );
    close (SCRIPT);
    my $result = `sqlplus /NOLOG \@$scriptFile`;
    my $retVal = $?;
    printFiltered ( $result );
    print ( "Unlinking $scriptFile\n" );
    unlink ( $scriptFile );
    if ($retVal) {
      die "The compilation of $sqlScript failed.\n";
    } else {
      print "Compilation of $sqlScript successful.\n";
    }
  }
}


