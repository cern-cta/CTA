#! /usr/bin/perl -w

use strict;
use warnings;
use Getopt::Long;
use Term::ReadKey;

# script to generate multiple host certificate requests, sign and submit
# them to CA - later upload to sindes 

# full options are:
# generate, sign and send reqs to CA
#    - needs from, user cert
# sign pre generated certificates and send to CA
#   - needs from, user cert
# upload files to SINDES
#   - user needs sindes rights and to be run on lxadm node

# may 2006

sub ParseHost(@);
sub GenerateCerts(@);
sub SignCerts(@);
sub SindesCerts(@);

my $from = my $service = undef;
my $renew = my $sign = my $sindes = 0;

my $req_dir = undef;

sub usage {
   print "--from=<email\@address>\n";
   print "--signonly\t\tTakes files in (default) dir, signs and sends to CA.\n";   
   print "--sindes\t\tUpload files in (default) dir to SINDES.\n";
   print "--dir\t\tDefine directory other than default.\n";
   print "--help\n";
   exit;}

my $rc = Getopt::Long::GetOptions("from=s"    => \$from,
                          #        "renew"     => \$renew,
                          #        "service=s" => \$service,
				  "signonly"  => \$sign,
				  "sindes"    => \$sindes,
				  "dir=s"     => \$req_dir,
				  "help"      =>  &usage
                                 );

$req_dir = $ENV{'HOME'}."/certificates" unless defined $req_dir;

my @host = ParseHost(@ARGV);

if (undef $req_dir) {}

#if hosts not defined, error
&usage() unless $#host >= 0;

#sign & send pre generated certs
if ($sign == 1){ 
    &usage() unless defined $from;
    &SignCerts(@host);
    exit;
}

#upload certs to sindes
if ($sindes == 1){
     &usage() unless $#host >= 0;
     &SindesCerts(@host);
}

#generate certs and send to CA
if ($sign == 0 && $sindes == 0 && $#host >= 0) {
 &usage() unless defined $from;
 &GenerateCerts(@host);
 &SignCerts(@host);
}
##########################################################
#
# generate certificates - create directory, generate hostcert.pem, 
# hostreq.pem, blank hostkey.pem
#
#
sub GenerateCerts(@) {
    print ">>>\n>>> First, generate hostkey and certificate requests...\n>>>\n\n\n\n";

    for my $host (@host){
	$host .= ".cern.ch" unless $host =~ /\./;
	my $dir = "$req_dir/$host";
	mkdir $dir,0755 unless -d $dir;
	my $cmd = "/afs/cern.ch/user/g/gridca/scripts/cert-req -host $host -dir $dir -quiet";
	$cmd .= " -prefix $service" if defined $service;
	system($cmd); # && exit -1;
    }
}

##########################################################
#
# sign certificates - mainly in case of typing in the wrong 
# pass phrase and having to start again
#
#
sub SignCerts(@) {

    my @host = @_;

    print ">>>\n>>> Sign the requests and mailing them off!\n>>>\n\n\n\n";

    undef my $passphrase;
    for my $host (@host){
      $host .= ".cern.ch" unless $host =~ /\./;
      my $dir = "${req_dir}/${host}";
      if (not defined $passphrase){
        print STDOUT "Enter pass phrase for ~/.globus/userkey.pem:";
        ReadMode("noecho",);
        chomp($passphrase = <STDIN>);
        ReadMode("normal");
        print "\n";
      }
      system("/usr/bin/openssl smime -passin pass:$passphrase -sign -to service-grid-ca\@cern.ch -from $from -subject \"Certificate Request\" -signer $ENV{HOME}/.globus/usercert.pem -inkey $ENV{HOME}/.globus/userkey.pem < $dir/hostreq.pem | /usr/sbin/sendmail -t");
      print "Certificate Request for \"$host\" mailed off!!\n";
    }#end for
}

##########################################################
#
# sindes certificates - tar ball and upload host certificates
#
#

sub SindesCerts(@) {
  my @hosts = @_;
  my $tmp_dir = "/tmp/grid-host-certificates"; 
  #changing this will break things!!!

  if (-e $req_dir && -d $req_dir) {
	opendir (DIR, $req_dir) or die "Problem opening directory $req_dir - $!\n";
  }else{
    print STDERR "Problem opening directory $req_dir - $!\n";
    exit;
  }


  foreach (@hosts) {

    next if $_ eq 'afsgsi';
    if (-e $tmp_dir) { 
      system ("rm", "-rf", $tmp_dir) == 0 
	  or die "Unable to delete tmp dir $tmp_dir $!\n";
    }
	
    mkpath ([$tmp_dir], 0, 0700);

    cp ("$req_dir/${_}.cern.ch/hostcert.pem", "${tmp_dir}/hostcert.pem") 
        or die "Problem with $_ hostcert.pem $!\n";
	
    cp ("$req_dir/${_}.cern.ch/hostkey.pem", "${tmp_dir}/hostkey.pem") 
        or die "Problem with $_ hostkey.pem $!\n";

    chdir("/tmp");
    system("tar czf grid-host-certificates.tgz grid-host-certificates") == 0 
	or die "Tar failure $!\n";
    
    system ("sindes-upload-file -f grid-host-certificates.tgz -i grid-host-certificates -t $_ -s node -S sindes-server") == 0 
	or die "Unable to upload file for $_\n"; 
    }
    closedir(DIR);
}

##########################################################
#
# expand strings like "lxdev03,lxplus0[01,09-12]" to arrays like
#  qw(lxdev03 lxplus001 lxplus009 lxplus010 lxplus011 lxplus012)
#

sub ParseHost(@){
    my @host = ();
    for my $arg (@_){
        $arg =~ s/(\d+),(\d+)/$1 $2/g;         # "lxdev03,lxplus0[01,03-13]" -> "lxdev03,lxplus0[01 03-13]"
        $arg =~ s/(\d+),(\d+)/$1 $2/g;         # a 2nd time is necessary for cases like "lxdev[1,2,3]"...
        my @err = ();
        for (split(/,/,$arg)){                 # ("lxdev03","lxplus0[01 03-13]")
            if (/\[(.*)\]/){
                my ($pre,$post) = ($`,$');     # ($pre,$post) = ("lxplus0","")
                for (split(/ /,$1)){           # ("01","03-13")
                    if (/-/){
                        if ($` < $'){
                            map {push(@host,"$pre$_$post")} ($`..$');
                        }else{
                            push(@err,"wrong range \"$`-$'\" specified");
                        }
                    }else{
                        push(@host,"$pre$_$post");
                    }
                }
            }else{
                push(@host,$_);
            }
        }
        if (@err){
            print STDERR "Error parsing \"$arg\":\n";
            map {print STDERR "   -- $_\n"} @err;
            return ();
        }
    }
    return @host;
}
