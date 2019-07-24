import os
import sys
import stat
from datetime import datetime
import subprocess
import inspect
import re
import json

cta_admin_tapepool_json     = '/usr/bin/cta-admin --json tapepool'
cta_admin_tape_json         = '/usr/bin/cta-admin --json tape'
cta_admin_drive_json        = '/usr/bin/cta-admin --json drive'
cta_admin_archivefile_json  = '/usr/bin/cta-admin --json archivefile'
cta_smc_query_json          = '/usr/bin/cta-smc -j -q'

mt                          = '/bin/mt'

TPCONFIG    = '/etc/cta/TPCONFIG'

drv2lib     = dict(I1L8='IBM1L8',
                   I3JD='IBM3JD',
                   I355='IBM355',
                   I4JD='IBM4JD',
                   I455='IBM455',
                   T10D5='STK5T10D',
                   T10D6='STK6T10D')

lib2ctlHost = dict(IBM1L8='ibmlib1rmc',
                   IBM3JD='ibmlib3rmc',
                   IBM355='ibmlib3rmc',
                   IBM4JD='ibmlib4rmc',
                   IBM455='ibmlib4rmc',
                   STK5T10D='acsls513',
                   STK6T10D='acsls613')


def eprint(msg):
    """
    Error printing
    """
    sys.stderr.write(msg + '\n')


def now():
    """
    Return date and time in "YYYY/MM/DD HH:MM:SS"
    :return: Current date and time
    """
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def info(name, msg):
    """
    Print a message to STDOUT
    :param name: The name of the script or function
    :param msg: Message to be printed
    """
    print(now() + ' ' + name + ' [INFO] - ' + msg)


def debug(name, msg):
    """
    Print a message to STDOUT
    :param name: The name of the script or function
    :param msg: Message to be printed
    """
    print(now() + ' ' + name + ' [DEBUG] - ' + msg)


def abort(name, msg):
    """
    Print a message to STDERR and abort the script
    :param name: The name of the script or function
    :param msg: Message to be printed
    """
    eprint(now() + ' ' + name + ' [ABORT] - ' + msg)
    exit(1)


def warning(name, msg):
    """
    Prints a warning message
    :param name: The name of the script or function
    :param msg: Message to be printed
    """
    eprint(now() + ' ' + name + ' [WARN] - ' + msg)


def error(name, message):
    """
    Prints an error message
    :param name: The name of the script of the function
    :param message: Message to be printed
    """
    eprint(now() + ' ' + name + ' [ERROR] - ' + message)


def is_root():
    """
    Checks if the user executing the script is root
    :return: True if root, False otherwise
    """
    return os.getuid() == 0


def effectively_readable(path):
    """
    Checks if a file is readable or not
    :param path: The path to the file we want to check
    :return: True if readable, False otherwise
    """
    uid = os.getuid()
    euid = os.geteuid()
    gid = os.getgid()
    egid = os.getegid()

    # This is probably true most of the time, so just let os.access()
    # handle it.  Avoids potential bugs in the rest of this function.
    if uid == euid and gid == egid:
        return os.access(path, os.R_OK)

    st = os.stat(path)

    # This may be wrong depending on the semantics of the OS.
    # i.e. if the file is -------r--, does the owner have access or not?
    if st.st_uid == euid:
        return st.st_mode & stat.S_IRUSR != 0

    # See comment for UID check above.
    groups = os.getgroups()
    if st.st_gid == egid or st.st_gid in groups:
        return st.st_mode & stat.S_IRGRP != 0

    return st.st_mode & stat.S_IROTH != 0


def effectively_executable(path):
    """
    Checks if a file is executable or not
    :param path: The path to the file we want to check
    :return: True if executable, False otherwise
    """
    uid = os.getuid()
    euid = os.geteuid()
    gid = os.getgid()
    egid = os.getegid()

    if uid == euid and gid == egid:
        return os.access(path, os.X_OK)

    st = os.stat(path)

    if st.st_uid == euid:
        return st.st_mode & stat.S_IXUSR != 0

    groups = os.getgroups()
    if st.st_gid == egid or st.st_gid in groups:
        return st.st_mode & stat.S_IXGRP != 0

    return st.st_mode & stat.S_IXOTH != 0


# TODO: Host dedication is not implemented ic CTA
# def is_host_dedicated():
#     """
#     Check if the local host is self-dedicated.
#     :return: True if host is dedicated, False otherwise
#     """
#     hostname = subprocess.run('/bin/hostname -s', shell=True, stdout=subprocess.PIPE, encoding='utf-8').stdout.rstrip()
#     text_wanted = 'host=' + hostname
#     cmd = showqueues + ' -x -S ' + hostname
#     return text_wanted in subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, encoding='utf-8').stdout


def is_erase_pool(pool):
    """
    Check if a pool is an erase pool
    :param pool: The pool name to check its existence
    :return: True if it is an erase pool, False otherwise
    """
    if pool_exists(pool):
        return 'erase_' in pool
    abort(get_caller_name(), 'no info found for the specified pool [' + pool + '] in cta-admin')


def pool_exists(pool):
    """
    Check if a pool exists
    :param pool: The pool name to check its existence
    :return: True if it exists, False otherwise
    """
    cmd = cta_admin_tapepool_json + ' ls'
    result_json_array = json.loads(run_cmd(cmd).stdout)

    for pool_info in result_json_array:
        if pool_info['name'] == pool:
            return True
    return False


def get_tape_pool(volume_id):
    """
    Gets the pool of the specified tape
    :param volume_id: The volume ID of the tape
    :return: The pool name
    """
    cmd = cta_admin_tape_json + ' ls --vid ' + volume_id
    result_json_array = json.loads(run_cmd(cmd).stdout)

    if not result_json_array:
        abort(get_caller_name(), 'no info found for the specified tape [' + volume_id + '] in cta-admin')

    return result_json_array[0]['tapepool']


def get_tape_vid_inside_drive(drive_name):
    """
    Gets the volume ID of the tape inside a specific drive (either mounted or ejected) if there exists one. None instead
    :param drive_name: The drive name
    :return: The volume ID of the tape inside the drive; None if there is no tape inside
    """
    cmd = cta_admin_drive_json + ' ls ' + drive_name
    cmd_result = run_cmd(cmd)
    if 'No such drive' in cmd_result.stderr:
        abort(get_caller_name(), 'Drive "' + drive_name + '" does not exist')

    cmd = cta_smc_query_json + ' D -D ' + get_ordinal(drive_name)
    json_result = json.loads(run_cmd(cmd).stdout)[0]

    if json_result['vid'] == '':
        return None
    return json_result['vid']


def get_caller_name():
    """
    Gets the name of the called function
    :return: The name of the called function
    """
    current_frame = inspect.currentframe()
    caller_frame = inspect.getouterframes(current_frame, 2)
    caller_name = caller_frame[1][3]
    return caller_name


def is_drive_known(drive):
    """
    Checks if a drive is known (it is listed in TPCONFIG)
    :param drive: The name of the drive
    :return: True if drive is known; False otherwise
    """
    if not drive:
        return False
    cmd = "grep --silent " + drive + " " + TPCONFIG
    return not run_cmd(cmd).returncode   # 0 = found; 1 = not-found ... We reverse the return code


def get_first_drive():
    """
    Gets the 1st drive from TPCONFIG (local)
    :return: The drive ID if found; aborts otherwise
    """
    if not effectively_readable(TPCONFIG):
        abort(get_caller_name(), TPCONFIG + ' is not readable')

    drive = None

    with open(TPCONFIG, 'r') as tpconfig:
        for line in tpconfig:
            if not line.startswith('#') and not is_blank_line(line):
                array_line = line.split()     # [Unit name, Device group, System device, Control method]
                drive = array_line[0]
                break
        tpconfig.close()

    if drive is None:
        abort(get_caller_name(), 'no drive found in ' + TPCONFIG + ', please check your configuration')

    return drive


def is_blank_line(line):
    """
    Checks if a line is formed only by spaces or just nothing (blank line)
    :param line: The line to check
    :return: True if blank; False otherwise
    """
    for char in line:
        if char == '' or char == '\n':
            return True
        elif char != ' ':
            return False
    return True


def get_library(drive):
    """
    Gets the library of the local drive. Takes the 1st one in TPCONFIG if none is given
    :return: The library name as derived from the name (or from the name of the 1st in TPCONFIG if not name given)
    """
    if not drive:
        return get_library(get_first_drive())

    else:
        if drive.startswith('I'):   # It is an IBM drive
            prefix = drive[:4]          # Only the 4 first characters
        else:                       # It is an Oracle drive
            prefix = drive[:5]          # Only the 5 first characters

        try:
            return drv2lib[prefix]
        except KeyError:
            abort(get_caller_name(), 'Could not get library for drive [' + drive + ']')


def get_device(drive=None):
    """
    Gets the device node of a given drive. If no drive given, it takes the first one
    :param drive: (optional) The drive name
    :return: The device of the drive, if one is given; the first entry in /etc/castor/TPCONFIG otherwise
    """
    if not effectively_readable(TPCONFIG):
        abort(get_caller_name(), TPCONFIG + ' not readable')

    device = None

    with open(TPCONFIG, 'r') as tpconfig:
        for line in tpconfig:
            if not line.startswith('#') and not is_blank_line(line):
                array_line = line.split()     # [Unit name, Device group, System device, Control method]
                if not drive:
                    device = array_line[2]    # Take the 1st one if no drive specified
                    break
                elif drive == array_line[0]:
                    device = array_line[2]    # Take the device whose drive matches the given drive name

    if device is None:
        if drive:
            abort(get_caller_name(), 'no such tape drive (' + drive + ') found in ' + TPCONFIG)
        abort(get_caller_name(), 'no device found in ' + TPCONFIG + ', please check your configuration')

    if not device.startswith('/dev/nst') and not device.startswith('/dev/tape'):
        abort(get_caller_name(), 'Found device [' + device + '] does not start neither with /dev/nst nor /dev/tape')

    return device


def do_VOL1_check(input_file, output_file, volume_id, log_function=None):
    """
    Performs a VOL1 check for the tape specified
    :param input_file: The input file for the 'if' of dd command
    :param output_file: The output file for the 'of' of dd command
    :param volume_id:  The tape volume ID
    :param log_function: (optional) The function to use for the log
    :return: 'ok' if VOL1 check was ok; 'not_ok' otherwise
    """
    return_code = 'ok'
    cmd = '/usr/bin/dd if=' + input_file + ' of=' + output_file + ' bs=80 count=3'
    if log_function:
        log_function(cmd)
    if run_cmd(cmd + ' > /dev/null 2>&1').returncode != 0:
        error(get_caller_name(), 'reading VOL1 failed for ' + volume_id)
        return_code = 'not_ok'
    else:
        if run_cmd('grep ^VOL1' + volume_id + ' ' + output_file + ' >/dev/null 2>&1').returncode != 0:
            abort(get_caller_name(), 'VOL1 does not contain ' + volume_id + ', wrong tape?')
            return_code = 'not_ok'
        else:
            if log_function:
                log_function('VOL1 contains ' + volume_id + ': OK')
    return return_code


def get_tape_info(volume_id):
    """
    Gets information about a tape from "cta-admin tape"
    :param volume_id: The tape VID
    :return: A dict with entries for all info returned by "cta-admin tape"
    """
    if volume_id == '' or volume_id is None:
        abort(get_caller_name(), 'No volume ID specified as parameter for get_tape_info')

    cmd = cta_admin_tape_json + ' ls --vid ' + volume_id
    tape_info_array = json.loads(run_cmd(cmd).stdout)

    if not tape_info_array:
        return None

    return tape_info_array[0]


def are_tape_and_drive_in_the_same_lib(volume_id, drive, opt_verbose):
    if opt_verbose:
        debug(get_caller_name(), 'checking if tape ' + volume_id + ' and local drive are in the same lib...')

    tape_info = get_tape_info(volume_id)

    cmd = cta_admin_drive_json + ' ls ' + drive
    drive_info_array = json.loads(run_cmd(cmd).stdout)

    if not drive_info_array:
        abort(get_caller_name(), 'Not info found for drive "' + drive + '" in cta-admin drive')

    drive_info = drive_info_array[0]

    lib_drive = drive_info['logicalLibrary']
    lib_tape = tape_info['logicalLibrary']

    return lib_tape == lib_drive, lib_tape, lib_drive


def tape_queryvolume(volume_id, opt_verbose, skip_simple_errors=False):
    """
    Determines the location of a tape (previously, this function was the module called tape-queryvolume)
    :param volume_id: The VID of the tape we want to locate
    :param opt_verbose: Be verbose or not
    :param skip_simple_errors: Used by some scripts that don´t want to abort when an error occurs, but skip it
    :return: Location of the tape ('DRIVE ([element address])' || 'HOME' || 'UNKNOWN ([message])'
    """
    lib = None
    tape_info = get_tape_info(volume_id)
    if 'logicalLibrary' in tape_info.keys():
        lib = tape_info['logicalLibrary']
    else:
        abort(get_caller_name(), 'library not found for tape "' + volume_id + '" in cta-admin tape')

    location = None

    if 'IBM' in lib:
        cmd = cta_smc_query_json + ' V -V ' + volume_id + ' 2>/dev/null'

        if opt_verbose:
            debug(get_caller_name(), 'running ' + cmd)

        json_result_list = json.loads(run_cmd(cmd).stdout)

        if not json_result_list or json_result_list[0]['vid'] != volume_id:
            if not skip_simple_errors:
                abort(get_caller_name(), 'no info found for tape "' + volume_id + '" in cta-smc')
            error(get_caller_name(), 'no info found for tape "' + volume_id + '" in cta-smc')

        json_result = json_result_list[0]
        drive_element_address = json_result['elementAddress']
        tape_location = json_result['elementType']

        # Analyze the output
        if tape_location == 'drive':
            location = 'DRIVE (element address = ' + str(drive_element_address) + ')'
        elif tape_location == 'slot':
            location = 'HOME'
        else:
            location = 'UNKNOWN (' + tape_location + ')'

    # TODO: Think about the inclusion of Oracle logic
    # elif 'SL' in lib or 'STK' in lib:
    #     # Basic SUN query
    #     ssh_cmd = 'ssh -ti /afs/cern.ch/project/castor/TOOLS/ssh/id_rsa -l acsss'
    #     control_host = get_control_host(lib)
    #     cmd = ssh_cmd + " " + control_host + " 'echo \"q vol " + volume_id + "\" | /export/home/ACSSS/bin/cmd_proc'"
    #     if opt_verbose:
    #         debug(get_caller_name(), 'running ' + cmd)
    #     result_array = subprocess.run(cmd+' 2>&1', stdout=subprocess.PIPE, encoding='utf-8', shell=True).stdout
    #
    #     line = None
    #     found = False
    #     for item in result_array:
    #         if volume_id in item:
    #             line = item
    #             found = True
    #             break
    #
    #     if not found:
    #         eprint('no ' + volume_id + ' in ACSLS output')
    #         exit(0)
    #
    #     # Analyze the output
    #     result_array = re.compile('\s{2,}').split(line)     # Splitting on 2 consecutive spaces at least
    #     if result_array[1] == 'in drive':
    #         location = 'DRIVE'
    #     elif result_array[1] == 'home':
    #         location = 'HOME'
    #     else:
    #         location = 'UNKNOWN (' + result_array[1] + ')'
    #     drive_element_address = result_array[2]

    else:
        abort(get_caller_name(), 'unknown library "' + lib + '"')

    if opt_verbose:
        debug(get_caller_name(), 'VID=' + volume_id + ' LIBRARY=' + lib +
              ' LOCATION=' + location + ' SLOT=' + str(drive_element_address))

    return location


def get_ordinal(the_drive):
    """
    Gets the ordinal number of an IBM drive from TPCONFIG
    :param the_drive: the drive name (optional, the 1st drive will be taken by default)
    :return: The ordinal device number: last 2 digits of the name
    """
    control_method = None
    drive_ordinal_number = None

    if not effectively_readable(TPCONFIG):
        abort(get_caller_name(), TPCONFIG + ' not readable')
    with open(TPCONFIG, 'r', encoding='utf-8') as tpconfig:
        for line in tpconfig:
            if not line.startswith('#') and not is_blank_line(line):
                array_line = line.split()  # [Unit name, Device group, System device, Control method]
                if not the_drive:
                    control_method = array_line[3]
                    break
                elif the_drive == array_line[0]:
                    control_method = array_line[3]
        tpconfig.close()

    # Extract the drive ordinal from TPCONFIG file, from the control method parameter, after comma
    pattern = re.compile('^smc(\d+)')
    if not control_method:
        abort(get_caller_name(), 'information about drive (' + the_drive + ') not found in ' + TPCONFIG)
    if re.match(pattern, control_method):
        drive_ordinal_number = pattern.match(control_method).group(1)

    return drive_ordinal_number


def is_drive_free(drive):
    """
    Checks if the local drive is free using "cta-smc" command
    :return: True if the driver is free; False otherwise
    """
    if not drive:
        abort(get_caller_name(), 'Parameter "drive" not specified for function is_drive_free(drive)')
    cmd = cta_smc_query_json + ' D -D ' + get_ordinal(drive)
    drive_info = json.loads(run_cmd(cmd).stdout)[0]
    return drive_info['status'] == 'free'


def is_tape_ejected(drive_name):
    """
    Checks if the drive has an ejected tape inside
    :param drive_name: The drive ordinal number
    :return: True if tape is ejected; False otherwise
    """
    cmd = cta_smc_query_json + ' D -D ' + get_ordinal(drive_name)
    drive_info = json.loads(run_cmd(cmd).stdout)[0]
    return drive_info['status'] == 'unloaded'


# TODO: Discuss if this is the best way to do it
def is_tape_empty(volume_id):
    """
    Checks if a tape volume is empty (OF FILES THAT WE DON'T CARE ABOUT, not physically empty)
    :param volume_id: The tape volume ID
    :return: True if "logically" empty. False otherwise
    """
    cmd = cta_admin_archivefile_json + ' ls --vid ' + volume_id + ' --summary'
    cmd_call = run_cmd(cmd)
    if cmd_call.returncode != 0:
        abort(get_caller_name(), 'Error executing command (' + cmd + '). RESULT = ' + cmd_call.stderr)

    array_json = json.loads(cmd_call.stdout)
    return int(array_json[0]['totalFiles']) == 0


def request_kinit_permissions(log_function=None):
    """
    Requests the renewal of KINIT permissions or requests a new ticket if the previous one expired
    """
    if run_cmd('/usr/sue/bin/kinit -R').returncode != 0:
        # If the KINIT ticket renewal fails, request another one. It requires prompt for the username to request
        info(get_caller_name(), 'Please specify the USERNAME and PASSWORD for requesting a new '
                                'Kerberos permission-granting ticket:')
        kinit_call = run_cmd('read kinit_username ; echo $kinit_username & kinit $kinit_username')
        if kinit_call.returncode != 0:
            msg = 'unable to obtain new Kerberos permission-granting ticket for the username: ' + \
                  kinit_call.stdout.rstrip()
            if log_function:
                log_function(get_caller_name(), 'ABORT: ' + msg)
            abort(get_caller_name(), msg)
        if log_function:
            log_function(get_caller_name(), 'succesfully obtained new Kerberos permission-granting ticket for username: ' +
                         kinit_call.stdout.rstrip())


def is_tape_write_protected():
    """
    Checks if the WR_PROT flag is set for a locally mounted tape.
    For the status (RDONLY, ...) see get_tape_info()
    :return: True if tape is write protected; False otherwise
    """
    device = get_device()
    if device is None:
        abort(get_caller_name(), 'no matching entry in ' + TPCONFIG + ' found for a device')

    cmd = f'{mt} -f {device} stat'
    return 'WR_PROT' in run_cmd(cmd).stdout


def remove_directory_recursively(path):
    """
    Removes recursively the specified directory and everything inside it
    :param path: The path of the directory to remove
    :return: The return code of the command run
    """
    cmd = f'rm -r {path}'
    return run_cmd(cmd).returncode


def run_cmd(cmd):
    """
    Runs a command in console using subprocess.run(...)
    :param cmd: The command to run
    :return: The result of the call (contains stdout, stderr, returncode, etc.)
    """
    return subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf-8')
