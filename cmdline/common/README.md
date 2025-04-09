# Contents
This directory contains the common logic/code for parsing the admin commands.
This is shared across both the XRootD/SSI and gRPC Frontends.

- `CtaAdminCmdParse` is about "translating" the command into options
- `CtaAdminCmd` is the class that does everything but actually **send** the command.
For sending the command we have another class because this is the only thing that's different across implementations. 

# Current implementation
- `CtaAdminTextFormatter` - responsible for formatting the output of headers and records (when tabular format for command output requested)
- `CtaAdminCmdParse` - just parse the command into options - then CtaAdminCmd will construct the request from the options
- `CtaAdminCmd` - this is the constructor of 


So in the common module I can put:
