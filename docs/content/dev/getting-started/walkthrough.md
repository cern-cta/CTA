# CTA Introductory Walkthrough

In this walkthrough we will go over some of the common workflows in CTA, step by step. We will be:

1. **Archiving files** - sending them to the EOS disk buffer and then on tape;
2. **Preparing files** for retrieval from tape - including aborting the prepare operation and evicting files from the EOS disk buffer;
3. **Repacking and reclaiming a tape** so that it can be reused.


## Before we start

!!! warning "Test setup needed"

    You will need the Kubernetes-based development environment to follow this guide.
    You will also need to run `cta-dev test setup`. Refer to the [setup guide](./setup.md) for more information.

We will be operating on the `cli` (`cta-cli-0`) and `client` (`cta-client-0`) pods. The former will be used for direct requests to the CTA frontend, through `cta-admin`,
while the latter is configured to communicate with the EOS buffer. You can access them using `kubectl exec -it -n dev <pod_name> -- bash`.

The first operation should be to enable all tape drives in our setup (there should be two of them).

!!! terminal "cli"

    ```console
    # cta-admin dr up '.*' # enable all drives
    # cta-admin dr ls # check that they are indeed both "Up"
    ```

!!! info Command shorthands
    As you've probably noticed, we are using the shorthand version of the `cta-admin` CLI commands, in order to minimize the writing effort.
    Make sure that you understand what each command means. `cta-admin --help` is your friend!


## Archiving files on tape

Let's start by archiving a single file. This is done through copying it to the EOS buffer (`ctaeos`), after which the EOS Workflow Engine (WFE)
will notify CTA about the creation of the file and trigger the transfer process. We will transfer our file to EOS using `xrdcp`.

!!! terminal "cli"

    ```console
    # echo "emmental" | xrdcp - root://ctaeos//eos/ctaeos/cta/cheese.txt
    ```

Now, let's check whether our file is on tape. The first column of the output should show how many copies of the file exist on disk (`d`) and on tape (`t`):

!!! terminal "client"

    ```console
    # eos root://ctaeos ls eos/ctaeos/cta/ -y # this should show `d0::t1`
    ```

If the file doesn't show immediately as on tape, try the command again. It should be almost instant with the current setup.
Let's consult the file's EOS metadata a little bit more in detail:

!!! terminal "client"

    ```console
    # eos root://ctaeos attr ls eos/ctaeos/cta/cheese.txt
    ```

You will notice the `sys.archive.file_id` parameter (henceforth `ARCHIVE_ID`), which is the unique ID of a file in the CTA catalogue.
This allows us to find out, for instance, on which tape the file is actually located:

!!! terminal "cli"

    ```console
    # cta-admin tf ls --id <ARCHIVE_ID> # find on which tape the file is located ('vid' column)
    # cta-admin tf ls --vid <TAPE_ID> # confirm using the reverse check (tape -> files)
    ```

Easy, right? Let's archive a bunch more files with random data, just for fun (and testing!)

!!! terminal "cli"

    ```console
    # for i in $(seq 1 9); do
        dd if=/dev/random of=/dev/stdout bs=1M count=20 | \
            xrdcp - root://ctaeos//eos/ctaeos/cta/papaya${i}.bin;
    done
    ```


## Preparing a file for retrieval

Now we will be asking CTA to "recover" a file from tape and make it available on the EOS buffer.
But first of all, let's set our kerberos-relate environment config so that the EOS client can authenticate.

!!! terminal "client"

    ```console
    # export KRB5CCNAME=/tmp/poweruser1/krb5cc_0 XrdSecPROTOCOL=krb5
    # kinit -kt /root/poweruser1.keytab poweruser1@TEST.CTA
    ```

Now we should be a power user on `ctaeos`.

Let's trigger the "prepare" request:

!!! terminal "client"

    ```console
    # xrdfs root://ctaeos prepare -s /eos/ctaeos/cta/cheese.txt # 's' means 'staging'
    ```

If everything goes as expected, the command should output the "request ID". This is the ID that the EOS WFE will
use to keep track of the ongoing transfer from CTA. In practice, this will be a highly asynchronous operation.
In our little test setup, it should take only a few seconds until you
see the file show up on disk (`d1::t1`):

!!! terminal "client"

    ```console
    # eos root://ctaeos ls -y eos/ctaeos/cta/cheese.txt
    ```

If you check the metadata of the file, you will see that there is a `sys.retrieve.evict_counter` extended attribute
which keeps track of how many times the file has been "prepared". Think of it as a reference counter, telling you
how many users/apps still need this file on disk:

!!! terminal "client"

    ```console
    # eos root://ctaeos attr ls eos/ctaeos/cta/cheese.txt # check that `sys.retrieve.evict_counter` == 1
    ```

Feel free to run the `prepare -s` command a couple of times more, and see the "evict counter" increase.
At this point you can finally retrieve the contents of the file:

!!! terminal "client"

    ```console
    # unset KRB5CCNAME XrdSecPROTOCOL # back to regular user
    # xrdcp root://ctaeos//eos/ctaeos/cta/cheese.txt - | less
    ```


## Evicting a file

Now we will "evict" our buffered file. This means removing the disk copy, while keeping the tape copy
unchanged. In order to do that, we need to set the "evict counter" back to zero. `prepare -e` decreases
it by one. Repeat it as many times as you prepared the file.

!!! terminal "client"

    ```console
    # xrdfs root://ctaeos prepare -e /eos/ctaeos/cta/cheese.txt # execute it as many times as needed
    ```

Once it reaches zero, check the number of on-disk copies:

!!! terminal "client"

    ```console
    # eos root://ctaeos ls -y eos/ctaeos/cta/cheese.txt
    ```

It should now be zero (`d0::t1`).


## Aborting a Prepare operation

Now we will simulate a situation where we abort a "prepare" operation halfway.
Since retrieval operations are fulfilled almost instantly in our test setup, we will disable
the tape where the file is located, so that we have time to send our "abort" request.

Let's first find the ARCHIVE_ID of one of the binary files we created:

!!! terminal "client"

    ```console
    # eos root://ctaeos attr ls eos/ctaeos/cta/papaya1.bin # check 'sys.archive.file_id'
    ```

!!! terminal "cli"

    ```console
    # cta-admin tf ls --id <FILE_ID> # find out on which tape the file is located ('vid' column)
    # cta-admin ta ch --vid <TAPE_ID> --state DISABLED \
        --reason 'Because I feel like it'
    # cta-admin ta ls --vid <TAPE_ID> # double-check that it's indeed disabled
    ```

Now we don't risk that our prepare operation will be immediately successful. Let's go ahead with it:

!!! terminal "client"

    ```console
    # xrdfs root://ctaeos prepare -s /eos/ctaeos/cta/papaya1.bin # copy the request id which is printed
    ```

At the same time, this is a perfect occasion to show you the `showqueues` (`sq` for short) command:

!!! terminal "cli"

    ```console
    # cta-admin sq
    ```

You will see there is a "Retrieve" operation in the queue. Let's abort it!

!!! terminal "client"

    ```console
    # eos root://ctaeos ls -y eos/ctaeos/cta/papaya1.bin # double-check there is no disk copy. the operation shouldn't actually happen!
    # xrdfs root://ctaeos prepare -a <REQUEST_ID> /eos/ctaeos/cta/papaya1.bin # abort the retrieval
    ```

This should do it, but in order to be sure, let's bring the tape back up and check whether our retrieve job succeeds at
putting the file in the disk buffer.

!!! terminal "cli"

    ```console
    # cta-admin ta ch --vid <TAPE_ID> --state ACTIVE # bring the tape back up
    # cta-admin sq # check that the queue is now empty
    ```

Finally, let's check that no disk copy was produced:

!!! terminal "client"

    ```
    # eos root://ctaeos ls -y eos/ctaeos/cta/papaya1.bin # there should be 'd0::t1'
    ```


## Repacking a Tape

The "repack" operation moves all files away from a particular tape. This can be done in order to:

* **consolidate** the contents of one or more tapes into a continuous sequence of files, thus **optimizing tape space**;
* **replace** a broken tape;
* upgrade to a **newer generation** of tape media.

More information about the repack workflow can be found [here](../../overview/tape/media/index.md#repack).

In this example, we will simulate the first scenario (optimizing tape space). Let's have a look at the occupancy of our first tape:

!!! terminal "cli"

    ```
    # cta-admin ta ls --vid ULT101 # check the 'occupancy' column
    ```

Now, let's see which files are stored on it:

!!! terminal "cli"

    ```console
    # cta-admin --json tf ls --vid ULT101 | jq -r '.[] | .af.archiveId' | xargs
    ```

These are the ARCHIVE_IDs of the files on your first tape. Match them with the ids from your test files:

!!! terminal "client"

    ```console
    # prefix=/eos/ctaeos/cta/papaya
    for i in $(seq 1 9); do
        eos root://ctaeos attr get sys.archive.file_id "${prefix}${i}.bin" \
            | sed -e "s|sys.archive.file_id=\"\(.*\)\"|${prefix}${i}.bin: \1|" \
    done
    ```

We wil be deleting a couple of the matching files, using `eos rm`. These will be the "holes" on the tape
which should go away with repacking:

!!! terminal "client"

    ```console
    # eos root://ctaeos rm /eos/ctaeos/cta/papaya{4,5,6}.bin
    ```

(those are just examples, make sure your tape actually contains them)

This will effectively remove them from the CTA catalogue. If you check again the list of files on the tape, you will notice
they will be gone:

!!! terminal "cli"

    ```console
    # cta-admin --json tf ls --vid ULT101 | jq -r '.[] | .af.archiveId' | xargs
    ```

The tape occupancy won't be lower, however. The file is still on tape, only in an untracked state:

!!! terminal "cli"

    ```
    # cta-admin ta ls --vid ULT101 # check 'occupancy column'
    ```

Before we go any further, we will need to create a buffer directory, where the data will be held temporarily.

This is the only instance where you will need to use the `eos-mgm-0` pod:

!!! terminal "eos-mgm"

    ```console
    # eos root://ctaeos mkdir eos/ctaeos/repack # create repack buffer dir
    # eos chmod 1777 /eos/ctaeos/repack
    ```

You should also have a Virtual Organization (VO) set up for repacking, as well as a Mount Policy (MP).
Our test script should have set those up for you, but feel free to double-check:

!!! terminal "cli"

    ```console
    # cta-admin vo ls # check that we have a 'vo_repack' VO for repacking. this should be the case.
    # cta-admin mp ls # there should be a mount policy called 'repack_ctasystest'
    ```

In order to repack our tape, we first have to set it to the `REPACKING` state, and set it to "full".

!!! terminal "cli"

    ```console
    # cta-admin ta ch --state REPACKING --reason "Testing repack" --vid ULT101
    # cta-admin ta ch --vid ULT101 -f true # set tape to full
    ```

Now, on to the actual operation:

!!! terminal "cli"

    ```console
    # cta-admin repack add --vid ULT101 --justmove \
        --mountpolicy repack_ctasystest --bufferurl root://ctaeos//eos/ctaeos/repack
    ```

Then, let's check on the status of our "repack" job. Hopefully, it will show up as "Complete".

!!! terminal "cli"

    ```console
    # cta-admin re ls # the repack job should be eventually listed as completed
    ```

Look into the ARCHIVE_IDs of the files you didn't delete and check that they are now in different tapes:

!!! terminal "cli"

    ```console
    # cta-admin tf ls --id <ARCHIVE_ID > # check that the file is now on a tape other than ULT101
    ```

... and the old tape doesn't hold any files anymore.

!!! terminal "cli"

    ```console
    # cta-admin tf ls --vid ULT101
    ```

Perhaps to your surprise, the occupancy will still be the same, however.
All files are still "physicaly" stored on the tape, in an untracked state. But since they are now copied elsewhere, we can
reset or "reclaim" the tape.

!!! terminal "cli"

    ```console
    # cta-admin re rm --vid ULT101 # remove the repack operation (needed to be able to use again the tape)
    # cta-admin ta ch --state DISABLED --reason "Reclaiming tape" --vid ULT101 # set the tape to DISABLED
    ```


## Reclaiming a Tape

Finally, we can reclaim our tape, so that we can reuse it for other things!

!!! terminal "cli"

    ```console
    # cta-admin ta reclaim --vid ULT101 # reclaim the tape
    # cta-admin --json ta ls --vid ULT101 | jq -r '.[] | .occupancy' # will be zero!
    ```

And that's the end of our journey.

**Thanks for having followed this walkthrough!**
