import uuid
import base64
import time
import json


def test_setup_remote_scripts(env):
    env.client[0].copyTo("system_tests/remote/client/", "/root/", permissions="+x")
    env.ctacli[0].copyTo("system_tests/remote/ctacli/", "/root/", permissions="+x")

def test_archive(env):
    print("I'm archiving stuff")
