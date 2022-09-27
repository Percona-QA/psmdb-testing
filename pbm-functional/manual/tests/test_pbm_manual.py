import os
import pytest
import testinfra
import subprocess
import json
import time
import testinfra.utils.ansible_runner
from datetime import datetime

TIMEOUT = int(os.getenv("TIMEOUT"))

def test_1_print():
    print("\nThe infrastructure is ready, waiting " + str(TIMEOUT) + " seconds")

def test_2_sleep():
    time.sleep(TIMEOUT)

