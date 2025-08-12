import os
import time

TIMEOUT = int(os.getenv("TIMEOUT"))


def test_1_print():
    print("\nThe infrastructure is ready, waiting " + str(TIMEOUT) + " seconds")


def test_2_sleep():
    time.sleep(TIMEOUT)
