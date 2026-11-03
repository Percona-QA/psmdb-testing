"""Isolated helper tests: run with unittest to bypass live pytest plugins."""

import json
import unittest
from collections import deque
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pymongo
from clustersync import Clustersync

OLD_PAUSE = datetime(2026, 11, 1, tzinfo=UTC)
NEW_PAUSE = datetime(2026, 11, 2, tzinfo=UTC)


def checkpoint(marker=None):
    repl = {"pauseTime": marker} if marker else {}
    return {"_id": "pcsm", "data": {"state": "paused", "repl": repl}}


class PauseBoundary:
    """HTTP and Mongo boundaries advance only on explicitly supplied events."""

    def __init__(self, events=(), response=None):
        self.events = deque(events)
        self.response = response if response is not None else {"ok": True}
        self.status_response: dict[str, bool | str] | None = None
        self.status_code = 200
        self.state = "running"
        self.elapsed = 0
        self.observed = []
        self.requests = []
        self.subscribed_at_post = False
        self.subscribed = False
        self.client = MagicMock()
        self.client.__enter__.return_value = self.client
        self.collection = self.client.__getitem__.return_value.__getitem__.return_value
        self.collection.find_one.return_value = checkpoint(OLD_PAUSE)
        self.stream = MagicMock()
        self.stream.__enter__.return_value = self.stream
        self.stream.alive = True
        self.stream.try_next.side_effect = self.next_event
        self.collection.watch.side_effect = self.watch

    def watch(self, *args, **kwargs):
        self.subscribed = True
        return self.stream

    def next_event(self):
        self.elapsed += 1
        state, document = self.events.popleft() if self.events else (self.state, None)
        self.state = state
        self.observed.append(document)
        if isinstance(document, Exception):
            raise document
        return {"fullDocument": document} if document else None

    def exec_run(self, command):
        self.requests.append(command)
        if "-X POST" in command:
            self.subscribed_at_post = self.subscribed
            body = self.response
        else:
            body = self.status_response or {"ok": True, "state": self.state}
        # Support the old unbounded helper as well as request() for baseline-red.
        code = 200 if "-X POST" in command else self.status_code
        suffix = f"\n{code}" if "%{http_code}" in command else ""
        return SimpleNamespace(exit_code=0, output=(json.dumps(body) + suffix).encode())


class PauseTests(unittest.TestCase):
    def run_pause(self, boundary, **kwargs):
        sync = Clustersync("unit", "mongodb://unused", "mongodb://unused")
        docker_client = MagicMock()
        docker_client.containers.get.return_value = SimpleNamespace(exec_run=boundary.exec_run)
        with (
            patch("clustersync.docker.from_env", return_value=docker_client),
            patch("clustersync.pymongo.MongoClient", return_value=boundary.client),
            patch("clustersync.time.monotonic", side_effect=lambda: boundary.elapsed),
            patch("clustersync.Cluster.log"),
        ):
            return sync.pause(**kwargs)

    def test_waits_for_live_completion_and_fresh_persistence(self):
        boundary = PauseBoundary(
            [
                ("pausing", checkpoint()),
                ("pausing", checkpoint(OLD_PAUSE)),
                ("paused", checkpoint(OLD_PAUSE)),
                ("paused", checkpoint(NEW_PAUSE)),
            ]
        )

        self.assertTrue(self.run_pause(boundary))

        self.assertEqual(boundary.state, "paused")
        self.assertEqual(len(boundary.observed), 4)
        self.assertTrue(boundary.subscribed_at_post)
        boundary.stream.__exit__.assert_called_once()
        boundary.client.__exit__.assert_called_once()

    def test_persistence_before_live_status_still_waits(self):
        boundary = PauseBoundary(
            [
                ("pausing", checkpoint(NEW_PAUSE)),
                ("paused", None),
            ]
        )

        self.assertTrue(self.run_pause(boundary))

        self.assertEqual(boundary.state, "paused")
        self.assertEqual(len(boundary.observed), 2)

    def test_stale_or_early_checkpoints_cannot_complete_pause(self):
        for document in (checkpoint(), checkpoint(OLD_PAUSE)):
            with self.subTest(document=document):
                boundary = PauseBoundary([("paused", document)])

                self.assertFalse(self.run_pause(boundary, timeout=4))

                self.assertEqual(boundary.elapsed, 4)
                boundary.stream.__exit__.assert_called_once()
                boundary.client.__exit__.assert_called_once()

    def test_failed_and_not_active_status_abort_pause(self):
        for code, response in (
            (200, {"ok": True, "state": "failed"}),
            (409, {"ok": False, "error": "not_active"}),
        ):
            with self.subTest(response=response):
                boundary = PauseBoundary([("pausing", checkpoint())])
                boundary.status_code = code
                boundary.status_response = response

                self.assertFalse(self.run_pause(boundary))

                boundary.stream.__exit__.assert_called_once()

    def test_clone_rejection_returns_false(self):
        boundary = PauseBoundary(response={"ok": False, "error": "Change Replication is not running"})

        self.assertFalse(self.run_pause(boundary))

        self.assertEqual(boundary.observed, [])
        boundary.client.__exit__.assert_called_once()

    def test_mongo_error_closes_resources(self):
        boundary = PauseBoundary([("pausing", pymongo.errors.ConnectionFailure("offline"))])

        self.assertFalse(self.run_pause(boundary))

        boundary.stream.__exit__.assert_called_once()
        boundary.client.__exit__.assert_called_once()

    def test_http_timeout_uses_remaining_total_budget(self):
        boundary = PauseBoundary([("paused", checkpoint(NEW_PAUSE))])

        self.assertTrue(self.run_pause(boundary, timeout=4))

        budgets = [float(command.split("-m ")[1].split()[0]) for command in boundary.requests]
        self.assertEqual(budgets[0], 4)
        self.assertEqual(budgets[-1], 3)

    def test_expired_setup_budget_prevents_pause_request(self):
        boundary = PauseBoundary()

        def find_one(query):
            boundary.elapsed = 4
            return checkpoint(OLD_PAUSE)

        boundary.collection.find_one.side_effect = find_one

        self.assertFalse(self.run_pause(boundary, timeout=4))

        self.assertEqual(boundary.requests, [])
        boundary.stream.__exit__.assert_called_once()
        boundary.client.__exit__.assert_called_once()

    def test_first_pause_without_previous_checkpoint(self):
        boundary = PauseBoundary([("paused", checkpoint(NEW_PAUSE))])
        boundary.collection.find_one.return_value = None

        self.assertTrue(self.run_pause(boundary))

        self.assertEqual(boundary.state, "paused")
        self.assertEqual(len(boundary.observed), 1)


if __name__ == "__main__":
    unittest.main()
