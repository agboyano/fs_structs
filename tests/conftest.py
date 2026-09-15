"""Shared fixtures.

Set the environment variable FS_STRUCTS_TEST_ROOT to a directory on the filesystem you want
to test (for example a network share) and the whole suite runs there instead of tmp_path::

    FS_STRUCTS_TEST_ROOT=/mnt/share/fs_structs_tests pytest -q
"""

import os
import shutil
import uuid
from pathlib import Path

import pytest


@pytest.fixture
def root(tmp_path):
    """A fresh, empty directory for one test."""
    base = os.environ.get("FS_STRUCTS_TEST_ROOT")
    if not base:
        yield tmp_path
        return
    path = Path(base) / f"fs_structs_test_{uuid.uuid4().hex}"
    path.mkdir(parents=True)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)
