import os
import shutil

import pytest

from dogtail import Dogtail


@pytest.fixture
def logpath(tmp_path):
    return tmp_path / "logfile"


@pytest.fixture
def test_lines():
    return ["1\n", "2\n", "3\n"]


@pytest.fixture
def test_str(test_lines):
    return "".join(test_lines)


@pytest.fixture
def logfile(logpath, test_str):
    with open(logpath, "w") as f:
        f.write(test_str)
    return logpath


@pytest.fixture
def offset_path(tmp_path):
    return tmp_path / "offset"


@pytest.fixture()
def dogtail(logfile_candidates, offset_path):
    return Dogtail(logfile_candidates=logfile_candidates, offset_path=offset_path)


@pytest.fixture()
def logfile_candidates(logfile):
    return (logfile, f"{logfile}.1")


def append(path, str):
    with open(path, "a") as f:
        f.write(str)


def logrotate(logfile_path):
    shutil.move(logfile_path, f"{logfile_path}.1")


def test_read(dogtail, test_str):
    assert dogtail.read() == test_str


def test_readlines(dogtail, test_lines):
    assert dogtail.readlines() == test_lines


def test_subsequent_read_with_no_new_data(dogtail, test_str):
    assert dogtail.read() == test_str
    assert not dogtail.read()


def test_subsequent_read_with_new_data(
    logfile, logfile_candidates, offset_path, test_str
):
    dogtail = Dogtail(logfile_candidates=logfile_candidates, offset_path=offset_path)
    assert dogtail.read() == test_str
    dogtail.update_offset_file()
    new_lines = "4\n5\n"
    append(logfile, new_lines)
    new_dogtail = Dogtail(
        logfile_candidates=logfile_candidates, offset_path=offset_path
    )
    assert new_dogtail.read() == new_lines


def test_logrotate_with_delay_compress(logfile, offset_path, logfile_candidates):
    new_lines = ["4\n5\n", "6\n7\n"]
    dogtail = Dogtail(logfile_candidates=logfile_candidates, offset_path=offset_path)
    dogtail.read()
    dogtail.update_offset_file()
    append(logfile, new_lines[0])
    logrotate(logfile)
    append(logfile, new_lines[1])
    dogtail = Dogtail(logfile_candidates=logfile_candidates, offset_path=offset_path)
    assert dogtail.read() == "".join(new_lines)


def test_offset_file(logfile, offset_path, logfile_candidates):
    dogtail = Dogtail(logfile_candidates=logfile_candidates, offset_path=offset_path)

    log_inode = os.stat(logfile).st_ino

    log_offset = 0

    for _ in range(3):
        next(dogtail)
        dogtail.update_offset_file()
        log_offset += 2
        with open(offset_path, "r") as f:
            inode, offset = int(next(f)), int(next(f))
        assert inode == log_inode
        assert offset == log_offset


def test_full_lines(dogtail, logfile, offset_path, test_str, logfile_candidates):
    """
    Tests lines are logged only when they have a new line at the end. This is useful to ensure that log lines
    aren't unintentionally split up.
    """
    new_lines = "4\n5,"
    last_line = "5.5\n6\n"

    append(logfile, new_lines)
    assert dogtail.read() == test_str + "4\n"
    dogtail.update_offset_file()
    dogtail = Dogtail(logfile_candidates=logfile_candidates, offset_path=offset_path)
    append(logfile, last_line)
    assert dogtail.read() == "5,5.5\n6\n"


def test_iterator_with_offsets(logfile_candidates, offset_path):
    """
    Test save offset is not automatically saved once the end of the file is reached
    """

    dogtail = Dogtail(logfile_candidates=logfile_candidates, offset_path=offset_path)

    lines, offsets = list(zip(*dogtail))
    assert len(lines) == 3
    dogtail.write_offset_to_file(offsets[1])

    for i in range(len(offsets) - 1):
        assert offsets[i] < offsets[i + 1]

    dogtail = Dogtail(logfile_candidates=logfile_candidates, offset_path=offset_path)
    lines_new, offsets_new = list(zip(*dogtail))
    assert len(lines_new) == 2
    for a, b in zip(offsets_new, offsets[1:], strict=True):
        assert (a.inode, a.offset) == (b.inode, b.offset)


def test_offset_comparisons(dogtail):
    """Test comparison operators of the Offset dataclass"""
    _, offsets = list(zip(*dogtail))
    for i in range(
        len(offsets) - 1,
    ):
        assert offsets[i] < offsets[i + 1]
        assert offsets[i] <= offsets[i + 1]
        assert offsets[i] <= offsets[i]
        assert offsets[i + 1] > offsets[i]
        assert offsets[i] >= offsets[i]
        assert offsets[i + 1] >= offsets[i]


def test_logrotate_race(dogtail, logfile, logfile_candidates, offset_path):
    old_log_inode = os.stat(logfile).st_ino
    dogtail = Dogtail(logfile_candidates=logfile_candidates, offset_path=offset_path)
    dogtail.read()
    dogtail.update_offset_file()

    dogtail = Dogtail(logfile_candidates=logfile_candidates, offset_path=offset_path)
    logrotate(logfile)
    append(logfile, "")
    new_log_inode = os.stat(logfile).st_ino
    assert new_log_inode != old_log_inode
    dogtail.read()
    dogtail.update_offset_file()

    with open(offset_path, "r") as f:
        inode, offset = int(next(f)), int(next(f))
    assert (inode, offset) in [(old_log_inode, 6), (new_log_inode, 0)]
