from os import fstat
from pathlib import Path
from typing import List, Optional, Set, TextIO, Tuple
import logging

logger = logging.getLogger(__name__)


class Offset:
    """Data-class to store file-offsets"""

    def __init__(self, counter, inode, offset):
        self.counter = counter
        self.inode = inode
        self.offset = offset

    def __eq__(self, other):
        return self.counter == other.counter and self.offset == other.offset

    def __lt__(self, other):
        return self.counter < other.counter or (
            self.counter == other.counter and self.offset < other.offset
        )

    def __le__(self, other):
        return self.__lt__(other) or self.__eq__(other)

    def __gt__(self, other):
        return not self.__le__(other)

    def __ge__(self, other):
        return not self.__le__(other) or self.__eq__(other)

    def __repr__(self):
        return "Offset(counter=%d, inode=%d, offset=%d)" % (
            self.counter,
            self.inode,
            self.offset,
        )


class Dogtail:
    _fh: Optional[TextIO]

    def __init__(self, logfile_candidates: List[Path], offset_path: Path):
        self._logfile_candidates = logfile_candidates
        self._offset_file = offset_path

        self._fh = None
        self.current_inode = None
        self.curr_offset = None

        self._counter = 1
        if self._offset_file.exists() and self._offset_file.stat().st_size:
            with open(self._offset_file) as f:
                inode, offset = [int(line.strip()) for line in f]
                logger.info(f"Read offset: {inode=} {offset=}")
                self._open_known_file(inode, offset)

    def __iter__(self):
        return self

    @property
    def filehandle(self) -> Optional[TextIO]:
        if self._fh is None:
            self._open()

        return self._fh

    def _get_next_line(self) -> Tuple[str, Offset]:
        if self.filehandle is None or self.filehandle.closed:
            raise StopIteration

        curr_offset = self.filehandle.tell()
        fh_inode = fstat(self.filehandle.fileno()).st_ino
        line = self.filehandle.readline()

        if not line.endswith("\n"):
            self.filehandle.seek(curr_offset)
            raise StopIteration
        if not line:
            raise StopIteration

        self.curr_offset = self.filehandle.tell()
        self.current_inode = fstat(self.filehandle.fileno()).st_ino

        return line, Offset(counter=self._counter, inode=fh_inode, offset=curr_offset)

    def _close(self):
        logger.info(f"Closing {self._fh=}")
        if not self._fh:
            return

        self._fh.close()
        self._fh = None
        logger.info(f"Closed {self._fh=}")

    def _try_open(self, path) -> Optional[TextIO]:
        try:
            return open(path, "r", 1, errors="backslashreplace")
        except OSError as e:
            logger.info(f"Failed to open {e=}")

    def _open_known_file(self, inode, offset):
        logger.info(f"Finding {inode=} in {self._logfile_candidates}")
        for filename in self._logfile_candidates:
            logger.info(f"Trying {filename=}")

            if (fh := self._try_open(filename)) is not None:
                fh_inode = fstat(fh.fileno()).st_ino
                if inode == fh_inode:
                    fh.seek(offset)
                    self._fh = fh
                    self.curr_offset = offset
                    self.current_inode = fh_inode

                    logger.info(f"Opened {self._fh=} at {offset=}")
                    return

        else:
            logger.info("No matching file found")

    def _open_first_file(self):
        if (fh := self._try_open(self._logfile_candidates[0])) is not None:
            fh_inode = fstat(fh.fileno()).st_ino
            logging.info(f"{fh_inode=} {self.current_inode=}")
            if (
                fh_inode == self.current_inode
            ):  # If the first file is also the file from the offset, we're done
                logging.info("aborting")
                return
            self._fh = fh
            logger.info(f"Opened {self._fh=}")
            self._counter += 1
            self.curr_offset = 0
            self.current_inode = fh_inode
            return

    def _open(self):
        if not self._fh and self._counter == 1:
            self._open_first_file()

    def __next__(self) -> Tuple[str, Offset]:
        logger.info("next")
        try:
            line = self._get_next_line()
            logger.info(f"read 1 {line=}")
            return line
        except StopIteration:
            logger.info("EOF")
            # EOF. open next file if possible and continue, else stop iteration
            # open up current logfile and continue
            self._close()
            line = self._get_next_line()
            logger.info(f"read 2 {line=}")
            return line

    def readlines(self):
        return [line for line, _ in self]

    def read(self):
        return "".join(self.readlines())

    def write_offset_to_file(self, offset):
        """Writes an `Offset` to the offset file"""
        with open(self._offset_file, "w") as f:
            f.write("%s\n%s\n" % (offset.inode, offset.offset))
        logging.info(f"Wrote offset to file {offset=}")

    def update_offset_file(self):
        with open(self._offset_file, "w") as f:
            f.write("%s\n%s\n" % (self.current_inode, self.curr_offset))
        logging.info(f"Wrote offset to file {self.current_inode=} {self.curr_offset=}")
