#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2010 Phil Sung
#
# This file is part of rdiff-snapshot-fs.
#
# rdiff-snapshot-fs is free software: you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program. If not, see <http://www.gnu.org/licenses/>.


# Usage:        ./rdiff-snapshot-fs.py <rdiff-backup-repository> <mountpoint>
# Unmount with: fusermount -u <mountpoint>
#
# Requirements: rdiff-backup, FUSE Python bindings

import errno
import fuse
import os
import re
import stat
import subprocess
import sys
import tempfile
import time

fuse.fuse_python_api = (0, 2)

# The interesting lines in the output of "rdiff-backup -l" (list all snapshots)
# should match this pattern.
INCREMENTS_PATTERN = re.compile(r"^    increments\.([-0-9T:]+)\.dir   [A-Za-z0-9 :]+$")

# Increment files in the increments/ directory should match the following
# pattern. We use the pattern to extract the interesting components from the
# filename, an example of which might be
# "foo.txt.2009-09-17T00:01:23-07:00.diff.gz".
INCREMENT_FILE_PATTERN = re.compile(
    r"^(.*)\.([0-9]{4}-[0-9]{2}-[0-9]{2}" +
    r"T[0-9]{2}:[0-9]{2}:[0-9]{2}-[0-9]{2}:[0-9]{2})" +
    r"\.((diff|snapshot)\.gz|snapshot|dir|missing)$")

def invoke_command(*cmd):
    """
    Invoke cmd at the shell and return a list of its output lines.
    """
    s = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=False)
    (stdout, stderr) = s.communicate()
    return stdout.split("\n")

def get_path_components(path):
    """
    Splits path (which is an absolute path in the filesystem, starting with
    "/") and returns a list of the components.
    """
    assert path.startswith("/")
    return path[1:].split("/")

def is_root(components):
    return len(components) == 0 or components == [""]

def is_snapshot_dir(components):
    return len(components) == 1

def parse_increment_filename(filename):
    """
    Given an increment filename (basename only), returns a tuple containing the
    basename of the underlying file, the snapshot time at which the increment
    was created, and the type of the increment (e.g. "diff" or "snapshot").
    """
    match = INCREMENT_FILE_PATTERN.match(filename)
    if match:
        basename = match.group(1)
        snapshot_time = match.group(2)
        objtype = match.group(3)
        return (basename, snapshot_time, objtype)
    else:
        raise ValueError("Invalid increment filename: " + filename)

# Possible file types
NONEXISTENT = "NONEXISTENT"
REGULAR_FILE = "REGULAR_FILE"
DIRECTORY = "DIRECTORY"
LINK = "LINK"

def get_file_type(stat_mode):
    if stat.S_ISREG(stat_mode):
        return REGULAR_FILE
    elif stat.S_ISDIR(stat_mode):
        return DIRECTORY
    elif stat.S_ISLNK(stat_mode):
        return LINK
    else:
        raise IOError("Unsupported mode: " + stat_mode)

class DeferredFile():
    """
    A DeferredFile object encapsulates all the information needed to
    reconstruct a particular version of a file, but only reconstructs the file
    data and metadata lazily.

    Creating such a data structure is useful because the information needed to
    create a DeferredFile for all the files in a directory can be done by
    listing the increments in that directory. We can cache the DeferredFile
    objects so that inspecting all the files in the directory requires only one
    scan instead of one scan per file.
    """
    def __init__(self, name, backing_file, file_type):
        self.name = name
        # These are the possibilities for the internal representation:
        #
        # 1. Representing a file that doesn't exist. In this case,
        # self.file_type is set to NONEXISTENT.
        #
        # 2. Representing a file that does exist. We store the most recent full
        # snapshot of the file (in self.backing_file), as well as a sequence of
        # reverse diffs, to be applied in order, to obtain the file that we
        # actually want.
        #
        # At present, whenever we have reverse diffs, we just invoke
        # rdiff-backup to restore the file. In that case we only require the
        # name of the corresponding increment file. So we don't really need to
        # store the whole list of increments, only the most "recent" (oldest)
        # one. But if we were ever to implement the file patching and
        # restoration natively here, we would need to hang on to the full list.
        self.backing_file = None
        self.diffs = []
        if backing_file:
            self.backing_file = backing_file
            self.backing_file_is_increment = False
            self.file_type = file_type
        else:
            self.file_type = NONEXISTENT
        # Store the most recent materialized file, and the associated path of
        # the increment file. If the client asks for the same file again, we'll
        # still have it lying around.
        self.most_recent_increment = None
        self.most_recent_materialized_file = None
    def _clear_diffs(self):
        "Clear the backing file and the stack of diffs."
        self.backing_file = None
        self.diffs[:] = []
    def apply(self, change_type, filename):
        "Applies a change to this deferred file."
        if change_type == 'missing':
            # File does not exist at or before this increment.
            self.file_type = NONEXISTENT
            self._clear_diffs()
            return
        self.file_exists = True
        if change_type == 'dir':
            # This is actually a directory.
            self.file_type = DIRECTORY
            self._clear_diffs()
            return
        if change_type == 'snapshot' or change_type == 'snapshot.gz':
            # This is a snapshot (or a gzipped snapshot) of this file.
            self.file_type = get_file_type(os.lstat(filename).st_mode)
            # Since we have the full file data, the previous backing files and
            # any diffs we've seen in the interim are now irrelevant. Forget
            # them.
            self._clear_diffs()
            self.backing_file = filename
            self.backing_file_is_increment = True
        if change_type == 'diff.gz':
            # This is a reverse diff. Apply it.
            self.diffs.append(filename)
    def get_direntry(self):
        """
        Returns a Direntry associated with this file, or raises KeyError if the
        file doesn't exist at the specified snapshot.
        """
        if self.file_type == NONEXISTENT:
            raise KeyError("File does not exist")
        return fuse.Direntry(self.name)
    def getattr(self):
        """
        Returns the attributes associated with this file.
        """
        if self.file_type == DIRECTORY:
            mtime = int(time.time())
            mode = stat.S_IFDIR | 0555
            size = 4096
            return SnapshotFsStat(mtime, mode, size = size)
        elif self.file_type == LINK:
            assert len(self.diffs) == 0
            statresult = os.lstat(self.backing_file)
            return SnapshotFsStat(statresult.st_mtime,
                                  statresult.st_mode & ~0222,
                                  statresult.st_size)
        elif self.file_type == REGULAR_FILE:
            # Computing the true size of the file may be expensive. Instead
            # we'll use the size of the nearest available snapshot. Even
            # determining the full size of a gzipped file may be expensive.
            # There may not be any really good options here. It may be a good
            # idea to allow the user to select between a "fast" mode and a
            # "slow but correct" mode.
            statresult = os.lstat(self.backing_file)
            size = statresult.st_size
            # TODO: figure out how reverse diffs affect the mode and mtime.
            # Disable write bit even if the backing file had it enabled.
            mode  = statresult.st_mode & ~0222
            mtime = statresult.st_mtime
            return SnapshotFsStat(mtime, mode, size = size)
    def readlink(self):
        """
        Returns the target associated with the current file, if it's a symlink.
        """
        assert self.file_type == LINK
        return os.readlink(self.backing_file)
    def read(self, size, offset):
        """
        Returns file data.
        """
        assert self.file_type == REGULAR_FILE
        # The backing file can be either (1) in the mirror (the most recent
        # snapshot), or an increment file: either a (2) .snapshot or (3)
        # .snapshot.gz file somewhere in in the increments/ directory. In cases
        # (1) and (2), when the client asks for data from the file, we can
        # provide direct access to the underlying file.
        #
        # Otherwise, we have to reconstruct the file by possibly unzipping the
        # backing file and then applying any reverse diffs. In this case we
        # defer to rdiff-backup.
        if len(self.diffs) == 0 \
                and (not self.backing_file_is_increment \
                         or self.backing_file.endswith(".snapshot")):
            with open(self.backing_file, 'r') as source_file:
                source_file.seek(offset, os.SEEK_SET)
                return source_file.read(size)
        else:
            # If present, the last diff in our sequence represents the
            # increment that identifies the data we want.
            if len(self.diffs) > 0:
                source_increment_file = self.diffs[-1]
            else:
                source_increment_file = self.backing_file

            # Cache the file data so we don't have to reconstruct the same file
            # if the client asks for the same file on the next request.
            if source_increment_file != self.most_recent_increment:
                # Remove the previous tempfile so we don't end up with an
                # arbitrarily large number of tempfiles floating around.
                if self.most_recent_materialized_file != None:
                    os.unlink(self.most_recent_materialized_file)
                (_, dest_file) = tempfile.mkstemp()
                # Restore the file into dest_file using rdiff-backup.
                invoke_command(
                    "/usr/bin/rdiff-backup", "--force", source_increment_file,
                    dest_file)
                self.most_recent_increment = source_increment_file
                self.most_recent_materialized_file = dest_file

            with open(self.most_recent_materialized_file, 'r') as cached_file:
                cached_file.seek(offset, os.SEEK_SET)
                return cached_file.read(size)

class SnapshotFsStat(fuse.Stat):
    def __init__(self, mtime, mode, size = 4096):
        self.st_mode = mode
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 2 if stat.S_ISDIR(mode) else 1
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = size
        self.st_atime = mtime
        self.st_mtime = mtime
        self.st_ctime = mtime

class RdiffSnapshotFs(fuse.Fuse):
    """
    Filesystem that provides a read-only view of snapshots in a repository
    created by rdiff-backup.
    """
    def __init__(self, repository_path, *args, **kw):
        fuse.Fuse.__init__(self, *args, **kw)
        self.repository_path = repository_path
        self.increments_path = os.path.join(
            repository_path, "rdiff-backup-data", "increments")
        self.snapshot_list = None

        self.last_requested_snapshot_ts = None
        self.last_relative_path = None

    def compute_snapshots(self):
        """
        Yields a sequence of available snapshots.
        """
        output_lines = invoke_command("/usr/bin/rdiff-backup", "-l",
                                      self.repository_path)
        for line in output_lines:
            match = INCREMENTS_PATTERN.match(line.rstrip())
            if match:
                snapshot_ts = match.group(1)
                yield snapshot_ts

    def get_snapshots(self):
        """
        Return a list of all available snapshots, in chronological order.
        Caches the result.
        """
        # TODO: invalidate cache when the repository directory has been
        # modified.
        if self.snapshot_list is None:
            self.snapshot_list = list(self.compute_snapshots())
        return self.snapshot_list

    def get_deferred_dir(self, requested_snapshot_ts, relative_path):
        """
        Returns a 'deferred directory', which is a data structure that contains
        all the information needed to reconstruct a historical snapshot, but
        doesn't actually do any of the work needed to do so until asked to.
        """
        # Memoizing wrapper around build_deferred_dir.
        #
        # TODO: to improve performance we could cache more than one deferred
        # directory.
        if requested_snapshot_ts != self.last_requested_snapshot_ts \
                or relative_path != self.last_relative_path:
            self.last_requested_snapshot_ts = requested_snapshot_ts
            self.last_relative_path = relative_path
            self.last_deferred_dir = self.build_deferred_dir(
                requested_snapshot_ts, relative_path)
        return self.last_deferred_dir

    def build_deferred_dir(self, requested_snapshot_ts, relative_path):
        """
        Returns a deferred directory, which is a dict mapping basenames to
        DeferredFile objects.
        """
        # To build a deferred directory, we need to dig around in the diffs to
        # figure out what files existed at the time this snapshot was taken.
        try:
            files = os.listdir(
                os.path.join(self.repository_path, *relative_path))
        except OSError:
            # Directory doesn't exist in current snapshot, i.e. it was deleted
            # since the requested snapshot was written. For diffing purposes
            # start with an empty base.
            files = []
        increment_dir = os.path.join(self.increments_path, *relative_path)
        try:
            increment_files = os.listdir(increment_dir)
        except OSError:
            # No corresponding directory exists in the increments/ directory.
            # That means no reverse diffs were recorded.
            increment_files = []

        file_info = {}

        # List all the files in the current snapshot of the directory.
        for filename in files:
            if filename == "rdiff-backup-data":
                continue
            backing_file_path = os.path.join(
                os.path.join(self.repository_path, *relative_path),
                filename)
            file_info[filename] = \
                DeferredFile(filename, backing_file_path,
                             get_file_type(os.lstat(backing_file_path).st_mode))

        # Identify all the diffs that are relevant to this snapshot. This means
        # all diffs with timestamps between this snapshot and the present time.
        diff_info = {}
        for increment_file in increment_files:
            try:
                file_mode = os.lstat(
                    os.path.join(increment_dir, increment_file)).st_mode
                if stat.S_ISREG(file_mode):
                    (basename, timestamp, objtype) = \
                        parse_increment_filename(increment_file)
                    # TODO: do a proper comparison with dates. Dates sort
                    # lexicographically, but only approximately...
                    if timestamp >= requested_snapshot_ts:
                        if basename not in diff_info:
                            diff_info[basename] = []
                        diff_info[basename].append(
                            (timestamp, objtype, increment_file))
            except OSError:
                pass

        for basename in diff_info:
            # For each file, process the diffs in reverse chronological order
            # to reconstruct the original directory listing.
            diff_info[basename].sort(
                key = lambda change_info : change_info[0],
                reverse = True)
            for (timestamp, change_type, increment_file) in diff_info[basename]:
                # Create a fake basefile we can apply diffs against.
                if basename not in file_info:
                    file_info[basename] = DeferredFile(basename, None, NONEXISTENT)
                file_info[basename].apply(
                    change_type,
                    os.path.join(increment_dir, increment_file))

        return file_info

    # ----- FUSE API functions below -----

    def getattr(self, path):
        """
        Return the attributes associated with PATH.
        """
        components = get_path_components(path)

        if is_root(components) or is_snapshot_dir(components):
            mtime = int(time.time())
            mode = stat.S_IFDIR | 0555
            size = 4096
            return SnapshotFsStat(mtime, mode, 4096)

        # This is a file underneath a snapshot directory.
        snapshots = self.get_snapshots()
        # Current snapshot?
        if components[0] == snapshots[-1]:
            # The underlying file is in the mirror. Just return the attributes
            # associated with that file.
            return os.lstat(
                os.path.join(self.repository_path, *components[1:]))
        else:
            # The file is in a historical snapshot. Construct the deferred
            # directory and obtain the attributes from there.
            file_info = self.get_deferred_dir(components[0], components[1:-1])
            return file_info[components[-1]].getattr()

    def readdir(self, path, offset):
        """
        Lists the contents of a directory, returning a sequence of
        fuse.Direntry objects.
        """
        # Entries common to all directories.
        dir_entries = [ ".", ".." ]
        for p in dir_entries:
            yield fuse.Direntry(p)

        components = get_path_components(path)

        if is_root(components):
            # Root directory. List all available snapshots.
            for snapshot_ts in self.get_snapshots():
                yield fuse.Direntry(snapshot_ts)
            return

        snapshots = self.get_snapshots()
        if components[0] not in snapshots:
            raise ValueError("Directory not found")

        # This is a file underneath a snapshot directory.
        if components[0] == snapshots[-1]:
            # The file is in the mirror. Read directly from there.
            files = os.listdir(
                os.path.join(self.repository_path, *components[1:]))
            for filename in files:
                if filename != "rdiff-backup-data":
                    yield fuse.Direntry(filename)
        else:
            file_info = self.get_deferred_dir(components[0], components[1:])
            for entry in file_info.values():
                try:
                    direntry = entry.get_direntry()
                    yield direntry
                except KeyError:
                    pass

    def readlink(self, path):
        """
        Return the attributes associated with PATH.
        """
        components = get_path_components(path)

        if is_root(components) or is_snapshot_dir(components):
            raise ValueError(path + " doesn't represent a link")

        # This is a file underneath a snapshot directory.
        snapshots = self.get_snapshots()
        if components[0] == snapshots[-1]: # Current snapshot?
            return os.readlink(
                os.path.join(self.repository_path, *components[1:]))
        else:
            file_info = self.get_deferred_dir(components[0], components[1:-1])
            return file_info[components[-1]].readlink()

    def read(self, path, size, offset):
        components = get_path_components(path)

        if is_root(components) or is_snapshot_dir(components):
            raise ValueError(path + " doesn't represent a file")

        # This is a file underneath a snapshot directory.
        snapshots = self.get_snapshots()
        if components[0] == snapshots[-1]: # Current snapshot?
            fd = os.open(os.path.join(self.repository_path, *components[1:]),
                         os.O_RDONLY)
            try:
                os.lseek(fd, offset, os.SEEK_SET)
                return os.read(fd, size)
            finally:
                os.close(fd)
        else:
            file_info = self.get_deferred_dir(components[0], components[1:-1])
            return file_info[components[-1]].read(size, offset)

    def open(self, path, flags):
        return 0
    def release(self, path, flags):
        return 0
    def truncate(self, path, size):
        return 0
    def utime(self, path, times):
        return 0
    def fsync(self, path, isfsyncfile):
        return 0
    def mknod(self, path, mode, dev):
        return -1
    def unlink(self, path):
        return -1
    def write(self, path, buf, offset):
        return -1
    def rename(self, pathfrom, pathto):
        return -1
    def mkdir(self, path, mode):
        return -1
    def rmdir(self, path):
        return -1

def main(argv):
    usage_msg = "Displays snapshots from rdiff-backup repositories."
    fs = RdiffSnapshotFs(
        repository_path = os.path.abspath(argv[1]),
        version = "rdiff-snapshot-fs 0.1",
        usage = usage_msg,
        dash_s_do = "setsingle")
    fs.parse(errex = 1)
    fs.main()

if __name__ == "__main__":
    main(sys.argv)
