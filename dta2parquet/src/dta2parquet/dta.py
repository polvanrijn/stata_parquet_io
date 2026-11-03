"""A fast, memory-mapped reader for Stata ``.dta`` files.

Supports dta formats 113, 114, 115 (Stata 8-13) and 117, 118, 119
(Stata 13+), in either byte order.

Unlike general-purpose readers (pandas, pyreadstat) this one never builds
Python objects per cell: the data section is memory-mapped and each column
is extracted with numpy strided views, so converting a chunk of rows costs
roughly one memcpy per column. It also exposes everything the Stata
metadata envelope needs - including *characteristics* (variable and
dataset notes live there), which the common readers do not surface.
"""

from __future__ import annotations

import mmap
import os
import struct
from dataclasses import dataclass, field

import numpy as np

# --- Stata missing-value thresholds (anything ABOVE these is ., .a-.z) ---
BYTE_MAX = 100
INT_MAX = 32740
LONG_MAX = 2147483620
FLOAT_MISSING = np.float32(2.0**127)  # >= this is missing
DOUBLE_MISSING = 2.0**1023  # >= this is missing

STRL_CODE_NEW = 32768  # 117+ type code for strL


@dataclass
class Variable:
    index: int  # 0-based position
    name: str
    stata_type: str  # byte|int|long|float|double|str|strl
    str_width: int  # bytes for str#, 0 otherwise
    format: str
    label: str
    value_label: str
    offset: int = 0  # byte offset within a row
    width: int = 0  # bytes within a row


@dataclass
class StrlEntry:
    binary: bool
    start: int  # absolute file offset of the payload
    length: int  # payload length (trailing NUL already removed for text)


@dataclass
class DtaFile:
    """Parsed header/metadata of a .dta file plus a memory map of its bytes."""

    path: str
    release: int
    byteorder: str  # "<" or ">"
    nvar: int
    nobs: int
    data_label: str
    timestamp: str
    variables: list[Variable]
    # characteristics: {owner ("_dta" or varname): {charname: value}}
    characteristics: dict[str, dict[str, str]]
    value_labels: dict[str, dict[int, str]]
    encoding: str
    data_offset: int
    row_width: int
    strl_offset: int | None
    _mm: mmap.mmap | None = field(default=None, repr=False)
    _buf: np.ndarray | None = field(default=None, repr=False)
    _strl_index: tuple | None = field(default=None, repr=False)

    # ----------------------------------------------------------------- utils
    def close(self) -> None:
        self._buf = None
        if self._mm is not None:
            try:
                self._mm.close()
            except BufferError:  # numpy views still alive; GC will close
                pass
            self._mm = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    @property
    def buffer(self) -> np.ndarray:
        return self._buf

    def rows(self, start: int, stop: int) -> np.ndarray:
        """Raw (n_rows, row_width) uint8 view onto rows [start, stop)."""
        a = self.data_offset + start * self.row_width
        b = self.data_offset + stop * self.row_width
        return self._buf[a:b].reshape(stop - start, self.row_width)

    def decode(self, raw: bytes) -> str:
        return raw.split(b"\x00", 1)[0].decode(self.encoding, errors="replace")

    # ------------------------------------------------------------------ strL
    def strl_index(self):
        """Index of the GSO (strL) section, built lazily.

        Returns (keys, starts, lengths, binary) as sorted numpy arrays, where
        key = (v << 40) | o identifies the (variable, observation) pair a strL
        reference points to.
        """
        if self._strl_index is None:
            self._strl_index = _parse_gso(self)
        return self._strl_index

    def binary_strl_vars(self) -> set[int]:
        """1-based variable numbers that own at least one binary strL."""
        keys, _, _, binary = self.strl_index()
        if not binary.any():
            return set()
        return set((keys[binary] >> 40).astype(np.int64).tolist())


# ======================================================================
#                              Parsing
# ======================================================================


class DtaFormatError(ValueError):
    pass


def open_dta(path: str | os.PathLike, encoding: str | None = None) -> DtaFile:
    """Open and parse a .dta file's header and metadata (not its data)."""
    path = os.fspath(path)
    with open(path, "rb") as fh:
        size = os.fstat(fh.fileno()).st_size
        if size == 0:
            raise DtaFormatError(f"{path}: empty file")
        mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
    buf = np.frombuffer(mm, dtype=np.uint8)
    try:
        if bytes(buf[:11]) == b"<stata_dta>":
            dta = _parse_new(path, mm, buf, encoding)
        else:
            dta = _parse_old(path, mm, buf, encoding)
        end = dta.data_offset + dta.nobs * dta.row_width
        if end > len(buf):
            dta.close()
            raise DtaFormatError(
                f"{path}: truncated file ({len(buf):,} bytes, data needs {end:,})"
            )
    except Exception:
        del buf
        try:
            mm.close()
        except BufferError:  # views still referenced by the traceback
            pass
        raise
    return dta


class _Cursor:
    def __init__(self, buf: np.ndarray, pos: int, bo: str):
        self.buf = buf
        self.pos = pos
        self.bo = bo

    def read(self, n: int) -> bytes:
        b = bytes(self.buf[self.pos : self.pos + n])
        if len(b) != n:
            raise DtaFormatError("unexpected end of file (truncated file?)")
        self.pos += n
        return b

    def expect(self, tag: bytes) -> None:
        got = self.read(len(tag))
        if got != tag:
            raise DtaFormatError(f"expected {tag!r} at {self.pos - len(tag)}, got {got!r}")

    def peek(self, n: int) -> bytes:
        return bytes(self.buf[self.pos : self.pos + n])

    def u(self, n: int) -> int:
        return int.from_bytes(self.read(n), "little" if self.bo == "<" else "big")

    def i32(self) -> int:
        return struct.unpack(self.bo + "i", self.read(4))[0]


def _strip(raw: bytes, enc: str) -> str:
    return raw.split(b"\x00", 1)[0].decode(enc, errors="replace")


def _type_from_new(code: int) -> tuple[str, int, int]:
    """(stata_type, str_width, byte width) for a 117+ type code."""
    if 1 <= code <= 2045:
        return "str", code, code
    if code == STRL_CODE_NEW:
        return "strl", 0, 8
    return {
        65526: ("double", 0, 8),
        65527: ("float", 0, 4),
        65528: ("long", 0, 4),
        65529: ("int", 0, 2),
        65530: ("byte", 0, 1),
    }[code]


def _type_from_old(code: int) -> tuple[str, int, int]:
    if 1 <= code <= 244:
        return "str", code, code
    return {
        255: ("double", 0, 8),
        254: ("float", 0, 4),
        253: ("long", 0, 4),
        252: ("int", 0, 2),
        251: ("byte", 0, 1),
    }[code]


def _parse_new(path, mm, buf, encoding) -> DtaFile:
    c = _Cursor(buf, 0, "<")
    c.expect(b"<stata_dta><header><release>")
    release = int(c.read(3))
    if release not in (117, 118, 119):
        raise DtaFormatError(f"unsupported dta release {release}")
    c.expect(b"</release><byteorder>")
    bo_raw = c.read(3)
    c.bo = "<" if bo_raw == b"LSF" else ">"
    c.expect(b"</byteorder><K>")
    nvar = c.u(4 if release == 119 else 2)
    c.expect(b"</K><N>")
    nobs = c.u(4 if release == 117 else 8)
    c.expect(b"</N><label>")
    lab_len = c.u(1 if release == 117 else 2)
    enc = encoding or ("utf-8" if release >= 118 else "latin-1")
    data_label = c.read(lab_len).decode(enc, errors="replace")
    c.expect(b"</label><timestamp>")
    ts_len = c.u(1)
    timestamp = c.read(ts_len).decode(enc, errors="replace")
    c.expect(b"</timestamp></header>")

    c.expect(b"<map>")
    offsets = [c.u(8) for _ in range(14)]
    c.expect(b"</map>")
    (
        _o_stata,
        _o_map,
        o_types,
        _o_names,
        _o_sort,
        _o_formats,
        _o_vlnames,
        _o_varlabels,
        _o_chars,
        o_data,
        o_strls,
        o_vallabels,
        _o_end_stata,
        _o_eof,
    ) = offsets

    namelen = 33 if release == 117 else 129
    fmtlen = 49 if release == 117 else 57
    varlablen = 81 if release == 117 else 321

    # The fixed-width blocks and characteristics are contiguous, so read
    # them sequentially instead of trusting each map entry: Stata 13 (117)
    # is known to write a wrong offset for <variable_labels>.
    c.pos = o_types
    c.expect(b"<variable_types>")
    type_codes = np.frombuffer(c.read(2 * nvar), dtype=c.bo + "u2").astype(int).tolist()
    c.expect(b"</variable_types>")

    def fixed_block(tag, width):
        c.expect(b"<" + tag + b">")
        raw = c.read(width * nvar)
        c.expect(b"</" + tag + b">")
        return [_strip(raw[i * width : (i + 1) * width], enc) for i in range(nvar)]

    names = fixed_block(b"varnames", namelen)
    c.expect(b"<sortlist>")
    c.read((4 if release == 119 else 2) * (nvar + 1))
    c.expect(b"</sortlist>")
    formats = fixed_block(b"formats", fmtlen)
    vlnames = fixed_block(b"value_label_names", namelen)
    varlabels = fixed_block(b"variable_labels", varlablen)

    variables = []
    off = 0
    for i in range(nvar):
        st, sw, w = _type_from_new(type_codes[i])
        variables.append(
            Variable(i, names[i], st, sw, formats[i], varlabels[i], vlnames[i], off, w)
        )
        off += w

    # characteristics
    chars: dict[str, dict[str, str]] = {}
    c.expect(b"<characteristics>")
    while c.peek(4) == b"<ch>":
        c.read(4)
        ln = c.u(4)
        body = c.read(ln)
        owner = _strip(body[:namelen], enc)
        cname = _strip(body[namelen : 2 * namelen], enc)
        value = _strip(body[2 * namelen :], enc)
        chars.setdefault(owner, {})[cname] = value
        c.expect(b"</ch>")
    c.expect(b"</characteristics>")
    if bytes(buf[o_data : o_data + 6]) != b"<data>":
        o_data = c.pos

    # value labels
    vlabels: dict[str, dict[int, str]] = {}
    c.pos = o_vallabels
    c.expect(b"<value_labels>")
    while c.peek(5) == b"<lbl>":
        c.read(5)
        ln = c.u(4)
        lname = _strip(c.read(namelen), enc)
        c.read(3)
        vlabels[lname] = _parse_vl_table(c.read(ln), c.bo, enc)
        c.expect(b"</lbl>")

    return DtaFile(
        path=path,
        release=release,
        byteorder=c.bo,
        nvar=nvar,
        nobs=nobs,
        data_label=data_label,
        timestamp=timestamp,
        variables=variables,
        characteristics=chars,
        value_labels=vlabels,
        encoding=enc,
        data_offset=o_data + len(b"<data>"),
        row_width=off,
        strl_offset=o_strls,
        _mm=mm,
        _buf=buf,
    )


def _parse_old(path, mm, buf, encoding) -> DtaFile:
    release = int(buf[0])
    if release not in (113, 114, 115):
        raise DtaFormatError(
            f"{path}: unsupported dta release {release} (supported: 113-115, 117-119); "
            "re-save it with a newer Stata (`saveold`/`save`)"
        )
    bo = ">" if buf[1] == 1 else "<"
    c = _Cursor(buf, 4, bo)
    nvar = c.u(2)
    nobs = c.u(4)
    enc = encoding or "latin-1"
    data_label = _strip(c.read(81), enc)
    timestamp = _strip(c.read(18), enc)
    type_codes = list(c.read(nvar))
    names = [_strip(c.read(33), enc) for _ in range(nvar)]
    c.read(2 * (nvar + 1))  # sortlist
    fmtlen = 12 if release == 113 else 49
    formats = [_strip(c.read(fmtlen), enc) for _ in range(nvar)]
    vlnames = [_strip(c.read(33), enc) for _ in range(nvar)]
    varlabels = [_strip(c.read(81), enc) for _ in range(nvar)]

    chars: dict[str, dict[str, str]] = {}
    while True:
        dtype = c.u(1)
        ln = c.u(4)
        if dtype == 0 and ln == 0:
            break
        body = c.read(ln)
        if dtype == 1:
            owner = _strip(body[:33], enc)
            cname = _strip(body[33:66], enc)
            chars.setdefault(owner, {})[cname] = _strip(body[66:], enc)

    variables = []
    off = 0
    for i in range(nvar):
        st, sw, w = _type_from_old(type_codes[i])
        variables.append(
            Variable(i, names[i], st, sw, formats[i], varlabels[i], vlnames[i], off, w)
        )
        off += w

    data_offset = c.pos
    c.pos = data_offset + off * nobs
    vlabels: dict[str, dict[int, str]] = {}
    while c.pos + 40 <= len(buf):
        ln = c.u(4)
        lname = _strip(c.read(33), enc)
        c.read(3)
        vlabels[lname] = _parse_vl_table(c.read(ln), bo, enc)

    return DtaFile(
        path=path,
        release=release,
        byteorder=bo,
        nvar=nvar,
        nobs=nobs,
        data_label=data_label,
        timestamp=timestamp,
        variables=variables,
        characteristics=chars,
        value_labels=vlabels,
        encoding=enc,
        data_offset=data_offset,
        row_width=off,
        strl_offset=None,
        _mm=mm,
        _buf=buf,
    )


def _parse_vl_table(tab: bytes, bo: str, enc: str) -> dict[int, str]:
    n, txtlen = struct.unpack(bo + "ii", tab[:8])
    offs = np.frombuffer(tab, dtype=bo + "i4", count=n, offset=8)
    vals = np.frombuffer(tab, dtype=bo + "i4", count=n, offset=8 + 4 * n)
    txt = tab[8 + 8 * n : 8 + 8 * n + txtlen]
    out = {}
    for o, v in zip(offs.tolist(), vals.tolist()):
        end = txt.find(b"\x00", o)
        out[int(v)] = txt[o : end if end >= 0 else len(txt)].decode(enc, errors="replace")
    return out


def _parse_gso(dta: DtaFile):
    """Scan the <strls> section, recording payload positions (not payloads)."""
    keys, starts, lens, binary = [], [], [], []
    if dta.strl_offset is None:
        e = np.empty(0, np.uint64)
        return e, np.empty(0, np.int64), np.empty(0, np.int64), np.empty(0, bool)
    buf = dta._buf
    c = _Cursor(buf, dta.strl_offset, dta.byteorder)
    c.expect(b"<strls>")
    osize = 4 if dta.release == 117 else 8
    while c.peek(3) == b"GSO":
        c.pos += 3
        v = c.u(4)
        o = c.u(osize)
        t = c.u(1)
        ln = c.u(4)
        start = c.pos
        c.pos += ln
        is_bin = t == 129
        if not is_bin and ln > 0 and buf[start + ln - 1] == 0:
            ln -= 1
        keys.append((v << 40) | o)
        starts.append(start)
        lens.append(ln)
        binary.append(is_bin)
    keys_a = np.array(keys, dtype=np.uint64)
    order = np.argsort(keys_a, kind="stable")
    return (
        keys_a[order],
        np.array(starts, dtype=np.int64)[order],
        np.array(lens, dtype=np.int64)[order],
        np.array(binary, dtype=bool)[order],
    )


def strl_refs_to_keys(raw8: np.ndarray, release: int, byteorder: str) -> np.ndarray:
    """Turn (n, 8) uint8 strL reference cells into (v << 40) | o keys."""
    x = np.ascontiguousarray(raw8).view(byteorder + "u8").reshape(-1).astype(np.uint64)
    vbits = {117: 32, 118: 16, 119: 24}[release]
    if byteorder == "<":
        v = x & np.uint64((1 << vbits) - 1)
        o = x >> np.uint64(vbits)
    else:
        v = x >> np.uint64(64 - vbits)
        o = x & np.uint64((1 << (64 - vbits)) - 1)
    return (v << np.uint64(40)) | o
