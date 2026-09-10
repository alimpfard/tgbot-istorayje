import array
import ctypes
import ctypes.util
import io
import struct
import wave

OPUS_RATE = 48000
FRAME_MS = 20
FRAME_SAMPLES = OPUS_RATE * FRAME_MS // 1000 # 960
OPUS_APPLICATION_VOIP = 2048
OPUS_APPLICATION_AUDIO = 2049
OPUS_OK = 0
PRE_SKIP = 312
PACKETS_PER_PAGE = 50 # 1s of audio per ogg page
MAX_PACKET = 4000

_lib = None


def _opus():
    global _lib
    if _lib is not None:
        return _lib
    name = ctypes.util.find_library("opus") or "libopus.so.0"
    lib = ctypes.CDLL(name)
    lib.opus_encoder_create.restype = ctypes.c_void_p
    lib.opus_encoder_create.argtypes = [
        ctypes.c_int32, ctypes.c_int, ctypes.c_int, ctypes.POINTER(ctypes.c_int)
    ]
    lib.opus_encode.restype = ctypes.c_int32
    lib.opus_encode.argtypes = [
        ctypes.c_void_p, ctypes.POINTER(ctypes.c_int16), ctypes.c_int,
        ctypes.POINTER(ctypes.c_ubyte), ctypes.c_int32,
    ]
    lib.opus_encoder_destroy.restype = None
    lib.opus_encoder_destroy.argtypes = [ctypes.c_void_p]
    lib.opus_strerror.restype = ctypes.c_char_p
    lib.opus_strerror.argtypes = [ctypes.c_int]
    _lib = lib
    return lib


def available():
    try:
        _opus()
        return True
    except OSError:
        return False


def _to_int16(frames, sampwidth):
    if sampwidth == 2:
        out = array.array("h")
        out.frombytes(frames)
        return out
    if sampwidth == 1:
        # 8-bit wav is unsigned
        return array.array("h", ((b - 128) << 8 for b in frames))
    if sampwidth == 3:
        return array.array(
            "h",
            (struct.unpack("<i", frames[i:i + 3] + (b"\xff" if frames[i + 2] & 0x80 else b"\x00"))[0] >> 8
             for i in range(0, len(frames), 3)),
        )
    if sampwidth == 4:
        src = array.array("i")
        src.frombytes(frames)
        return array.array("h", (s >> 16 for s in src))
    raise Exception(f"unsupported wav sample width {sampwidth}")


def _resample(samples, channels, src_rate, dst_rate):
    if src_rate == dst_rate:
        return samples
    n_in = len(samples) // channels
    n_out = int(n_in * dst_rate / src_rate)
    out = array.array("h", bytes(2 * n_out * channels))
    step = src_rate / dst_rate
    last = n_in - 1
    for i in range(n_out):
        pos = i * step
        j = int(pos)
        frac = pos - j
        k = j + 1 if j < last else j
        for c in range(channels):
            a = samples[j * channels + c]
            b = samples[k * channels + c]
            out[i * channels + c] = int(a + (b - a) * frac)
    return out


def decode_wav(data):
    with wave.open(io.BytesIO(data), "rb") as w:
        channels = w.getnchannels()
        rate = w.getframerate()
        width = w.getsampwidth()
        frames = w.readframes(w.getnframes())
    samples = _to_int16(frames, width)
    if channels > 2:
        mono = array.array("h", (
            int(sum(samples[i:i + channels]) / channels)
            for i in range(0, len(samples) - channels + 1, channels)
        ))
        samples, channels = mono, 1
    samples = _resample(samples, channels, rate, OPUS_RATE)
    return samples, channels, rate


def _crc_table():
    table = []
    for i in range(256):
        r = i << 24
        for _ in range(8):
            r = ((r << 1) ^ 0x04C11DB7) if r & 0x80000000 else (r << 1)
        table.append(r & 0xFFFFFFFF)
    return table


_CRC = _crc_table()


def _ogg_crc(data):
    crc = 0
    for b in data:
        crc = ((crc << 8) & 0xFFFFFFFF) ^ _CRC[((crc >> 24) & 0xFF) ^ b]
    return crc


def _ogg_page(packets, serial, seq, granule, bos=False, eos=False):
    lacing = bytearray()
    for p in packets:
        n = len(p)
        while n >= 255:
            lacing.append(255)
            n -= 255
        lacing.append(n)
    if len(lacing) > 255:
        raise Exception("too many segments for one ogg page")
    flags = (0x02 if bos else 0) | (0x04 if eos else 0)
    header = struct.pack(
        "<4sBBqIIIB", b"OggS", 0, flags, granule, serial, seq, 0, len(lacing)
    ) + bytes(lacing)
    body = b"".join(packets)
    crc = _ogg_crc(header + body)
    return header[:22] + struct.pack("<I", crc) + header[26:] + body


def _opus_head(channels, input_rate):
    return struct.pack("<8sBBHIhB", b"OpusHead", 1, channels, PRE_SKIP, input_rate, 0, 0)


def _opus_tags():
    vendor = b"istorayje"
    return b"OpusTags" + struct.pack("<I", len(vendor)) + vendor + struct.pack("<I", 0)


def wav_to_ogg_opus(data, voice=True):
    lib = _opus()
    samples, channels, src_rate = decode_wav(data)

    err = ctypes.c_int(0)
    enc = lib.opus_encoder_create(
        OPUS_RATE, channels,
        OPUS_APPLICATION_VOIP if voice else OPUS_APPLICATION_AUDIO,
        ctypes.byref(err),
    )
    if not enc or err.value != OPUS_OK:
        raise Exception(f"opus_encoder_create: {lib.opus_strerror(err.value).decode()}")

    out = io.BytesIO()
    serial = 0x1570_4A3E
    seq = 0
    try:
        out.write(_ogg_page([_opus_head(channels, src_rate)], serial, seq, 0, bos=True))
        seq += 1
        out.write(_ogg_page([_opus_tags()], serial, seq, 0))
        seq += 1

        frame_len = FRAME_SAMPLES * channels
        total = len(samples)
        # pad the tail to a whole frame
        if total % frame_len:
            samples.frombytes(bytes(2 * (frame_len - total % frame_len)))
        buf = (ctypes.c_ubyte * MAX_PACKET)()
        pcm = (ctypes.c_int16 * frame_len)()

        packets = []
        granule = PRE_SKIP
        n_frames = len(samples) // frame_len
        for f in range(n_frames):
            pcm[:] = samples[f * frame_len:(f + 1) * frame_len]
            n = lib.opus_encode(enc, pcm, FRAME_SAMPLES, buf, MAX_PACKET)
            if n < 0:
                raise Exception(f"opus_encode: {lib.opus_strerror(n).decode()}")
            packets.append(bytes(buf[:n]))
            granule += FRAME_SAMPLES
            last = f == n_frames - 1
            if len(packets) == PACKETS_PER_PAGE or last:
                out.write(_ogg_page(packets, serial, seq, granule, eos=last))
                seq += 1
                packets = []
        if n_frames == 0:
            out.write(_ogg_page([], serial, seq, granule, eos=True))
    finally:
        lib.opus_encoder_destroy(enc)
    return out.getvalue()
