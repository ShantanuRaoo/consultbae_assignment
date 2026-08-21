"""
Audio property extraction for Task 3.

Note: loudness_db is an RMS-based dBFS approximation, not true LUFS metering.
Stated explicitly rather than presented as precise — a proper loudness standard
(e.g. ITU-R BS.1770) needs frequency weighting this doesn't attempt.

Note: browsers record via MediaRecorder as audio/webm (Opus codec) by default,
which soundfile/libsndfile cannot read directly (it only supports WAV/FLAC/OGG/
AIFF-style containers). Every uploaded file is converted to WAV via ffmpeg
before extraction runs, regardless of the original format — this makes the
pipeline format-agnostic rather than assuming a specific upload type.
"""

import os
import subprocess
import uuid

import numpy as np
import soundfile as sf
import librosa


def convert_to_wav(input_path: str) -> str:
    """Converts any ffmpeg-readable audio file to a mono 44.1kHz WAV, returns
    the path to the converted temp file. Caller is responsible for cleaning
    it up after extraction."""
    output_path = os.path.join(
        os.path.dirname(input_path), f"_tmp_{uuid.uuid4().hex}.wav"
    )
    result = subprocess.run(
        ["ffmpeg", "-y", "-i", input_path, "-ar", "44100", "-ac", "1", output_path],
        capture_output=True, text=True
    )
    if result.returncode != 0 or not os.path.exists(output_path):
        raise RuntimeError(f"ffmpeg conversion failed: {result.stderr[-500:]}")
    return output_path


def extract_audio_props(filepath: str) -> dict:
    wav_path = convert_to_wav(filepath)
    try:
        data, sample_rate = sf.read(wav_path)

        if len(data) == 0:
            raise ValueError(f"Audio file is empty or unreadable: {filepath}")

        duration = len(data) / sample_rate
        # bitrate approximated from the ORIGINAL file's size (what the user
        # actually uploaded), not the converted WAV — a WAV's size doesn't
        # reflect the source recording's real bitrate at all
        file_size_bytes = os.path.getsize(filepath)
        bitrate = int((file_size_bytes * 8) / duration) if duration > 0 else 0

        mono = data if data.ndim == 1 else data.mean(axis=1)
        rms = librosa.feature.rms(y=mono.astype(float))
        loudness_db = 20 * np.log10(np.mean(rms) + 1e-9)

        return {
            "duration_sec": round(float(duration), 2),
            "sample_rate": int(sample_rate),
            "bitrate": bitrate,
            "loudness_db": round(float(loudness_db), 2),
        }
    finally:
        if os.path.exists(wav_path):
            os.remove(wav_path)  # temp conversion artifact, not the file we store/serve


def estimate_quality(props: dict) -> str:
    """Bonus: a rough, explicitly-labeled noise/quality heuristic — not a real
    audio quality metric, just a sanity signal based on sample rate + loudness."""
    if props["sample_rate"] < 16000:
        return "low (sample rate below typical voice-quality threshold)"
    if props["loudness_db"] < -40:
        return "low (very quiet recording, possible mic/environment issue)"
    if props["loudness_db"] > -3:
        return "low (clipping risk, recording may be too loud)"
    return "acceptable"