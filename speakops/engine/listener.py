import io
import os
import wave
import tempfile
import collections
import numpy as np
import pyaudio
import webrtcvad
from faster_whisper import WhisperModel
from typing import Optional

# ═══════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════
RATE          = 16000   # Whisper requires 16kHz
CHUNK         = 320     # 20ms chunks — webrtcvad requirement
CHANNELS      = 1
FORMAT        = pyaudio.paInt16
VAD_MODE      = 3       # 0-3, 3 = most aggressive noise filtering
SILENCE_LIMIT = 1.5     # seconds of silence before stopping
MAX_DURATION  = 10      # max recording seconds

DEVOPS_PROMPT = (
    "DevOps voice commands: scale, deploy, rollback, restart, diagnose, "
    "kubernetes, speakops, pods, replicas, namespace, helm, argocd, "
    "jenkins, grafana, show, status, logs, events, broken, down, "
    "pipeline, trigger, sync, monitoring, dashboard, what, is"
)

# ═══════════════════════════════════════════════════════
# MODEL — load once, reuse
# ═══════════════════════════════════════════════════════
_model: Optional[WhisperModel] = None

def get_model() -> WhisperModel:
    global _model
    if _model is None:
        _model = WhisperModel(
            "small",
            device="cpu",
            compute_type="int8",    # fastest on CPU, low memory
        )
    return _model


# ═══════════════════════════════════════════════════════
# VAD CAPTURE — Smart silence detection
# ═══════════════════════════════════════════════════════
def capture_with_vad() -> Optional[bytes]:
    vad = webrtcvad.Vad(VAD_MODE)
    pa  = pyaudio.PyAudio()

    stream = pa.open(
        format            = FORMAT,
        channels          = CHANNELS,
        rate              = RATE,
        input             = True,
        frames_per_buffer = CHUNK,
    )

    # Ring buffer — keeps last N frames for context
    ring_buffer     = collections.deque(maxlen=30)
    voiced_frames   = []
    triggered       = False
    silent_chunks   = 0
    max_chunks      = int(MAX_DURATION * RATE / CHUNK)
    silence_chunks  = int(SILENCE_LIMIT * RATE / CHUNK)
    total_chunks    = 0

    try:
        while total_chunks < max_chunks:
            chunk = stream.read(CHUNK, exception_on_overflow=False)
            total_chunks += 1

            is_speech = False
            try:
                is_speech = vad.is_speech(chunk, RATE)
            except Exception:
                pass

            if not triggered:
                ring_buffer.append((chunk, is_speech))
                num_voiced = len([f for f, s in ring_buffer if s])

                # start recording when 70% of ring buffer is speech
                if num_voiced > 0.7 * ring_buffer.maxlen:
                    triggered = True
                    voiced_frames.extend([f for f, s in ring_buffer])
                    ring_buffer.clear()
            else:
                voiced_frames.append(chunk)
                ring_buffer.append((chunk, is_speech))
                num_unvoiced = len([f for f, s in ring_buffer if not s])

                # stop when 90% of ring buffer is silence
                if num_unvoiced > 0.9 * ring_buffer.maxlen:
                    break

    finally:
        stream.stop_stream()
        stream.close()
        pa.terminate()

    if not voiced_frames:
        return None

    return b"".join(voiced_frames)


# ═══════════════════════════════════════════════════════
# TRANSCRIBE
# ═══════════════════════════════════════════════════════
def transcribe(audio_bytes: bytes) -> Optional[str]:
    if not audio_bytes:
        return None

    # write to temp wav file
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as f:
        tmp_path = f.name
        with wave.open(f.name, "wb") as wf:
            wf.setnchannels(CHANNELS)
            wf.setsampwidth(2)      # paInt16 = 2 bytes
            wf.setframerate(RATE)
            wf.writeframes(audio_bytes)

    try:
        model    = get_model()
        segments, info = model.transcribe(
            tmp_path,
            language        = "en",
            beam_size       = 5,
            initial_prompt  = DEVOPS_PROMPT,
            vad_filter      = True,
            vad_parameters  = dict(
                min_silence_duration_ms = 300,
                speech_pad_ms           = 200,
            ),
            temperature     = [0.0, 0.2, 0.4],
            word_timestamps = False,
            condition_on_previous_text = False,
        )

        text = " ".join(seg.text.strip() for seg in segments).strip()
        return text if text else None

    finally:
        try:
            os.unlink(tmp_path)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════
# MAIN — capture + transcribe in one call
# ═══════════════════════════════════════════════════════
def listen_once() -> Optional[str]:
    audio = capture_with_vad()
    if not audio:
        return None
    return transcribe(audio)


# ═══════════════════════════════════════════════════════
# FALLBACK — text input when mic not available
# ═══════════════════════════════════════════════════════
def text_fallback(prompt: str = "❯ Type command: ") -> str:
    return input(prompt).strip()


# ═══════════════════════════════════════════════════════
# HEALTH CHECK — mic available?
# ═══════════════════════════════════════════════════════
def mic_available() -> bool:
    try:
        pa     = pyaudio.PyAudio()
        count  = pa.get_device_count()
        pa.terminate()
        return count > 0
    except Exception:
        return False


# ═══════════════════════════════════════════════════════
# CLI TEST
# ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n🎙  SpeakOps Voice Pipeline Test\n")

    if not mic_available():
        print("❌  No microphone found")
        exit(1)

    print("✅  Microphone found")
    print("⏳  Loading Whisper model (first time may take 30s)...")

    model = get_model()
    print("✅  Model ready\n")

    print("🎙  Speak a DevOps command (auto-stops on silence)...")
    text = listen_once()

    if text:
        print(f'\n✅  Heard: "{text}"')
    else:
        print("\n❌  Nothing heard — check microphone")