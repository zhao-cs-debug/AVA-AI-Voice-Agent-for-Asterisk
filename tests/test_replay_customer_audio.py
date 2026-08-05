import wave

import pytest

from scripts.replay_customer_audio import read_audio_clip


def _write_wav(path, *, sample_rate=8000, channels=1, sample_width=2, frames=b""):
    with wave.open(str(path), "wb") as wav_file:
        wav_file.setnchannels(channels)
        wav_file.setsampwidth(sample_width)
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(frames)


def test_read_audio_clip_preserves_pcm16_and_applies_time_range(tmp_path):
    path = tmp_path / "customer.wav"
    frames = b"".join(index.to_bytes(2, "little", signed=True) for index in range(8000))
    _write_wav(path, frames=frames)

    clip = read_audio_clip(path, start_sec=0.25, end_sec=0.75)

    assert clip.sample_rate == 8000
    assert clip.start_sec == 0.25
    assert clip.end_sec == 0.75
    assert clip.duration_sec == 0.5
    assert clip.pcm16 == frames[4000:12000]


@pytest.mark.parametrize(
    ("channels", "sample_width", "sample_rate"),
    [(2, 2, 8000), (1, 1, 8000), (1, 2, 44100)],
)
def test_read_audio_clip_rejects_unsupported_wav_formats(
    tmp_path,
    channels,
    sample_width,
    sample_rate,
):
    path = tmp_path / "unsupported.wav"
    _write_wav(
        path,
        channels=channels,
        sample_width=sample_width,
        sample_rate=sample_rate,
        frames=b"\x00" * 320,
    )

    with pytest.raises(ValueError):
        read_audio_clip(path)
