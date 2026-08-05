import wave

from src.utils.audio_capture import CustomerAudioCaptureManager


def test_customer_capture_is_disabled_without_explicit_opt_in(tmp_path):
    capture = CustomerAudioCaptureManager(
        enabled=False,
        caller_allowlist={"701"},
        base_dir=str(tmp_path),
    )

    capture.append_pcm16(
        call_id="call-disabled",
        caller_number="701",
        called_number="unknown",
        pcm16=b"\x01\x00" * 80,
        sample_rate=8000,
    )
    capture.close_call("call-disabled")

    assert not (tmp_path / "call-disabled" / "customer.wav").exists()


def test_customer_capture_only_keeps_allowlisted_caller_audio(tmp_path):
    capture = CustomerAudioCaptureManager(
        enabled=True,
        caller_allowlist={"701"},
        base_dir=str(tmp_path),
    )
    pcm16 = b"\x01\x00" * 160

    capture.append_pcm16(
        call_id="call-other",
        caller_number="702",
        called_number="unknown",
        pcm16=pcm16,
        sample_rate=8000,
    )
    capture.append_pcm16(
        call_id="call-701",
        caller_number="701",
        called_number="unknown",
        pcm16=pcm16,
        sample_rate=8000,
    )
    capture.close_call("call-other")
    capture.close_call("call-701")

    assert not (tmp_path / "call-other" / "customer.wav").exists()
    output = tmp_path / "call-701" / "customer.wav"
    assert output.exists()
    with wave.open(str(output), "rb") as wav_file:
        assert wav_file.getnchannels() == 1
        assert wav_file.getsampwidth() == 2
        assert wav_file.getframerate() == 8000
        assert wav_file.readframes(wav_file.getnframes()) == pcm16


def test_customer_capture_accepts_allowlisted_called_number(tmp_path):
    capture = CustomerAudioCaptureManager(
        enabled=True,
        caller_allowlist={"701"},
        base_dir=str(tmp_path),
    )

    capture.append_pcm16(
        call_id="call-to-701",
        caller_number="anonymous",
        called_number="701",
        pcm16=b"\x02\x00" * 80,
        sample_rate=16000,
    )
    capture.close_call("call-to-701")

    assert (tmp_path / "call-to-701" / "customer.wav").exists()


def test_customer_capture_from_environment_requires_nonempty_allowlist(tmp_path):
    capture = CustomerAudioCaptureManager.from_environment(
        {
            "VOICEAI_CUSTOMER_CAPTURE_ENABLED": "true",
            "VOICEAI_CUSTOMER_CAPTURE_CALLERS": "  ",
            "VOICEAI_CUSTOMER_CAPTURE_DIR": str(tmp_path),
        }
    )

    assert capture.enabled is False
    assert capture.caller_allowlist == frozenset()
