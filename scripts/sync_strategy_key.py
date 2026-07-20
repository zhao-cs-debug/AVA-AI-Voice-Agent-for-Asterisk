from pathlib import Path
import os


SOURCE = Path("/root/cenaniVoice-dev/.env")
TARGET = Path("/root/AVA-AI-Voice-Agent-for-Asterisk/.env")
PREFIX = "STRATEGY_NETWORK_API_KEY="


def read_key() -> str:
    for line in SOURCE.read_text(encoding="utf-8").splitlines():
        if line.startswith(PREFIX):
            return line[len(PREFIX):].strip()
    return ""


def main() -> None:
    key = read_key()
    if not key:
        raise SystemExit("strategy key is not configured in source environment")
    lines = TARGET.read_text(encoding="utf-8").splitlines() if TARGET.exists() else []
    updated = False
    output = []
    for line in lines:
        if line.startswith(PREFIX):
            output.append(PREFIX + key)
            updated = True
        else:
            output.append(line)
    if not updated:
        output.append(PREFIX + key)
    temporary = TARGET.with_name(".env.strategy-key.tmp")
    temporary.write_text("\n".join(output) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, TARGET)
    print("strategy_key_configured")


if __name__ == "__main__":
    main()
