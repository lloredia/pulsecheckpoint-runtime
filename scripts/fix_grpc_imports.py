#!/usr/bin/env python3
"""Make grpc_tools output importable as part of the pulse package."""

from pathlib import Path

PATH = Path("sdk/pulse/generated/pulse_pb2_grpc.py")


def main() -> None:
    text = PATH.read_text()
    old = "import pulse_pb2 as pulse__pb2"
    new = "from pulse.generated import pulse_pb2 as pulse__pb2"
    if old not in text and new not in text:
        raise SystemExit(f"unexpected grpc import in {PATH}")
    PATH.write_text(text.replace(old, new))


if __name__ == "__main__":
    main()
