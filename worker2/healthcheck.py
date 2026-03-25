"""Healthcheck: verify worker can reach Zeebe gRPC."""
import sys

import grpc


def check() -> bool:
    target = "orchestration:26500"
    try:
        channel = grpc.insecure_channel(target)
        # Use gRPC channel connectivity check with short timeout
        future = grpc.channel_ready_future(channel)
        future.result(timeout=5)
        channel.close()
        return True
    except Exception:
        return False


if __name__ == "__main__":
    sys.exit(0 if check() else 1)
