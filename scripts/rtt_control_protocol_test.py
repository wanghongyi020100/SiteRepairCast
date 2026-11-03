#!/usr/bin/env python3
import argparse
import hashlib
import socket
import struct
import time


MAGIC = 0x53524343
VERSION = 11
REGISTER = 1
SECTION_STATUS = 2
RECEIVER_COMPLETE = 4
COMPLETE_ACK = 5
TRANSFER_RESULT = 6
SECTION_END = 7
RECEIVER_READY = 8
SESSION_END = 9
FILE_META = 10
META_READY = 11
SECTION_END_ACK = 22


def header(message_type):
    return struct.pack("!IHH", MAGIC, VERSION, message_type)


def send_frame(sock, payload):
    sock.sendall(struct.pack("!I", len(payload)) + payload)


def receive_exact(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise RuntimeError("control connection closed")
        data.extend(chunk)
    return bytes(data)


def receive_frame(sock):
    (size,) = struct.unpack("!I", receive_exact(sock, 4))
    if size == 0 or size > 1024 * 1024:
        raise RuntimeError(f"invalid control frame size: {size}")
    return receive_exact(sock, size)


def decode_header(frame):
    if len(frame) < 8:
        raise RuntimeError("short control frame")
    magic, version, message_type = struct.unpack("!IHH", frame[:8])
    if magic != MAGIC or version != VERSION:
        raise RuntimeError("unexpected control header")
    return message_type


def connect_receiver(host, port, peer_udp_port, state, transfer_id):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(3)
    sock.bind(("127.0.0.2", 0))
    sock.connect((host, port))
    register = header(REGISTER) + struct.pack(
        "!QHHH", transfer_id, 51000, peer_udp_port, state
    )
    send_frame(sock, register)
    return sock


def decode_file_meta(frame):
    if decode_header(frame) != FILE_META:
        raise RuntimeError("expected FILE_META")
    if len(frame) < 74:
        raise RuntimeError("short FILE_META")
    transfer_id = struct.unpack_from("!Q", frame, 8)[0]
    file_size = struct.unpack_from("!Q", frame, 20)[0]
    total_blocks = struct.unpack_from("!I", frame, 32)[0]
    section_blocks = struct.unpack_from("!I", frame, 36)[0]
    name_size = struct.unpack_from("!H", frame, 40)[0]
    digest_offset = 42 + name_size
    if len(frame) != digest_offset + 32:
        raise RuntimeError("invalid FILE_META size")
    return transfer_id, file_size, total_blocks, section_blocks, frame[digest_offset:]


def send_meta_ready(sock, transfer_id):
    send_frame(sock, header(META_READY) + struct.pack("!Q", transfer_id))


def send_section_status(sock, transfer_id, section_id, round_id):
    payload = header(SECTION_STATUS) + struct.pack(
        "!QIIHHII", transfer_id, section_id, round_id, 2, 0, 0, 0
    )
    send_frame(sock, payload)


def send_complete(sock, transfer_id, file_size, digest):
    send_frame(
        sock,
        header(RECEIVER_COMPLETE) + struct.pack("!QQ", transfer_id, file_size) + digest,
    )


def send_ready(sock, transfer_id):
    send_frame(sock, header(RECEIVER_READY) + struct.pack("!Q", transfer_id))


def run_file(sock, first_ack_delay=0.0):
    meta = receive_frame(sock)
    transfer_id, file_size, total_blocks, section_blocks, digest = decode_file_meta(meta)
    send_meta_ready(sock, transfer_id)
    section_count = (total_blocks + section_blocks - 1) // section_blocks
    waits = []

    for expected_section in range(section_count):
        frame = receive_frame(sock)
        if decode_header(frame) != SECTION_END or len(frame) != 32:
            raise RuntimeError("expected 32-byte SECTION_END")
        current_transfer, section_id, round_id, received_blocks, drain_wait_ms = struct.unpack(
            "!QIIII", frame[8:]
        )
        if (
            current_transfer != transfer_id
            or section_id != expected_section
            or round_id != 0
            or received_blocks != total_blocks
        ):
            raise RuntimeError("unexpected SECTION_END fields")
        if not 10 <= drain_wait_ms <= 200:
            raise RuntimeError(f"drain wait outside bounds: {drain_wait_ms}")
        waits.append(drain_wait_ms)

        stale_ack = header(SECTION_END_ACK) + struct.pack(
            "!QII", transfer_id, section_id + 1, round_id
        )
        send_frame(sock, stale_ack)
        if expected_section == 0 and first_ack_delay:
            time.sleep(first_ack_delay)
        valid_ack = header(SECTION_END_ACK) + struct.pack(
            "!QII", transfer_id, section_id, round_id
        )
        send_frame(sock, valid_ack)

        if expected_section == 0:
            sock.settimeout(0.05)
            try:
                unexpected = receive_frame(sock)
            except socket.timeout:
                unexpected = None
            finally:
                sock.settimeout(3)
            if unexpected is not None:
                raise RuntimeError(
                    "SECTION_END_ACK advanced the round before SECTION_STATUS"
                )

        send_section_status(sock, transfer_id, section_id, round_id)
        if expected_section + 1 == section_count:
            send_complete(sock, transfer_id, file_size, digest)
            if decode_header(receive_frame(sock)) != COMPLETE_ACK:
                raise RuntimeError("expected COMPLETE_ACK")

    if not waits or waits[0] != 200:
        raise RuntimeError(f"first section did not use 200ms default: {waits}")
    return transfer_id, waits


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peer-udp-port", type=int, default=51001)
    args = parser.parse_args()

    sock = connect_receiver(args.host, args.port, args.peer_udp_port, 1, 0)
    first_transfer, first_waits = run_file(sock, first_ack_delay=0.025)
    if len(first_waits) < 2 or first_waits[1] >= 200:
        raise RuntimeError(f"application RTT did not update the next wait: {first_waits}")
    sock.close()
    time.sleep(0.1)

    deadline = time.monotonic() + 3
    while True:
        try:
            sock = connect_receiver(args.host, args.port, args.peer_udp_port, 1, 0)
            break
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.02)

    sock.settimeout(12)
    second_transfer, second_waits = run_file(sock, first_ack_delay=0.25)
    if len(second_waits) < 2 or second_waits[1] != 200:
        raise RuntimeError(f"200ms upper bound was not applied: {second_waits}")
    if second_transfer == first_transfer:
        raise RuntimeError("transfer id was reused")
    send_ready(sock, second_transfer)
    if decode_header(receive_frame(sock)) != SESSION_END:
        raise RuntimeError("expected SESSION_END")
    sock.close()


if __name__ == "__main__":
    main()
