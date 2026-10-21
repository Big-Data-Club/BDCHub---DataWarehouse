#!/usr/bin/env python3
"""Transfer an existing private capture through nested interactive SSH.

Passwords are entered at SSH prompts, never through command arguments or files.
Remote source bytes go into a local private file and are never printed. This path
supports gateways that permit nested SSH but do not support direct TCP forwarding.
"""
import argparse
import base64
import hashlib
import json
import os
import select
import shlex
import subprocess
import sys
import time
from pathlib import Path

BEGIN = b'BDC_PRIVATE_CAPTURE_BEGIN_20261021'
END = b'BDC_PRIVATE_CAPTURE_END_20261021'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gateway', required=True)
    parser.add_argument('--target', required=True)
    parser.add_argument('--remote-file', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.gateway.startswith('-') or args.target.startswith('-'):
        parser.error('SSH destination cannot start with an option.')
    if not args.remote_file.startswith('/tmp/bdc-warehouse-capture-') or not args.remote_file.endswith('/batch.ndjson'):
        parser.error('Only the known temporary warehouse capture path may be transferred.')
    if args.output.exists():
        parser.error('Destination already exists; retain and inspect it rather than overwriting.')
    args.output.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    remote_code = (
        'import base64,sys; from pathlib import Path; '
        f'data=Path({args.remote_file!r}).read_bytes(); '
        "assert len(data)<=16777216; "
        f'print({BEGIN.decode()!r},flush=True); '
        'sys.stdout.write(base64.encodebytes(data).decode()); '
        f'print({END.decode()!r},flush=True)'
    )
    inner = shlex.join(['ssh', '-tt', '-o', 'StrictHostKeyChecking=accept-new',
                        '-o', 'ConnectTimeout=60', args.target,
                        shlex.join(['python3', '-c', remote_code])])
    proc = subprocess.Popen([
        'ssh', '-tt', '-o', 'StrictHostKeyChecking=accept-new',
        '-o', 'ConnectTimeout=60', args.gateway, inner,
    ], stdout=subprocess.PIPE)
    buffer = bytearray()
    deadline = time.monotonic() + 240
    prompted = False
    try:
        while time.monotonic() < deadline:
            ready, _, _ = select.select([proc.stdout], [], [], 1)
            if not ready:
                if proc.poll() is not None:
                    break
                continue
            data = os.read(proc.stdout.fileno(), 16384)
            if not data:
                break
            buffer.extend(data)
            if len(buffer) > 24 * 1024 * 1024:
                raise RuntimeError('Transfer exceeds the bounded capture size.')
            if BEGIN not in buffer and b'password:' in bytes(buffer).lower() and not prompted:
                print('Second-hop SSH password:', flush=True)
                prompted = True
            if END in buffer:
                break
        if BEGIN not in buffer or END not in buffer:
            raise RuntimeError('SSH transfer was not acknowledged; no destination file created.')
        encoded = bytes(buffer).split(BEGIN, 1)[1].split(END, 1)[0]
        payload = base64.b64decode(b''.join(encoded.split()), validate=True)
        fd = os.open(args.output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as handle:
            handle.write(payload)
        print(json.dumps({'private_capture_transferred': True, 'bytes': len(payload),
                          'sha256': hashlib.sha256(payload).hexdigest()}), flush=True)
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError) as exc:
        print(f'capture_transfer: {exc}', file=sys.stderr)
        raise SystemExit(1)
