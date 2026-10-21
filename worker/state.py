"""Single-writer SQLite batch journal; acknowledgements precede Kafka commits."""
import fcntl
import json
import os
import sqlite3
from pathlib import Path


class Journal:
    def __init__(self, root):
        self.root=Path(root)
        self.root.mkdir(parents=True,mode=0o700,exist_ok=True)
        self.lock=(self.root/'worker.lock').open('a')
        fcntl.flock(self.lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        self.db=sqlite3.connect(self.root/'checkpoint.sqlite3')
        os.chmod(self.root/'checkpoint.sqlite3',0o600)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.execute('PRAGMA synchronous=FULL')
        self.db.execute('PRAGMA secure_delete=ON')
        self.db.executescript('''
CREATE TABLE IF NOT EXISTS batches (
 job_id TEXT PRIMARY KEY, payload BLOB NOT NULL, row_count INTEGER NOT NULL,
 next_offsets TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS counters (day TEXT PRIMARY KEY, load_attempts INTEGER NOT NULL);
''')
        self.db.commit()

    def pending(self):
        row=self.db.execute('SELECT job_id,payload,row_count,next_offsets,acknowledged FROM batches ORDER BY created_at,job_id LIMIT 1').fetchone()
        return dict(zip(('job_id','payload','row_count','next_offsets','acknowledged'),row)) if row else None

    def prepare(self,job_id,payload,row_count,next_offsets,max_bytes):
        if self.pending() is not None:
            raise RuntimeError('pending_batch_must_be_recovered_first')
        if len(payload)>max_bytes:
            raise RuntimeError('spool_limit_reached')
        # Payload and offset frontier commit atomically in the same durable DB.
        with self.db:
            self.db.execute('INSERT INTO batches(job_id,payload,row_count,next_offsets) VALUES (?,?,?,?)',
                            (job_id,payload,row_count,json.dumps(next_offsets,sort_keys=True)))
        return self.pending()

    def acknowledge(self,job_id):
        with self.db:
            self.db.execute('UPDATE batches SET acknowledged=1 WHERE job_id=?',(job_id,))

    def finish(self,job_id):
        row=self.pending()
        if row is None or row['job_id']!=job_id or not row['acknowledged']:
            raise RuntimeError('cannot_finish_unacknowledged_batch')
        with self.db:
            self.db.execute('DELETE FROM batches WHERE job_id=?',(job_id,))
        self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)')

    def reserve_load_attempt(self,day,maximum):
        with self.db:
            row=self.db.execute('SELECT load_attempts FROM counters WHERE day=?',(day,)).fetchone()
            used=row[0] if row else 0
            if used>=maximum:
                raise RuntimeError('daily_load_job_guardrail')
            self.db.execute('INSERT INTO counters VALUES (?,1) ON CONFLICT(day) DO UPDATE SET load_attempts=load_attempts+1',(day,))

    def close(self):
        self.db.close()
        self.lock.close()
