"""
backup.py
─────────
R2 backup / restore helpers.  Imported by migrate.py (upload after publish)
and app.py (restore at startup + status in /api/debug).

Credentials (all required for R2 to be active):
    R2_ENDPOINT           https://<account_id>.r2.cloudflarestorage.com
    R2_ACCESS_KEY_ID      Cloudflare R2 access key
    R2_SECRET_ACCESS_KEY  Cloudflare R2 secret
    R2_BUCKET             bucket name

If any credential is missing, all operations are no-ops that return
{ok:False, error:'R2 not configured'}.  Never raises — caller always
gets a dict back.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Module-level status updated after each backup / restore operation.
# Exposed via get_status() → included in /api/debug response.
_status: dict[str, Any] = {
    'last_backup_at':  None,
    'last_backup_ok':  None,
    'last_restore_at': None,
    'last_restore_ok': None,
    'backup_error':    None,
}


def _client_and_bucket():
    """Return (boto3_s3_client, bucket_name) or (None, '') if not configured."""
    endpoint   = os.environ.get('R2_ENDPOINT', '').strip()
    access_key = os.environ.get('R2_ACCESS_KEY_ID', '').strip()
    secret_key = os.environ.get('R2_SECRET_ACCESS_KEY', '').strip()
    bucket     = os.environ.get('R2_BUCKET', '').strip()
    if not all([endpoint, access_key, secret_key, bucket]):
        return None, bucket
    try:
        import boto3
        from botocore.config import Config as _BotoConfig
        client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='auto',
            config=_BotoConfig(connect_timeout=10, read_timeout=30,
                               retries={'max_attempts': 1}),
        )
        return client, bucket
    except ImportError:
        return None, bucket


def backup(db_path: Path, pdfs_dir: Path) -> dict:
    """
    Upload hidroviadata.db + all *.pdf files to R2.

    Layout in bucket:
        backups/<timestamp>/hidroviadata.db
        backups/<timestamp>/pdfs/<filename>.pdf
        latest.json   → {"timestamp": "...", "prefix": "backups/<ts>"}

    Returns {ok, timestamp, prefix, n_pdfs} on success.
    Returns {ok:False, error:"..."} on failure.
    Never raises.
    """
    client, bucket = _client_and_bucket()
    if client is None:
        err = 'R2 not configured (missing env vars or boto3 not installed)'
        _status.update({'last_backup_at': _now(), 'last_backup_ok': False,
                        'backup_error': err})
        return {'ok': False, 'error': err}

    ts     = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    prefix = f'backups/{ts}'
    try:
        # 1. DB
        client.upload_file(str(db_path), bucket, f'{prefix}/hidroviadata.db')

        # 2. PDFs
        n_pdfs = 0
        if pdfs_dir.is_dir():
            for pdf in sorted(pdfs_dir.glob('*.pdf')):
                client.upload_file(str(pdf), bucket,
                                   f'{prefix}/pdfs/{pdf.name}')
                n_pdfs += 1

        # 3. latest.json pointer (overwrites previous)
        client.put_object(
            Bucket=bucket,
            Key='latest.json',
            Body=json.dumps({'timestamp': ts, 'prefix': prefix}).encode(),
            ContentType='application/json',
        )

        _status.update({'last_backup_at': _now(), 'last_backup_ok': True,
                        'backup_error': None})
        print(f'[backup] uploaded ok  prefix={prefix}  db+{n_pdfs} pdfs',
              flush=True)
        return {'ok': True, 'timestamp': ts, 'prefix': prefix,
                'n_pdfs': n_pdfs}

    except Exception as exc:
        err = str(exc)
        _status.update({'last_backup_at': _now(), 'last_backup_ok': False,
                        'backup_error': err})
        print(f'[backup] upload FAILED: {err}', flush=True)
        return {'ok': False, 'error': err}


def restore(db_path: Path, pdfs_dir: Path) -> dict:
    """
    Download the latest backup from R2 and restore db + pdfs/.

    Returns {ok, timestamp, prefix, n_pdfs} on success.
    Returns {ok:False, error:"..."} on failure.
    Never raises.
    """
    client, bucket = _client_and_bucket()
    if client is None:
        err = 'R2 not configured (missing env vars or boto3 not installed)'
        _status.update({'last_restore_at': _now(), 'last_restore_ok': False,
                        'backup_error': err})
        return {'ok': False, 'error': err}

    try:
        # 1. Read pointer
        resp    = client.get_object(Bucket=bucket, Key='latest.json')
        pointer = json.loads(resp['Body'].read())
        prefix  = pointer['prefix']
        ts      = pointer['timestamp']

        # 2. Restore DB
        db_path.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(bucket, f'{prefix}/hidroviadata.db', str(db_path))

        # 3. Restore PDFs
        pdfs_dir.mkdir(parents=True, exist_ok=True)
        paginator = client.get_paginator('list_objects_v2')
        n_pdfs    = 0
        for page in paginator.paginate(Bucket=bucket,
                                        Prefix=f'{prefix}/pdfs/'):
            for obj in page.get('Contents', []):
                key      = obj['Key']
                filename = key.split('/')[-1]
                if filename:
                    client.download_file(bucket, key,
                                         str(pdfs_dir / filename))
                    n_pdfs += 1

        _status.update({'last_restore_at': _now(), 'last_restore_ok': True,
                        'backup_error': None})
        print(f'[restore] restored from backup  prefix={prefix}  '
              f'db + {n_pdfs} pdfs', flush=True)
        return {'ok': True, 'timestamp': ts, 'prefix': prefix,
                'n_pdfs': n_pdfs}

    except Exception as exc:
        err = str(exc)
        _status.update({'last_restore_at': _now(), 'last_restore_ok': False,
                        'backup_error': err})
        print(f'[restore] FAILED: {err}', flush=True)
        return {'ok': False, 'error': err}


def get_status() -> dict:
    """Return a safe copy of the current backup/restore status."""
    return dict(_status)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')
