import streamlit as st
import pandas as pd
import os
import zipfile
import tempfile
import time
import subprocess
import sys
from datetime import datetime
from main import Business, BusinessList, extract_coordinates_from_url
import threading
import queue
import json
import io
import sqlite3
import ast
import re
from typing import Optional
import streamlit.components.v1 as components

# Configure Streamlit page
st.set_page_config(
    page_title="Google Maps Data Scraper",
    page_icon="🗺️",
    layout="wide"
)

# Initialize session state
if 'scraping_progress' not in st.session_state:
    st.session_state.scraping_progress = 0
if 'scraping_status' not in st.session_state:
    st.session_state.scraping_status = "idle"
if 'scraped_data' not in st.session_state:
    st.session_state.scraped_data = None
if 'search_history' not in st.session_state:
    st.session_state.search_history = []
if 'tag_stats' not in st.session_state:
    st.session_state.tag_stats = {}
if 'scrape_manager' not in st.session_state:
    st.session_state.scrape_manager = None
if 'saved_export_excel_bytes' not in st.session_state:
    st.session_state.saved_export_excel_bytes = None


DB_PATH = os.path.join(os.getcwd(), "scraper_data.sqlite")


def _db_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db():
    conn = _db_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS businesses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dedupe_key TEXT NOT NULL UNIQUE,
                tag TEXT,
                batch_name TEXT,
                name TEXT,
                address TEXT,
                website TEXT,
                phone_number TEXT,
                reviews_count INTEGER,
                reviews_average REAL,
                latitude REAL,
                longitude REAL,
                created_at TEXT NOT NULL
            )
            """
        )

        # Migration: Add batch_name column if it doesn't exist
        try:
            conn.execute("SELECT batch_name FROM businesses LIMIT 1")
        except Exception:
            conn.execute("ALTER TABLE businesses ADD COLUMN batch_name TEXT")
            conn.commit()

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS keyword_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_name TEXT NOT NULL UNIQUE,
                file_name TEXT,
                total_keywords INTEGER,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS keywords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_name TEXT NOT NULL,
                keyword TEXT NOT NULL,
                status TEXT,
                assigned_worker TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT,
                FOREIGN KEY(batch_name) REFERENCES keyword_batches(batch_name)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_keywords_batch_status ON keywords(batch_name, status)")
        conn.commit()
    finally:
        conn.close()


def create_keyword_batch(batch_name: str, file_name: str, keywords: list[str]) -> None:
    conn = _db_conn()
    try:
        now = datetime.now().isoformat(timespec="seconds")
        conn.execute(
            """
            INSERT INTO keyword_batches (batch_name, file_name, total_keywords, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (batch_name, file_name or "", int(len(keywords)), now),
        )

        rows = [(batch_name, kw, None, None, now, None) for kw in keywords]
        conn.executemany(
            """
            INSERT INTO keywords (batch_name, keyword, status, assigned_worker, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    finally:
        conn.close()


def list_keyword_batches() -> pd.DataFrame:
    conn = _db_conn()
    try:
        return pd.read_sql_query(
            """
            SELECT batch_name, file_name, total_keywords, created_at
            FROM keyword_batches
            ORDER BY created_at DESC
            """,
            conn,
        )
    finally:
        conn.close()


def fetch_keywords_preview(batch_name: str, limit: int = 50) -> pd.DataFrame:
    conn = _db_conn()
    try:
        return pd.read_sql_query(
            """
            SELECT keyword, status, assigned_worker, created_at, updated_at
            FROM keywords
            WHERE batch_name = ?
            ORDER BY 
                CASE 
                    WHEN status = 'in_progress' THEN 0
                    WHEN status = 'done' THEN 1
                    ELSE 2
                END,
                updated_at DESC NULLS LAST,
                id ASC
            LIMIT ?
            """,
            conn,
            params=(batch_name, int(limit)),
        )
    finally:
        conn.close()


def fetch_all_keywords(batch_name: str) -> list[str]:
    conn = _db_conn()
    try:
        rows = conn.execute(
            """
            SELECT keyword
            FROM keywords
            WHERE batch_name = ?
            ORDER BY id ASC
            """,
            (batch_name,),
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        conn.close()


def delete_keyword_batch(batch_name: str) -> None:
    conn = _db_conn()
    try:
        conn.execute("DELETE FROM keywords WHERE batch_name = ?", (batch_name,))
        conn.execute("DELETE FROM keyword_batches WHERE batch_name = ?", (batch_name,))
        conn.commit()
    finally:
        conn.close()


def lock_next_keyword(batch_name: str, worker_id: str) -> Optional[tuple[int, str]]:
    conn = _db_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT id, keyword
            FROM keywords
            WHERE batch_name = ?
              AND (status IS NULL OR status = '' OR status = 'undone')
            ORDER BY id ASC
            LIMIT 1
            """,
            (batch_name,),
        ).fetchone()

        if not row:
            conn.execute("COMMIT")
            return None

        kid, kw = int(row[0]), str(row[1])
        now = datetime.now().isoformat(timespec="seconds")
        conn.execute(
            """
            UPDATE keywords
            SET status = 'in_progress', assigned_worker = ?, updated_at = ?
            WHERE id = ?
            """,
            (str(worker_id), now, kid),
        )
        conn.execute("COMMIT")
        return kid, kw
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()


def mark_keyword_done(keyword_id: int) -> None:
    conn = _db_conn()
    try:
        now = datetime.now().isoformat(timespec="seconds")
        conn.execute(
            """
            UPDATE keywords
            SET status = 'done', updated_at = ?
            WHERE id = ?
            """,
            (now, int(keyword_id)),
        )
        conn.commit()
    finally:
        conn.close()


def mark_keyword_undone(keyword_id: int) -> None:
    conn = _db_conn()
    try:
        now = datetime.now().isoformat(timespec="seconds")
        conn.execute(
            """
            UPDATE keywords
            SET status = 'undone', assigned_worker = NULL, updated_at = ?
            WHERE id = ?
            """,
            (now, int(keyword_id)),
        )
        conn.commit()
    finally:
        conn.close()

class ScrapeManager:
    def __init__(self):
        self._lock = threading.Lock()
        self.running = False
        self.mode = None
        self.status = "idle"
        self.error = None
        self.total_scraped = 0
        self.inserted = 0
        self.skipped = 0
        self.mobile_found = 0
        self.progress = 0
        self.current_tag = None
        self.current_batch = None
        self.proc = None
        self.thread = None
        self.worker_procs = []
        self.worker_threads = []
        self.worker_current_keyword = {}
        self.worker_assigned_keywords = {}
        self.completed_keywords = set()
        self.worker_errors = {}

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "running": self.running,
                "mode": self.mode,
                "status": self.status,
                "error": self.error,
                "total_scraped": self.total_scraped,
                "inserted": self.inserted,
                "skipped": self.skipped,
                "mobile_found": self.mobile_found,
                "progress": self.progress,
                "current_tag": self.current_tag,
                "current_batch": self.current_batch,
                "worker_current_keyword": dict(self.worker_current_keyword),
                "worker_assigned_keywords": {k: list(v) for k, v in self.worker_assigned_keywords.items()},
                "completed_keywords": list(self.completed_keywords),
                "worker_errors": dict(self.worker_errors),
            }

    def stop(self):
        with self._lock:
            procs = self.worker_procs.copy()
        for proc in procs:
            if proc and proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except Exception:
                    proc.kill()
        with self._lock:
            self.worker_procs = []
            self.worker_threads = []
            self.worker_current_keyword = {}
            self.worker_assigned_keywords = {}
            self.completed_keywords = set()
            self.worker_errors = {}
            self.running = False
            self.status = "stopped"
            self.current_tag = None
            self.current_batch = None

    def start_campaign(self, batch_name: str, effective_limit: int, worker_count: int = 1):
        # Reset any stuck in_progress keywords from previous crashed runs
        reset_in_progress_to_undone(batch_name)
        
        # Get total keyword count for progress calculation
        status_counts = get_keyword_status_counts(batch_name)
        total_keywords = sum(status_counts.values())
        
        with self._lock:
            if self.running:
                return

            self.running = True
            self.mode = "campaign"
            self.status = "running"
            self.error = None
            self.total_scraped = 0
            self.inserted = 0
            self.skipped = 0
            self.mobile_found = 0
            self.progress = 0
            self.current_tag = None
            self.current_batch = (batch_name or "")
            self.total_keywords = total_keywords  # Store for progress calc
            self.limit_per_keyword = effective_limit
            self.worker_procs = []
            self.worker_threads = []
            self.worker_current_keyword = {}
            self.worker_assigned_keywords = {}
            self.completed_keywords = set()
            self.worker_errors = {}

        def _run_worker(worker_id: int):
            """
            Immortal worker — NEVER exits due to any error.

            Behaviour:
            • On browser crash / non-zero exit / any exception: kill the old
              process, wait a short back-off, then spawn a FRESH browser
              process (--incognito flag passed to Playwright via env var) for
              the SAME keyword.  The keyword is retried up to
              MAX_RETRIES_PER_KEYWORD times.
            • If a keyword exhausts all retries it is marked 'undone' so it
              can be re-run later, an error note is recorded, and the worker
              moves straight on to the NEXT keyword — it does NOT stop.
            • The worker only exits cleanly when there are no more keywords
              left to lock (lock_next_keyword returns None) OR when
              self.running is set to False by an explicit manager.stop() call.
            """
            wid = str(worker_id)
            MAX_RETRIES_PER_KEYWORD = 5   # retries before skipping a keyword
            BASE_BACKOFF = 3              # seconds — doubles each retry, capped at 60s

            while True:
                # ── Respect an explicit stop() ──────────────────────────────
                with self._lock:
                    if not self.running:
                        return

                # ── Grab next keyword atomically ────────────────────────────
                locked = lock_next_keyword(batch_name, worker_id=wid)
                if not locked:
                    # No more keywords — this worker is done
                    with self._lock:
                        if worker_id in self.worker_current_keyword:
                            del self.worker_current_keyword[worker_id]
                    return

                keyword_id, keyword = locked
                with self._lock:
                    self.worker_current_keyword[worker_id] = keyword
                    self.current_tag = keyword
                    # Clear any previous error note for this worker
                    self.worker_errors.pop(worker_id, None)

                keyword_succeeded = False
                for attempt in range(1, MAX_RETRIES_PER_KEYWORD + 1):
                    # ── Respect stop between retries ────────────────────────
                    with self._lock:
                        if not self.running:
                            return

                    proc = None
                    try:
                        current_python = sys.executable
                        cmd = [
                            current_python, "main.py",
                            "-s", keyword,
                            "-t", str(int(effective_limit)),
                            "-b", batch_name,
                        ]
                        env = os.environ.copy()
                        env["PYTHONUNBUFFERED"] = "1"
                        env["PYTHONIOENCODING"] = "utf-8"
                        # Signal main.py to use incognito / fresh browser profile
                        # so a crashed previous instance doesn't leave stale state
                        env["SCRAPER_INCOGNITO"] = "1"

                        proc = subprocess.Popen(
                            cmd,
                            cwd=os.getcwd(),
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            text=True,
                            encoding="utf-8",
                            errors="replace",
                            bufsize=1,
                            env=env,
                        )

                        with self._lock:
                            self.worker_procs.append(proc)

                        # ── Stream stdout and collect STATS lines ───────────
                        for raw_line in proc.stdout:
                            # If the process died mid-stream just drain and break
                            if proc.poll() is not None:
                                break
                            line = (raw_line or "").strip()
                            if not line:
                                continue
                            if line.startswith("STATS:"):
                                try:
                                    stats_str = line.split(":", 1)[1].strip()
                                    stats_d = {}
                                    for part in stats_str.split(", "):
                                        k, v = part.split("=")
                                        stats_d[k] = int(v)
                                    with self._lock:
                                        self.total_scraped += stats_d.get("scraped", 0)
                                        self.inserted += stats_d.get("inserted", 0)
                                        self.skipped += stats_d.get("skipped", 0)
                                        if stats_d.get("mobile"):
                                            self.mobile_found += 1
                                        total_expected = self.total_keywords * self.limit_per_keyword
                                        self.progress = int(
                                            min(99, (self.total_scraped / max(1, total_expected)) * 100)
                                        )
                                except Exception:
                                    pass  # malformed STATS line — ignore, keep going

                        proc.wait()

                        if proc.returncode != 0:
                            stderr_output = ""
                            try:
                                stderr_output = proc.stderr.read(2000)  # cap stderr read
                            except Exception:
                                pass
                            raise RuntimeError(
                                f"exit {proc.returncode}" + (f": {stderr_output[:300]}" if stderr_output else "")
                            )

                        # ── Success ─────────────────────────────────────────
                        mark_keyword_done(keyword_id)
                        with self._lock:
                            self.completed_keywords.add(keyword)
                            self.worker_errors.pop(worker_id, None)
                        keyword_succeeded = True
                        break  # exit retry loop — move to next keyword

                    except Exception as exc:
                        # ── Kill the dead process cleanly ───────────────────
                        if proc is not None:
                            try:
                                if proc.poll() is None:
                                    proc.kill()
                                    proc.wait(timeout=5)
                            except Exception:
                                pass

                        err_msg = f"W{worker_id} keyword '{keyword}' attempt {attempt}/{MAX_RETRIES_PER_KEYWORD}: {exc}"
                        with self._lock:
                            self.worker_errors[worker_id] = err_msg
                            self.error = f"Worker {worker_id} retrying (attempt {attempt})…"

                        if attempt < MAX_RETRIES_PER_KEYWORD:
                            backoff = min(60, BASE_BACKOFF * (2 ** (attempt - 1)))
                            # Honour a stop() during back-off
                            for _ in range(int(backoff * 2)):
                                time.sleep(0.5)
                                with self._lock:
                                    if not self.running:
                                        return
                        # else: loop ends, fall through to skip logic below

                # ── Keyword failed all retries — skip it, keep worker alive ─
                if not keyword_succeeded:
                    mark_keyword_undone(keyword_id)
                    with self._lock:
                        self.worker_errors[worker_id] = (
                            f"Skipped '{keyword}' after {MAX_RETRIES_PER_KEYWORD} failed attempts — moving to next keyword"
                        )
                    # Small pause before taking next keyword
                    time.sleep(2)

                # Clear current keyword display before looping back
                with self._lock:
                    self.worker_current_keyword.pop(worker_id, None)

        for i in range(int(worker_count)):
            t = threading.Thread(target=_run_worker, args=(i,), daemon=True)
            with self._lock:
                self.worker_threads.append(t)
            t.start()

        def _monitor_workers():
            while True:
                with self._lock:
                    if not self.running and self.status in ("stopped", "error"):
                        break
                    all_done = all(t.is_alive() is False for t in self.worker_threads)
                    if all_done:
                        self.running = False
                        self.status = "completed_with_errors" if self.worker_errors else "completed"
                        self.progress = 100
                        self.current_tag = None
                        break
                time.sleep(0.5)

        monitor_thread = threading.Thread(target=_monitor_workers, daemon=True)
        monitor_thread.start()

    def start_multi(self, search_queries: list[str], effective_limit: int, worker_count: int = 1):
        with self._lock:
            if self.running:
                return

        # Distribute keywords evenly among workers
        import math
        keywords_per_worker = math.ceil(len(search_queries) / worker_count)
        worker_keywords = []
        for i in range(worker_count):
            start = i * keywords_per_worker
            end = min((i + 1) * keywords_per_worker, len(search_queries))
            worker_keywords.append(search_queries[start:end])

        with self._lock:
            self.running = True
            self.mode = "multi"
            self.status = "running"
            self.error = None
            self.total_scraped = 0
            self.inserted = 0
            self.skipped = 0
            self.mobile_found = 0
            self.progress = 0
            self.current_tag = None
            self.current_batch = None
            self.worker_procs = []
            self.worker_threads = []
            self.worker_current_keyword = {}
            self.worker_assigned_keywords = {i: list(worker_keywords[i]) for i in range(len(worker_keywords))}
            self.completed_keywords = set()
            self.worker_errors = {}

        def _run_worker(worker_id: int, keywords: list[str]):
            try:
                # Prepare unique input.txt for this worker
                temp_input_file = f"input_worker_{worker_id}.txt"
                with open(temp_input_file, 'w') as f:
                    for query in keywords:
                        if query.strip():
                            f.write(query.strip() + '\n')

                current_python = sys.executable
                cmd = [current_python, "main.py", "-i", temp_input_file, "-t", str(effective_limit)]

                env = os.environ.copy()
                env["PYTHONUNBUFFERED"] = "1"
                env["PYTHONIOENCODING"] = "utf-8"

                proc = subprocess.Popen(
                    cmd,
                    cwd=os.getcwd(),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    bufsize=1,
                    env=env,
                )

                with self._lock:
                    self.worker_procs.append(proc)

                expected_total = effective_limit * len(keywords)

                last_tag = None
                for raw_line in proc.stdout:
                    if proc.poll() is not None:
                        break
                    line = (raw_line or "").strip()
                    if not line:
                        continue

                    if line.startswith("TAG:"):
                        tag = line.split(":", 1)[1].strip()
                        with self._lock:
                            if last_tag:
                                self.completed_keywords.add(last_tag)
                            last_tag = tag
                            self.worker_current_keyword[worker_id] = tag
                            self.current_tag = tag
                    elif line.startswith("STATS:"):
                        stats_str = line.split(":", 1)[1].strip()
                        stats = {}
                        for part in stats_str.split(", "):
                            k, v = part.split("=")
                            stats[k] = int(v)
                        with self._lock:
                            self.total_scraped += stats["scraped"]
                            if stats["inserted"]:
                                self.inserted += 1
                            if stats["skipped"]:
                                self.skipped += 1
                            if stats["mobile"]:
                                self.mobile_found += 1
                            self.progress = int(min(99, (self.total_scraped / max(1, expected_total)) * 100))

                proc.wait()

                with self._lock:
                    if last_tag:
                        self.completed_keywords.add(last_tag)
                    if worker_id in self.worker_current_keyword:
                        del self.worker_current_keyword[worker_id]

                if proc.returncode != 0:
                    stderr_output = proc.stderr.read()
                    raise Exception(f"Worker {worker_id} failed (exit {proc.returncode}) - {stderr_output}")

            except Exception as e:
                try:
                    pass  # temp file cleanup not needed since unique
                except Exception:
                    pass
                with self._lock:
                    self.worker_errors[worker_id] = str(e)
                    self.error = "One or more workers failed. Scraping will continue with remaining workers."

        # Start worker threads
        for i, keywords in enumerate(worker_keywords):
            t = threading.Thread(target=_run_worker, args=(i, keywords), daemon=True)
            with self._lock:
                self.worker_threads.append(t)
            t.start()

        # Wait for all workers to finish
        def _monitor_workers():
            while True:
                with self._lock:
                    if not self.running:
                        break
                    all_done = all(t.is_alive() is False for t in self.worker_threads)
                    if all_done:
                        self.running = False
                        self.status = "completed_with_errors" if self.worker_errors else "completed"
                        self.progress = 100
                        self.current_tag = None
                        break
                time.sleep(0.5)

        monitor_thread = threading.Thread(target=_monitor_workers, daemon=True)
        monitor_thread.start()


def _norm(s: str) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", str(s).strip().lower())


def _phone_key(phone: str) -> str:
    if not phone:
        return ""
    digits = re.sub(r"\D+", "", phone)
    return digits


def make_dedupe_key(business: dict) -> str:
    phone = _phone_key(business.get("phone_number"))
    if not phone:
        return ""
    return f"p:{phone}"


def insert_business(tag: str, batch_name: str, business: dict) -> bool:
    dedupe_key = make_dedupe_key(business)
    if not dedupe_key:
        return False
    conn = _db_conn()
    try:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO businesses (
                dedupe_key, tag, batch_name, name, address, website, phone_number,
                reviews_count, reviews_average, latitude, longitude, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                dedupe_key,
                tag,
                batch_name or "",
                business.get("name") or "",
                business.get("address") or "",
                business.get("website") or "",
                business.get("phone_number") or "",
                int(business.get("reviews_count")) if str(business.get("reviews_count")).isdigit() else None,
                float(business.get("reviews_average")) if _norm(business.get("reviews_average")) else None,
                business.get("latitude"),
                business.get("longitude"),
                datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()
        return cur.rowcount == 1
    finally:
        conn.close()


def fetch_last_rows(limit: int = 25) -> pd.DataFrame:
    conn = _db_conn()
    try:
        return pd.read_sql_query(
            """
            SELECT batch_name, tag, name, phone_number, address, website, reviews_count, reviews_average, latitude, longitude, created_at
            FROM businesses
            ORDER BY id DESC
            LIMIT ?
            """,
            conn,
            params=(limit,),
        )
    finally:
        conn.close()


def reset_in_progress_to_undone(batch_name: str) -> None:
    """Reset keywords that were left in_progress (from crashed workers) back to undone."""
    conn = _db_conn()
    try:
        now = datetime.now().isoformat(timespec="seconds")
        conn.execute(
            """
            UPDATE keywords
            SET status = 'undone', assigned_worker = NULL, updated_at = ?
            WHERE batch_name = ? AND status = 'in_progress'
            """,
            (now, batch_name),
        )
        conn.commit()
    finally:
        conn.close()


def get_keyword_status_counts(batch_name: str) -> dict:
    """Get counts of keywords by status for a batch."""
    conn = _db_conn()
    try:
        rows = conn.execute(
            """
            SELECT 
                COALESCE(status, 'undone') as status,
                COUNT(*) as count
            FROM keywords
            WHERE batch_name = ?
            GROUP BY status
            """,
            (batch_name,),
        ).fetchall()
        return {r[0]: r[1] for r in rows}
    finally:
        conn.close()


def get_batch_stats() -> dict:
    """Get per-batch counts from businesses table."""
    conn = _db_conn()
    try:
        rows = conn.execute(
            """
            SELECT tag, batch_name,
                   COUNT(*) AS scraped,
                   SUM(CASE WHEN phone_number IS NOT NULL AND TRIM(phone_number) <> '' THEN 1 ELSE 0 END) AS mobile_found
            FROM businesses
            GROUP BY tag, batch_name
            """
        ).fetchall()
        # Structure: {batch_name: {tag: {scraped: int, mobile_found: int}}}
        stats = {}
        for r in rows:
            tag, batch, scraped, mobile = r[0] or "", r[1] or "(untagged)", int(r[2] or 0), int(r[3] or 0)
            if batch not in stats:
                stats[batch] = {}
            stats[batch][tag] = {"scraped": scraped, "mobile_found": mobile}
        return stats
    finally:
        conn.close()


def get_batch_summary() -> pd.DataFrame:
    """Get summary per batch (total records, with phone)."""
    conn = _db_conn()
    try:
        return pd.read_sql_query(
            """
            SELECT 
                COALESCE(batch_name, '(untagged)') AS batch_name,
                COUNT(*) AS total_records,
                SUM(CASE WHEN phone_number IS NOT NULL AND TRIM(phone_number) <> '' THEN 1 ELSE 0 END) AS with_phone
            FROM businesses
            GROUP BY batch_name
            ORDER BY total_records DESC
            """,
            conn,
        )
    finally:
        conn.close()


def get_tag_stats() -> dict:
    """Get per-tag statistics from businesses table."""
    conn = _db_conn()
    try:
        rows = conn.execute(
            """
            SELECT tag,
                   COUNT(*) AS scraped,
                   SUM(CASE WHEN phone_number IS NOT NULL AND TRIM(phone_number) <> '' THEN 1 ELSE 0 END) AS mobile_found
            FROM businesses
            GROUP BY tag
            """
        ).fetchall()
        return {r[0] or "": {"scraped": int(r[1] or 0), "mobile_found": int(r[2] or 0)} for r in rows}
    finally:
        conn.close()


def fetch_rows_by_tag(tag: str, limit: int = 50) -> pd.DataFrame:
    conn = _db_conn()
    try:
        return pd.read_sql_query(
            """
            SELECT tag, name, phone_number, address, website, reviews_count, reviews_average, latitude, longitude, created_at
            FROM businesses
            WHERE tag = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            conn,
            params=(tag, limit),
        )
    finally:
        conn.close()


def fetch_all_rows() -> pd.DataFrame:
    conn = _db_conn()
    try:
        return pd.read_sql_query(
            """
            SELECT batch_name, tag, name, phone_number, address, website, reviews_count, reviews_average, latitude, longitude, created_at
            FROM businesses
            ORDER BY id ASC
            """,
            conn,
        )
    finally:
        conn.close()

def clear_all_data() -> bool:
    """Delete all data from the businesses table (reset DB)"""
    conn = _db_conn()
    try:
        conn.execute("DELETE FROM businesses")
        conn.execute("DELETE FROM sqlite_sequence WHERE name='businesses'")
        conn.commit()
        return True
    except Exception as e:
        print(f"Error clearing database: {e}")
        return False
    finally:
        conn.close()


def load_search_history():
    """Load search history from file"""
    history_file = "search_history.json"
    if os.path.exists(history_file):
        try:
            with open(history_file, 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def save_search_history(history):
    """Save search history to file"""
    history_file = "search_history.json"
    with open(history_file, 'w') as f:
        json.dump(history, f, indent=2)

def scrape_single_search(search_query, limit, progress_callback=None, row_callback=None, status_callback=None):
    """Scrape data for a single search query using subprocess"""
    try:
        current_python = sys.executable

        cmd = [current_python, "main.py", "-s", search_query]
        if limit is not None:
            cmd += ["-t", str(limit)]

        if progress_callback:
            progress_callback(0)

        scraped_count = 0
        inserted_count = 0
        mobile_found = 0
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"
        env["PYTHONIOENCODING"] = "utf-8"
        start_time = time.time()
        
        with subprocess.Popen(
            cmd,
            cwd=os.getcwd(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
        ) as proc:
            for raw_line in proc.stdout:
                line = (raw_line or "").strip()
                if not line:
                    continue

                if line.startswith("STATS:"):
                    stats_str = line.split(":", 1)[1].strip()
                    stats = {}
                    for part in stats_str.split(", "):
                        k, v = part.split("=")
                        stats[k] = int(v)
                    scraped_count += stats["scraped"]
                    if stats["inserted"]:
                        inserted_count += 1
                    if stats["mobile"]:
                        mobile_found += 1
                    if row_callback:
                        row_callback()

                    if status_callback:
                        status_callback(
                            {
                                "status": "running",
                                "total_scraped": scraped_count,
                                "inserted": inserted_count,
                                "mobile_found": mobile_found,
                            }
                        )

                    if progress_callback:
                        if limit is not None and int(limit) > 0:
                            pct = int(min(99, (scraped_count / max(1, int(limit))) * 100))
                            progress_callback(pct)

            proc.wait()

            stderr_output = proc.stderr.read()
            if proc.returncode != 0:
                error_msg = f"Scraping failed (exit {proc.returncode})"
                if stderr_output:
                    error_msg += f" - Error: {stderr_output}"
                raise Exception(error_msg)

        if progress_callback:
            progress_callback(100)

        return {"total_scraped": scraped_count, "inserted": inserted_count, "mobile_found": mobile_found}
        
    except Exception as e:
        raise Exception(f"Scraping error: {str(e)}")

def scrape_multiple_searches(search_queries, limit, progress_callback=None, row_callback=None, status_callback=None):
    """Scrape data for multiple search queries using subprocess"""
    try:
        # Create a temporary input file
        temp_input_file = "temp_input.txt"
        with open(temp_input_file, 'w') as f:
            for query in search_queries:
                if query.strip():
                    f.write(query.strip() + '\n')
        
        if progress_callback:
            progress_callback(0)
        expected_total = (limit or 0) * len(search_queries)
        
        # Get the current Python executable (should be from virtual environment)
        current_python = sys.executable
        
        cmd = [current_python, "main.py"]
        if limit is not None:
            cmd += ["-t", str(limit)]
        
        # Temporarily rename input.txt if it exists
        input_backup = None
        if os.path.exists("input.txt"):
            input_backup = "input_backup.txt"
            os.rename("input.txt", input_backup)
        
        # Copy temp file to input.txt
        os.rename(temp_input_file, "input.txt")
        
        try:
            env = os.environ.copy(); env["PYTHONUNBUFFERED"] = "1"; env["PYTHONIOENCODING"] = "utf-8"
            scraped_total = 0
            inserted_total = 0
            mobile_found_total = 0
            start_time = time.time()

            current_tag = None
            expecting_dict = False

            with subprocess.Popen(cmd, cwd=os.getcwd(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1, env=env) as proc:
                for raw_line in proc.stdout:
                    line = (raw_line or "").strip()
                    if not line:
                        continue

                    m = re.match(r"^\d+\s*-\s*(.+)$", line)
                    if m:
                        current_tag = m.group(1).strip()
                        continue

                    if "Scraped Business:" in line:
                        expecting_dict = True
                        continue

                    if expecting_dict and line.startswith("{"):
                        expecting_dict = False
                        scraped_total += 1
                        try:
                            business = ast.literal_eval(line)
                            if isinstance(business, dict):
                                tag = current_tag or ""
                                inserted = insert_business(tag, business)
                                if inserted:
                                    inserted_total += 1
                                    if _phone_key(business.get("phone_number")):
                                        mobile_found_total += 1
                                if row_callback:
                                    row_callback()
                        except Exception:
                            pass

                        if status_callback:
                            status_callback(
                                {
                                    "status": "running",
                                    "total_scraped": scraped_total,
                                    "inserted": inserted_total,
                                    "mobile_found": mobile_found_total,
                                }
                            )

                        if progress_callback:
                            if expected_total > 0:
                                pct = int(min(99, (scraped_total / max(1, expected_total)) * 100))
                                progress_callback(pct)

                proc.wait()

                stderr_output = proc.stderr.read()
                if proc.returncode != 0:
                    error_msg = f"Multi-search scraping failed (exit {proc.returncode})"
                    if stderr_output:
                        error_msg += f" - Error: {stderr_output}"
                    raise Exception(error_msg)
            

            
            if progress_callback:
                progress_callback(100)

            return {"total_scraped": scraped_total, "inserted": inserted_total, "mobile_found": mobile_found_total}
            
        finally:
            # Restore original input.txt if it existed
            if os.path.exists("input.txt"):
                os.remove("input.txt")
            if input_backup:
                os.rename(input_backup, "input.txt")
                
    except Exception as e:
        raise Exception(f"Multi-search error: {str(e)}")

def create_zip_file(results_dict):
    """Create a zip file containing all Excel files"""
    temp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(temp_dir, "google_maps_data.zip")
    
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for search_query, business_list in results_dict.items():
            # Create Excel file
            clean_search = search_query.strip().replace(' ', '_').replace('/', '_')
            filename = f"google_maps_data_{clean_search}"
            excel_path = business_list.save_to_excel(filename)
            
            # Add to zip
            zipf.write(excel_path, f"{filename}.xlsx")
    
    return zip_path

# Main App
st.title("🗺️ Google Maps Data Scraper")
st.markdown("---")

init_db()

if st.session_state.scrape_manager is None:
    st.session_state.scrape_manager = ScrapeManager()

manager: ScrapeManager = st.session_state.scrape_manager

# Create tabs
tab1, tab2, tab3, tab4 = st.tabs(["🔍 Single Search", "📋 Multi Search", "📊 History", "💾 Saved Data"])

col_refresh_1, col_refresh_2 = st.columns([2, 1])
with col_refresh_1:
    auto_refresh = st.checkbox("Auto-refresh stats while scraping", value=False, key="auto_refresh_scrape")
with col_refresh_2:
    refresh_interval = st.select_slider(
        "Refresh (sec)",
        options=[5, 10, 15, 30, 60],
        value=10,
        disabled=not auto_refresh,
        key="auto_refresh_interval",
    )

# Tab 1: Single Search
with tab1:
    st.header("Single Search")
    st.markdown("Enter a search query to scrape Google Maps data")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_query = st.text_input(
            "What and Where",
            placeholder="e.g., Hotels in New York",
            help="Enter your search query (what you're looking for and where)"
        )
    
    with col2:
        no_limit_single = st.checkbox("No limit", value=True, key="no_limit_single")
        limit = st.number_input(
            "Limit",
            min_value=1,
            value=20,
            help="Maximum number of results to scrape",
            disabled=no_limit_single,
        )
    
    st.info("Single Search background mode is not enabled yet. Use Multi Search.")

# Tab 2: Multi Search
with tab2:
    st.header("Multi Search")
    st.markdown("Upload keywords file and manage keyword batches")

    if "campaign_settings_open" not in st.session_state:
        st.session_state.campaign_settings_open = False
    if "campaign_settings_batch" not in st.session_state:
        st.session_state.campaign_settings_batch = ""

    def _parse_keywords_from_upload(file_name: str, data: bytes) -> list[str]:
        if not file_name:
            return []
        lower = file_name.lower()
        if lower.endswith(".txt"):
            content = (data or b"").decode("utf-8", errors="replace")
            kws = [line.strip() for line in content.split("\n") if line.strip()]
            return kws
        if lower.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(data))
        elif lower.endswith(".xlsx") or lower.endswith(".xls"):
            df = pd.read_excel(io.BytesIO(data))
        else:
            return []

        if df is None or df.empty:
            return []

        cols = [str(c).strip().lower() for c in df.columns]
        keyword_col = None
        for candidate in ("keyword", "keywords"):
            if candidate in cols:
                keyword_col = df.columns[cols.index(candidate)]
                break
        if keyword_col is None:
            keyword_col = df.columns[0]

        kws = []
        for v in df[keyword_col].tolist():
            s = ("" if v is None else str(v)).strip()
            if s:
                kws.append(s)
        return kws
    
    uploaded_file = st.file_uploader(
        "Upload keywords file",
        type=["txt", "csv", "xlsx"],
        help="Upload .txt (one keyword per line) or .csv/.xlsx (keyword column)",
        key="keywords_uploader",
    )

    uploaded_keywords: list[str] = []
    uploaded_filename = ""
    uploaded_bytes = b""
    if uploaded_file is not None:
        uploaded_filename = uploaded_file.name or ""
        uploaded_bytes = uploaded_file.getvalue()
        uploaded_keywords = _parse_keywords_from_upload(uploaded_filename, uploaded_bytes)

    if uploaded_file is not None:
        st.info(f"Found {len(uploaded_keywords)} keywords")
        with st.expander("📋 Preview keywords"):
            for i, kw in enumerate(uploaded_keywords[:50], 1):
                st.markdown(f"{i}. {kw}")
            if len(uploaded_keywords) > 50:
                st.caption(f"Showing first 50 of {len(uploaded_keywords)}")

        batch_name = st.text_input(
            "Batch Name (required)",
            value="",
            help="Batch name must be unique",
            key="new_batch_name",
        )

        limit_multi = st.number_input(
            "Limit per keyword",
            min_value=1,
            max_value=500,
            value=20,
            help="Maximum number of results to scrape for each keyword",
            key="campaign_limit",
        )
        worker_count = st.number_input(
            "Number of Workers",
            min_value=1,
            max_value=20,
            value=1,
            help="Workers pull keywords from DB dynamically",
            key="campaign_workers",
        )

        save_col, run_col = st.columns([1, 1])
        with save_col:
            save_only = st.button("Save Batch", type="secondary", key="save_batch_btn")
        with run_col:
            run_now = st.button("Save + Run Campaign Now", type="primary", key="save_run_batch_btn")

        if save_only or run_now:
            bn = (batch_name or "").strip()
            if not bn:
                st.error("Batch Name is required")
            elif not uploaded_keywords:
                st.error("No keywords found in uploaded file")
            else:
                try:
                    create_keyword_batch(bn, uploaded_filename, uploaded_keywords)
                    st.success(f"Saved batch '{bn}' with {len(uploaded_keywords)} keywords")
                    if run_now:
                        manager.start_campaign(bn, int(limit_multi), int(worker_count))
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to save batch: {e}")

    st.markdown("---")
    st.subheader("Batch Management")

    batches_df = list_keyword_batches()
    st.dataframe(batches_df, use_container_width=True)

    batch_names = list(batches_df["batch_name"].tolist()) if not batches_df.empty else []
    selected_batch = st.selectbox(
        "Select batch",
        options=[""] + batch_names,
        index=0,
        key="selected_batch_manage",
    )

    if selected_batch:
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            view_btn = st.button("👁 View", key="view_batch_btn")
        with c2:
            download_btn = st.button("⬇ Download", key="download_batch_btn")
        with c3:
            start_btn = st.button("🚀 Start Campaign", key="start_campaign_btn")
        with c4:
            delete_btn = st.button("🗑 Delete", key="delete_batch_btn")

        if view_btn:
            st.subheader(f"Preview: {selected_batch}")
            st.dataframe(fetch_keywords_preview(selected_batch, limit=50), use_container_width=True)

        if download_btn:
            try:
                row = batches_df[batches_df["batch_name"] == selected_batch].iloc[0].to_dict()
                orig = (row.get("file_name") or "").lower()
                kws = fetch_all_keywords(selected_batch)
                if orig.endswith(".csv"):
                    out_df = pd.DataFrame({"keyword": kws})
                    st.download_button(
                        "Download CSV",
                        data=out_df.to_csv(index=False).encode("utf-8"),
                        file_name=f"{selected_batch}.csv",
                        mime="text/csv",
                        key="download_csv_btn",
                    )
                elif orig.endswith(".xlsx") or orig.endswith(".xls"):
                    out_df = pd.DataFrame({"keyword": kws})
                    buf = io.BytesIO()
                    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                        out_df.to_excel(writer, index=False, sheet_name="keywords")
                    st.download_button(
                        "Download Excel",
                        data=buf.getvalue(),
                        file_name=f"{selected_batch}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="download_xlsx_btn",
                    )
                else:
                    st.download_button(
                        "Download TXT",
                        data=("\n".join(kws)).encode("utf-8"),
                        file_name=f"{selected_batch}.txt",
                        mime="text/plain",
                        key="download_txt_btn",
                    )
            except Exception as e:
                st.error(f"Download failed: {e}")

        if start_btn:
            st.session_state.campaign_settings_open = True
            st.session_state.campaign_settings_batch = selected_batch

        show_settings = bool(st.session_state.get("campaign_settings_open")) and (
            st.session_state.get("campaign_settings_batch") == selected_batch
        )
        if show_settings:
            st.subheader("Campaign Settings")
            with st.form(key=f"campaign_settings_form__{selected_batch}"):
                limit_per_kw = st.number_input(
                    "Limit per keyword",
                    min_value=1,
                    max_value=500,
                    value=int(st.session_state.get("start_campaign_limit", 20) or 20),
                    key="start_campaign_limit",
                )
                workers = st.number_input(
                    "Workers",
                    min_value=1,
                    max_value=20,
                    value=int(st.session_state.get("start_campaign_workers", 1) or 1),
                    key="start_campaign_workers",
                )

                start_submit = st.form_submit_button("Start")
                cancel_submit = st.form_submit_button("Cancel")

            if cancel_submit:
                st.session_state.campaign_settings_open = False
                st.session_state.campaign_settings_batch = ""
                st.rerun()
            if start_submit:
                manager.start_campaign(selected_batch, int(limit_per_kw), int(workers))
                st.session_state.campaign_settings_open = False
                st.session_state.campaign_settings_batch = ""
                st.rerun()

        if delete_btn:
            if "confirm_delete_batch" not in st.session_state:
                st.session_state.confirm_delete_batch = None
            st.session_state.confirm_delete_batch = selected_batch

    if st.session_state.get("confirm_delete_batch"):
        bn = st.session_state.get("confirm_delete_batch")
        st.warning(f"Delete batch '{bn}'? This will remove keywords and batch record.")
        dc1, dc2 = st.columns(2)
        with dc1:
            if st.button("Yes, Delete", type="primary", key="confirm_delete_batch_yes"):
                delete_keyword_batch(bn)
                st.session_state.confirm_delete_batch = None
                st.rerun()
        with dc2:
            if st.button("Cancel", type="secondary", key="confirm_delete_batch_no"):
                st.session_state.confirm_delete_batch = None
                st.rerun()

    snap = manager.snapshot()
    if snap.get("mode") == "campaign":
        st.markdown("---")
        st.subheader("Campaign Status")
        st.markdown(f"**Running Status** `{snap.get('status')}`")
        if snap.get("current_batch"):
            st.markdown(f"**Batch** `{snap.get('current_batch')}`")
        if snap.get("current_tag"):
            st.markdown(f"**Current Keyword** `{snap.get('current_tag')}`")

        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Scraped", int(snap.get("total_scraped", 0)))
        m2.metric("Inserted", int(snap.get("inserted", 0)))
        m3.metric("Skipped (Duplicates)", int(snap.get("skipped", 0)))
        m4.metric("Mobile Found", int(snap.get("mobile_found", 0)))
        st.progress(int(snap.get("progress", 0)))

        # Show worker keyword assignments
        worker_keywords = snap.get("worker_current_keyword") or {}
        if worker_keywords:
            st.markdown("**Worker Assignments:**")
            for wid, keyword in sorted(worker_keywords.items(), key=lambda x: int(x[0])):
                # Get stats for this keyword if available
                keyword_stats = stats.get(keyword, {}) if 'stats' in dir() else {}
                total_found = keyword_stats.get("scraped", 0) if keyword_stats else 0
                if total_found > 0:
                    st.markdown(f"**Worker {wid}:** `{keyword}` (Found: {total_found})")
                else:
                    st.markdown(f"**Worker {wid}:** `{keyword}`")

        stop_campaign = st.button(
            "⏹️ Stop Campaign",
            type="secondary",
            key="stop_campaign_btn",
            disabled=not bool(snap.get("running")),
        )
        if stop_campaign:
            manager.stop()
            st.rerun()

        worker_errors = snap.get("worker_errors") or {}
        if worker_errors:
            st.warning(snap.get("error") or "One or more workers failed.")
            for wid in sorted(worker_errors.keys()):
                st.caption(f"Worker {wid}: {worker_errors[wid]}")

# Tab 3: History
with tab3:
    st.header("Search History")
    
    # Load history
    if not st.session_state.search_history:
        st.session_state.search_history = load_search_history()
    
    if st.session_state.search_history:
        st.markdown(f"**Total searches performed:** {len(st.session_state.search_history)}")
        
        # Display history
        for i, entry in enumerate(reversed(st.session_state.search_history), 1):
            with st.expander(f"Search #{len(st.session_state.search_history) - i + 1} - {entry['type'].title()} Search"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Timestamp:** {entry['timestamp']}")
                    st.write(f"**Type:** {entry['type'].title()}")
                    st.write(f"**Results:** {entry['results_count']} businesses")
                
                with col2:
                    if entry['type'] == 'single':
                        st.write(f"**Query:** {entry['query']}")
                        st.write(f"**Limit:** {entry['limit']}")
                    else:
                        st.write(f"**Queries ({len(entry['queries'])})**:")
                        for j, query in enumerate(entry['queries'], 1):
                            st.write(f"{j}. {query}")
        
        # Clear history button
        if st.button("🗑️ Clear History", type="secondary"):
            st.session_state.search_history = []
            save_search_history([])
            st.success("History cleared!")
            st.rerun()
    else:
        st.info("No search history found. Start scraping to see your search history here!")

# Tab 4: Saved Data
with tab4:
    st.header("Saved Data")
    st.markdown("Preview of data stored in SQLite database")

    col_a, col_b = st.columns([1, 3])
    with col_a:
        if st.button("Refresh", key="refresh_saved_data"):
            st.rerun()

    preview_limit = st.number_input(
        "Rows to preview",
        min_value=1,
        value=50,
        help="Shows the most recently inserted rows",
        key="saved_data_preview_limit",
    )

    df_last = fetch_last_rows(int(preview_limit))
    st.dataframe(df_last, use_container_width=True)

    st.markdown("---")
    st.subheader("Download")

    cexp1, cexp2 = st.columns([1, 2])
    with cexp1:
        if st.button("Prepare export", key="prepare_export_btn"):
            all_df = fetch_all_rows()
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
                all_df.to_excel(writer, index=False, sheet_name="Businesses")
            st.session_state.saved_export_excel_bytes = excel_buffer.getvalue()
            st.rerun()

    export_bytes = st.session_state.get("saved_export_excel_bytes")
    with cexp2:
        st.download_button(
            label="Download All Data (Excel)",
            data=(export_bytes or b""),
            file_name="google_maps_data.sqlite_export.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_all_excel",
            disabled=not bool(export_bytes),
        )

    st.markdown("---")
    st.subheader("Database Management")

    # Initialize confirmation state
    if "confirm_delete_db" not in st.session_state:
        st.session_state.confirm_delete_db = False

    if not st.session_state.confirm_delete_db:
        if st.button("🗑️ Remove Data from DB", type="primary", key="remove_data_btn"):
            st.session_state.confirm_delete_db = True
            st.rerun()
    else:
        st.warning("⚠️ Are you sure? This will permanently delete ALL data from the database!")
        col_confirm, col_cancel = st.columns(2)
        with col_confirm:
            if st.button("✅ Yes, Delete All Data", type="primary", key="confirm_delete_btn"):
                if clear_all_data():
                    st.success("✅ All data has been permanently deleted from the database!")
                    st.session_state.confirm_delete_db = False
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ Failed to delete data from database.")
        with col_cancel:
            if st.button("❌ Cancel", type="secondary", key="cancel_delete_btn"):
                st.session_state.confirm_delete_db = False
                st.rerun()


    # Refresh button and batch summary
    st.markdown("---")
    st.subheader("Batch Summary")
    
    col_refresh, _ = st.columns([1, 4])
    with col_refresh:
        if st.button("🔄 Refresh Stats", key="refresh_stats_btn"):
            st.rerun()
    
    batch_summary = get_batch_summary()
    if not batch_summary.empty:
        st.dataframe(batch_summary, use_container_width=True, hide_index=True)
        
        total_records = int(batch_summary["total_records"].sum())
        total_with_phone = int(batch_summary["with_phone"].sum())
        st.metric("Grand Total", f"{total_records} records ({total_with_phone} with phone)")
    else:
        st.info("No batch data found.")

    st.markdown("---")
    st.subheader("Tag Dashboard")

    stats = get_tag_stats()
    if stats:
        total_data = sum(int(s.get("scraped", 0) or 0) for s in stats.values())
        total_mobile = sum(int(s.get("mobile_found", 0) or 0) for s in stats.values())

        m1, m2, m3 = st.columns(3)
        m1.metric("Total Records", int(total_data))
        m2.metric("With Phone", int(total_mobile))
        m3.metric("Keywords", int(len(stats)))

        if "show_keyword_breakdown" not in st.session_state:
            st.session_state.show_keyword_breakdown = False

        btn_label = "Hide all keywords" if st.session_state.show_keyword_breakdown else "View all keywords"
        if st.button(btn_label, key="toggle_keyword_breakdown_btn"):
            st.session_state.show_keyword_breakdown = not st.session_state.show_keyword_breakdown
            st.rerun()

        if st.session_state.show_keyword_breakdown:
            lines = ["| Keyword | Total Data | Total Mobile No |"]
            lines.append("|---|---|---|")
            for tag, s in sorted(stats.items(), key=lambda kv: kv[0].lower()):
                lines.append(f"| {tag} | {s.get('scraped', 0)} | {s.get('mobile_found', 0)} |")
            st.markdown("\n".join(lines))
    else:
        st.info("No data in database yet.")

snap = manager.snapshot()
if auto_refresh and snap.get("running"):
    time.sleep(int(refresh_interval))
    st.rerun()

# Footer
st.markdown("---")
st.markdown("Built with ❤️ using Streamlit and Playwright")