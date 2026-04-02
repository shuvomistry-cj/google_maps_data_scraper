"""
monitor.py — Live terminal stats for Google Maps Scraper
Run in a separate terminal while scraping:
    python monitor.py
"""

import sqlite3
import time
import os
import sys

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scraper_data.sqlite")
REFRESH_SECONDS = 5


def clear():
    os.system("cls" if os.name == "nt" else "clear")


def get_stats():
    if not os.path.exists(DB_PATH):
        return None
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        # Total in DB
        total_db = conn.execute("SELECT COUNT(*) FROM businesses").fetchone()[0] or 0
        total_phone = conn.execute(
            "SELECT COUNT(*) FROM businesses WHERE phone_number IS NOT NULL AND TRIM(phone_number) != ''"
        ).fetchone()[0] or 0

        # Per-batch summary
        batch_rows = conn.execute("""
            SELECT COALESCE(batch_name,'(untagged)') AS batch,
                   COUNT(*) AS total,
                   SUM(CASE WHEN phone_number IS NOT NULL AND TRIM(phone_number)!='' THEN 1 ELSE 0 END) AS phone
            FROM businesses
            GROUP BY batch_name
            ORDER BY batch_name
        """).fetchall()

        # Active campaign keyword stats
        kw_batches = conn.execute(
            "SELECT DISTINCT batch_name FROM keyword_batches ORDER BY created_at DESC LIMIT 5"
        ).fetchall()

        kw_stats = []
        for (bname,) in kw_batches:
            row = conn.execute("""
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) AS done,
                    SUM(CASE WHEN status='in_progress' THEN 1 ELSE 0 END) AS inprog,
                    SUM(CASE WHEN status IS NULL OR status='' OR status='undone' THEN 1 ELSE 0 END) AS pending
                FROM keywords WHERE batch_name=?
            """, (bname,)).fetchone()

            last_row = conn.execute(
                """
                SELECT keyword, status, updated_at
                FROM keywords
                WHERE batch_name = ? AND updated_at IS NOT NULL AND TRIM(updated_at) <> ''
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                (bname,),
            ).fetchone()

            if last_row:
                last_keyword, last_status, last_updated_at = last_row[0], last_row[1], last_row[2]
            else:
                last_keyword, last_status, last_updated_at = None, None, None

            kw_stats.append((bname, row[0], row[1], row[2], row[3], last_keyword, last_status, last_updated_at))

        # Currently in_progress keywords
        in_progress = conn.execute("""
            SELECT batch_name, keyword, assigned_worker, updated_at
            FROM keywords WHERE status='in_progress'
            ORDER BY updated_at DESC LIMIT 20
        """).fetchall()

        return {
            "total_db": total_db,
            "total_phone": total_phone,
            "batch_rows": batch_rows,
            "kw_stats": kw_stats,
            "in_progress": in_progress,
        }
    finally:
        conn.close()


def main():
    print("🗺️  Google Maps Scraper — Live Terminal Monitor")
    print(f"   DB: {DB_PATH}")
    print(f"   Refreshing every {REFRESH_SECONDS}s  |  Ctrl+C to quit\n")

    iteration = 0
    while True:
        try:
            stats = get_stats()
            clear()
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            iteration += 1

            print("=" * 65)
            print(f"  🗺️  Google Maps Scraper Monitor   [{now}]  #{iteration}")
            print("=" * 65)

            if stats is None:
                print("\n  ⚠️  Database not found. Start the scraper first.\n")
            else:
                print(f"\n  📦 TOTAL IN DATABASE")
                print(f"     Records   : {stats['total_db']:,}")
                print(f"     With Phone: {stats['total_phone']:,}")

                if stats["batch_rows"]:
                    print(f"\n  📊 BY BATCH")
                    print(f"  {'Batch':<35} {'Records':>8} {'Phone':>8}")
                    print(f"  {'-'*35} {'-'*8} {'-'*8}")
                    for r in stats["batch_rows"]:
                        print(f"  {str(r[0])[:35]:<35} {int(r[1] or 0):>8,} {int(r[2] or 0):>8,}")

                if stats["kw_stats"]:
                    print(f"\n  🔑 KEYWORD BATCH PROGRESS")
                    for (bname, total, done, inprog, pending, last_keyword, last_status, last_updated_at) in stats["kw_stats"]:
                        pct = int((done / max(1, total)) * 100)
                        bar_filled = int(pct / 5)
                        bar = "█" * bar_filled + "░" * (20 - bar_filled)
                        print(f"\n  Batch : {bname}")
                        print(f"  [{bar}] {pct}%")
                        print(f"  Total={total:,}  Done={done:,}  In-Progress={inprog}  Pending={pending:,}")
                        if last_keyword:
                            lk = str(last_keyword)[:70]
                            ls = str(last_status or "")
                            lu = str(last_updated_at or "")
                            print(f"  Last = {lk} | {ls} | {lu}")

                if stats["in_progress"]:
                    print(f"\n  ⚡ CURRENTLY SCRAPING ({len(stats['in_progress'])} active)")
                    print(f"  {'Worker':<8} {'Keyword':<45} {'Since'}")
                    print(f"  {'-'*8} {'-'*45} {'-'*19}")
                    for r in stats["in_progress"]:
                        worker = str(r[2] or "?")
                        kw = str(r[1] or "")[:44]
                        since = str(r[3] or "")[:19]
                        print(f"  W{worker:<7} {kw:<45} {since}")
                else:
                    print(f"\n  ⏸️  No keywords currently in_progress")

            print(f"\n  Refreshing in {REFRESH_SECONDS}s...  Ctrl+C to quit")
            print("=" * 65)
            time.sleep(REFRESH_SECONDS)

        except KeyboardInterrupt:
            print("\n\n  Monitor stopped. Scraper continues in background.\n")
            sys.exit(0)
        except Exception as e:
            print(f"\n  Error reading DB: {e}")
            time.sleep(REFRESH_SECONDS)


if __name__ == "__main__":
    main()