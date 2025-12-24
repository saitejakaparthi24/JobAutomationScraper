# db_client.py

import psycopg2
from datetime import datetime

class DBClient:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="jobs_scraper",
            user="postgres",
            password="admin"
        )
        self.cur = self.conn.cursor()

    # -----------------------------
    # START RUN LOG
    # -----------------------------
    def log_run_start(self, keyword, source_portal):
        sql = """
        INSERT INTO scraper_run_log (keyword, source_portal, run_date, start_time, create_id)
        VALUES (%s, %s, CURRENT_DATE, NOW(), 'SCRAPER')
        RETURNING run_id;
        """
        self.cur.execute(sql, (keyword, source_portal))
        run_id = self.cur.fetchone()[0]
        self.conn.commit()
        print(f"🟢 Run started for keyword '{keyword}' | Run ID: {run_id}")
        return run_id

    # -----------------------------
    # END RUN LOG
    # -----------------------------
    def log_run_end(self, run_id, total):
        sql = """
        UPDATE scraper_run_log
        SET end_time = NOW(),
            total_jobs_scraped = %s,
            update_date = NOW(),
            update_id = 'SCRAPER'
        WHERE run_id = %s;
        """
        self.cur.execute(sql, (total, run_id))
        self.conn.commit()
        print(f"🟢 Run ended | Run ID: {run_id} | Total jobs: {total}")

    # -----------------------------
    # UPSERT INTO job_master
    # -----------------------------
    def upsert_master(self, job, source_portal):
        sql = """
            INSERT INTO job_master
            (job_title, company_name, location, posted_date, keyword, job_url, mobile_url,
             source_portal, create_id, create_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'SCRAPER', NOW())
            RETURNING job_id;
            """
        values = (
            job["Title"],
            job["Company"],
            job["Location"],
            job["Date Posted"],
            job["Keyword"],
            job["Job Link"],
            job["Mobile Link"],
            source_portal
        )
        self.cur.execute(sql, values)
        job_id = self.cur.fetchone()[0]
        self.conn.commit()
        return job_id

    # -----------------------------
    # ARCHIVE MASTER TO HISTORY
    # -----------------------------
    def archive_master_to_history(self):
        # Archive all jobs from job_master to job_daily_history
        sql = """
        INSERT INTO job_daily_history
        (job_id, snapshot_date, keyword, job_title, company_name, location,
         posted_date, job_url, mobile_url, source_portal, create_id)
        SELECT job_id, CURRENT_DATE, keyword, job_title, company_name, location,
               posted_date, job_url, mobile_url, source_portal, 'SCRAPER'
        FROM job_master;
        """
        self.cur.execute(sql)
        self.conn.commit()


    # -----------------------------
    # Clear job_master for today run
    # -----------------------------
    def clear_master(self):
        self.cur.execute("""
            DELETE FROM job_master 
            WHERE create_date < CURRENT_DATE;
        """)
        self.conn.commit()
        print("🧹 Cleared job_master for today's run")


    # -----------------------------
    # CLEANUP HISTORY (>100 days)
    # -----------------------------
    def cleanup_history(self):
        self.cur.execute("""
            DELETE FROM job_daily_history 
            WHERE snapshot_date < CURRENT_DATE - INTERVAL '100 days';
        """)
        self.cur.execute("""
            DELETE FROM scraper_run_log
            WHERE run_date < CURRENT_DATE - INTERVAL '100 days';
        """)
        self.conn.commit()
        print("🟢 Cleanup completed for history and run_log older than 100 days")

    # -----------------------------
    # CLOSE CONNECTION
    # -----------------------------
    def close(self):
        self.cur.close()
        self.conn.close()
        print("🟢 Database connection closed")
