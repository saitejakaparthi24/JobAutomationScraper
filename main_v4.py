# =============================================
#   BEAST MODE LINKEDIN SCRAPER (Daily CSV + Dedupe)
#   Console Output + CSV + Last 7 Days + Sorted
# =============================================

import asyncio
import httpx
from bs4 import BeautifulSoup
import csv
import re
import random
import os
from datetime import datetime, timedelta

# ---------------------------------------------
# CONFIG
# ---------------------------------------------
KEYWORDS = ["Data Engineer"]
LOCATION = "United States"
MAX_RETRY = 3


# ---------------------------------------------
# DATE FILTER (Last 7 days)
# ---------------------------------------------
def posted_within_last_week(date_str):
    try:
        post_date = datetime.fromisoformat(date_str.replace("Z", ""))
        return post_date >= datetime.now() - timedelta(days=7)
    except:
        return False


# ---------------------------------------------
# CLEAN JOB ID → Mobile Link
# ---------------------------------------------
def get_mobile_link(job_link):
    match = re.search(r"/jobs/view/.*?-(\d+)", job_link)
    return f"https://www.linkedin.com/jobs/view/{match.group(1)}" if match else job_link


# ---------------------------------------------
# LOAD PREVIOUS DAY JOB IDS FOR DEDUPLICATION
# ---------------------------------------------
def load_previous_ids(keyword):
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    filename = f"LinkedIn_jobs_{keyword.replace(' ', '')}_{yesterday}.csv"

    if not os.path.exists(filename):
        return set()

    previous_ids = set()
    try:
        with open(filename, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                link = row.get("Job Link", "")
                m = re.search(r"/jobs/view/.*?-(\d+)", link)
                if m:
                    previous_ids.add(m.group(1))
    except:
        pass

    return previous_ids


# ---------------------------------------------
# SCRAPE PER KEYWORD
# ---------------------------------------------
async def fetch_jobs_for_keyword(client, keyword):
    url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }

    job_postings = []
    seen_ids_today = set()
    seen_ids_yesterday = load_previous_ids(keyword)

    print(f"\n🚀 Starting scrape for keyword: {keyword}")
    print(f"📌 Loaded {len(seen_ids_yesterday)} previous job IDs (for dedupe)")

    page = 0
    retry = 0

    while True:
        params = {
            "keywords": keyword,
            "location": LOCATION,
            "sortBy": "R",          # Most recent
            "f_TPR": "r604800",     # Last 7 days
            "start": page * 25
        }

        print(f"🔄 Fetching page {page}...")

        try:
            resp = await client.get(url, headers=headers, params=params)

            # RATE LIMIT
            if resp.status_code == 429:
                wait_time = random.randint(45, 90)
                print(f"🚫 Rate limited (429). Sleeping {wait_time} sec...")
                await asyncio.sleep(wait_time)
                continue

            # OTHER ERRORS
            if resp.status_code != 200:
                print(f"❌ HTTP {resp.status_code} on page {page}")
                retry += 1
                if retry > MAX_RETRY:
                    print("❌ Too many failures. Stopping scraper.")
                    break
                await asyncio.sleep(5)
                continue

            retry = 0
            soup = BeautifulSoup(resp.content, "lxml")
            job_cards = soup.select("li")

            if not job_cards:
                print(f"ℹ️ No more jobs found for: {keyword}")
                break

            page += 1

            for job in job_cards:
                try:
                    title_tag = job.find("h3")
                    company_tag = job.find("h4")
                    location_tag = job.find("span", class_="job-search-card__location")
                    time_tag = job.find("time")
                    link_tag = job.find("a", href=True)

                    if not all([title_tag, company_tag, location_tag, time_tag, link_tag]):
                        continue

                    job_link = link_tag["href"]
                    mobile_link = get_mobile_link(job_link)

                    job_id_match = re.search(r"/jobs/view/.*?-(\d+)", job_link)
                    if not job_id_match:
                        continue
                    job_id = job_id_match.group(1)

                    # Dedupe: today's run
                    if job_id in seen_ids_today:
                        continue

                    # Dedupe: yesterday's run (main requirement)
                    if job_id in seen_ids_yesterday:
                        continue

                    seen_ids_today.add(job_id)

                    title = title_tag.text.strip()
                    company = company_tag.text.strip()
                    location = location_tag.text.strip()
                    date_posted = time_tag.get("datetime", "")

                    if not posted_within_last_week(date_posted):
                        continue

                    job_postings.append({
                        "Title": title,
                        "Company": company,
                        "Location": location,
                        "Date Posted": date_posted,
                        "Keyword": keyword,
                        "Job Link": job_link,
                        "Mobile Link": mobile_link
                    })

                except Exception as e:
                    print(f"⚠️ Parse Error: {e}")
                    continue

            await asyncio.sleep(random.uniform(1.5, 3.5))

        except Exception as e:
            print(f"⚠️ Fatal Error: {e}")
            break

    print(f"✅ Completed '{keyword}' — {len(job_postings)} jobs found.\n")
    return job_postings


# ---------------------------------------------
# MAIN
# ---------------------------------------------
async def main():
    async with httpx.AsyncClient(timeout=45.0) as client:
        for keyword in KEYWORDS:

            jobs = await fetch_jobs_for_keyword(client, keyword)

            # DAILY CSV NAME
            date_code = datetime.now().strftime("%Y%m%d")
            safe_kw = keyword.replace(" ", "")
            output_file = f"LinkedIn_jobs_{safe_kw}_{date_code}.csv"

            # SAVE CSV
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=[
                    "Title", "Company", "Location", "Date Posted",
                    "Keyword", "Job Link", "Mobile Link"
                ])
                writer.writeheader()
                writer.writerows(jobs)

            # CONSOLE OUTPUT
            print("\n========================")
            print(f"📢 RESULTS FOR {keyword}")
            print("========================\n")

            for job in jobs:
                print("🧾 Title   :", job["Title"])
                print("🏢 Company :", job["Company"])
                print("📍 Location:", job["Location"])
                print("📅 Posted  :", job["Date Posted"])
                print("🔑 Keyword :", job["Keyword"])
                print("🔗 Link    :", job["Mobile Link"])
                print("-" * 80)

            print(f"\n📁 Saved to file: {output_file}")
            print("🎉 Scraping Completed Successfully!\n")


# RUN
if __name__ == "__main__":
    asyncio.run(main())
