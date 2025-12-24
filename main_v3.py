# LinkedIn New Grad / University / Early Career Job Scraper
import asyncio
import httpx
from bs4 import BeautifulSoup
import csv
import re
import random
from datetime import datetime, timedelta

keywords_list = ["Data Engineer"]

def posted_within_last_week(date_str):
    try:
        post_date = datetime.fromisoformat(date_str.replace("Z",""))
        return post_date >= datetime.now() - timedelta(days=7)
    except:
        return True

async def fetch_jobs_for_keyword(client, keyword):
    url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }

    # Keywords for new grad/university/early career roles

    job_postings = []
    page = 0
    while True:
        params = {
            "keywords": keyword,
            "location": "United States",
            "start": page * 25
        }

        print(f"🔄 Fetching '{keyword}' - page {page}...")
        try:
            resp = await client.get(url, headers=headers, params=params)

            if resp.status_code == 429:
                wait_time = random.randint(60, 120)
                print(f"🚫 Rate limited. Sleeping {wait_time} sec...")
                await asyncio.sleep(wait_time)
                continue

            elif resp.status_code == 400:
                print(f"ℹ️ Page {page} returned 400 – likely end of listings. Stopping.")
                break

            elif resp.status_code != 200:
                print(f"❌ Failed page {page}: HTTP {resp.status_code}")
                break

            soup = BeautifulSoup(resp.content, "lxml")
            job_cards = soup.select("li")

            if not job_cards:
                print(f"ℹ️ No jobs found on page {page} for '{keyword}'.")
                break

            page += 1

            for job in job_cards:
                try:
                    title_tag = job.find("h3")
                    company_tag = job.find("h4")
                    location_tag = job.find("span", class_="job-search-card__location")
                    time_tag = job.find("time")
                    link_tag = job.find("a", href=True)
                    exp_tag = job.find("span", class_="job-card-container__experience")

                    if not all([title_tag, company_tag, location_tag, time_tag, link_tag]):
                        continue

                    # Optional: filter by entry level
                    if exp_tag and "entry" not in exp_tag.text.lower():
                        continue

                    title = title_tag.text.strip()
                    company = company_tag.text.strip()
                    location = location_tag.text.strip()
                    date_posted = time_tag.get("datetime", "")
                    # Filter for only last 7 days
                    if not posted_within_last_week(date_posted):
                        continue
                    job_link = link_tag["href"]

                    # Extract job ID and form mobile-friendly link
                    job_id_match = re.search(r'/jobs/view/.*?-(\d+)(?:\?|$)', job_link)
                    mobile_link = f"https://www.linkedin.com/jobs/view/{job_id_match.group(1)}" if job_id_match else job_link

                    job_postings.append({
                        "Title": title,
                        "Company": company,
                        "Location": location,
                        "Date Posted": date_posted,
                        "Keyword": keyword,
                        "Job Link": job_link,
                        "Mobile Link": mobile_link
                    })

                    await asyncio.sleep(random.uniform(1.5, 3.0))

                except Exception as e:
                    print(f"⚠️ Error parsing job: {e}")
                    continue

            await asyncio.sleep(random.uniform(2.0, 5.0))

        except Exception as e:
            print(f"⚠️ Error fetching page {page}: {e}")
            continue

    return job_postings


async def main():
    all_jobs = []
    async with httpx.AsyncClient(timeout=45.0) as client:
        for keyword in keywords_list:
            jobs = await fetch_jobs_for_keyword(client, keyword)
            all_jobs.extend(jobs)

    # Save all results to CSV
    with open("new_grad_linkedin_jobs.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Title", "Company", "Location", "Date Posted", "Keyword", "Job Link",
                                               "Mobile Link"])
        writer.writeheader()
        writer.writerows(all_jobs)

    print(f"\n✅ Scraped {len(all_jobs)} jobs across keywords and saved to 'new_grad_linkedin_jobs.csv'.\n")

    # Optional: print results
    for job in all_jobs:
        print("🧾 Title   :", job["Title"])
        print("🏢 Company :", job["Company"])
        print("📍 Location:", job["Location"])
        print("📅 Posted  :", job["Date Posted"])
        print("🔑 Keyword :", job["Keyword"])
        print("🔗 Link    :", job["Mobile Link"])
        print("-" * 80)


# Run the scraper
if __name__ == "__main__":
    asyncio.run(main())
