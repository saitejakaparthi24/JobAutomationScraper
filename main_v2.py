import asyncio
import httpx
from bs4 import BeautifulSoup
import csv
import re
import random

async def main():
    url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    params = {
        "keywords": "Software Engineer",  # Change this keyword if needed
        "location": "United States",
        "start": 0
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }

    job_postings = []
    pages = 100  # Number of pages (10 jobs per page, adjust as needed)

    async with httpx.AsyncClient(timeout=45.0) as client:
        for page in range(pages):
            params["start"] = page * 25  # Correct LinkedIn pagination step

            print(f"🔄 Fetching page {page}...")

            resp = await client.get(url, headers=headers, params=params)

            # Retry on 429 error
            if resp.status_code == 429:
                wait_time = random.randint(60, 120)
                print(f"🚫 Rate limited on page {page}. Sleeping for {wait_time} seconds before retrying...")
                await asyncio.sleep(wait_time)
                resp = await client.get(url, headers=headers, params=params)
                if resp.status_code == 429:
                    print(f"❌ Still rate limited after retry. Skipping page {page}.")
                    continue

            elif resp.status_code == 400:
                print(f"⚠️ Bad request on page {page}. No more jobs or invalid query. Stopping further scraping.")
                break  # Exit the loop — nothing more to scrape

            elif resp.status_code != 200:
                print(f"❌ Failed to fetch page {page}: HTTP {resp.status_code}. Sleeping briefly and skipping.")
                await asyncio.sleep(5)
                continue


            soup = BeautifulSoup(resp.content, "lxml")
            job_cards = soup.select("li")

            if not job_cards:
                print(f"ℹ️ No jobs found on page {page}. Stopping further requests.")
                break

            for job in job_cards:
                try:
                    title_tag = job.find("h3")
                    company_tag = job.find("h4")
                    location_tag = job.find("span", class_="job-search-card__location")
                    time_tag = job.find("time", class_="job-search-card__listdate--new")
                    link_tag = job.find("a", href=True)
                    earlyapplicant_tag = job.find("span", class_="job-posting-benefits__text")

                    if not all([title_tag, company_tag, location_tag, time_tag, link_tag, earlyapplicant_tag]):
                        continue

                    # Filter: Skip early/hot jobs (optionally keep if desired)
                    tag_text = earlyapplicant_tag.text.lower()
                    if ("actively hiring" in tag_text or "be an early applicant" in tag_text) and "hours" in time_tag.text.lower():
                        continue

                    title = title_tag.text.strip()
                    company = company_tag.text.strip()
                    location = location_tag.text.strip()
                    date_posted = time_tag["datetime"]
                    job_link = link_tag["href"]

                    # Extract job ID and form mobile-friendly link
                    job_id_match = re.search(r'/jobs/view/.*?-(\d+)(?:\?|$)', job_link)
                    mobile_link = ""
                    if job_id_match:
                        job_id = job_id_match.group(1)
                        mobile_link = f"https://www.linkedin.com/jobs/view/{job_id}"

                    # Double-check job is not expired/high-competition
                    job_detail_resp = await client.get(job_link, headers=headers)
                    job_detail_text = job_detail_resp.text.lower()

                    if ("no longer accepting applications" in job_detail_text or
                        "over 100 people clicked apply" in job_detail_text):
                        print(f"⚠️ Skipping expired/high-engagement job: {job_link}")
                        continue

                    # Add to final results
                    job_postings.append({
                        "Title": title,
                        "Company": company,
                        "Location": location,
                        "Date Posted": date_posted,
                        "Special Tag": earlyapplicant_tag.text.strip(),
                        "Job Link": job_link,
                        "Mobile Link": mobile_link
                    })

                    await asyncio.sleep(random.uniform(1.5, 3.0))  # Be polite

                except Exception as e:
                    print(f"⚠️ Error parsing job: {e}")
                    continue

            await asyncio.sleep(random.uniform(2.0, 5.0))  # Throttle between pages

    # Save results to CSV
    with open("filtered_linkedin_jobs.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Title", "Company", "Location", "Date Posted", "Special Tag", "Job Link", "Mobile Link"])
        writer.writeheader()
        writer.writerows(job_postings)

    print(f"\n✅ Scraped {len(job_postings)} filtered jobs and saved to 'filtered_linkedin_jobs.csv'.\n")

    # Print results
    for job in job_postings:
        print("🧾 Title       :", job["Title"])
        print("🏢 Company     :", job["Company"])
        print("📍 Location    :", job["Location"])
        print("✨ Tag         :", job["Special Tag"])
        # print("💻 Web Link    :", job["Job Link"])
        # print("📱 Mobile Link :", job["Mobile Link"])
        print("🔗 Link        :", job["Mobile Link"])
        print("-" * 80)

# Run the scraper
if __name__ == "__main__":
    asyncio.run(main())
