import asyncio
import httpx
from bs4 import BeautifulSoup
import csv
import re

async def main():
    url = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
    params = {
        "keywords": "Software",  # Change this keyword if needed
        "location": "United States",
        "start": 0
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }

    job_postings = []
    pages = 10  # Number of pages to scrape (10 jobs per page)

    async with httpx.AsyncClient(timeout=45.0) as client:
        for page in range(pages):
            params["start"] = page * 25
            resp = await client.get(url, headers=headers, params=params)

            if resp.status_code != 200:
                print(f"Failed to fetch page {page}: {resp.status_code}")
                continue

            soup = BeautifulSoup(resp.content, "lxml")
            job_cards = soup.select("li")

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

                    # Filter: Check "Be an early applicant"
                    if (earlyapplicant_tag.text.lower().__contains__("actively hiring")
                            or earlyapplicant_tag.text.lower().__contains__("be an early applicant")
                            and time_tag.text.lower().__contains__("hours")):
                        continue
                    title = title_tag.text.strip()
                    company = company_tag.text.strip()
                    location = location_tag.text.strip()
                    date_posted = time_tag["datetime"]
                    job_link = link_tag["href"]
                    # Extract job ID from the original link
                    job_id_match = re.search(r'/jobs/view/.*?-(\d+)(?:\?|$)', job_link)
                    mobile_link = ""
                    if job_id_match:
                        job_id = job_id_match.group(1)
                        mobile_link = f"https://www.linkedin.com/jobs/view/{job_id}"



                    # Filter out expired or high-engagement jobs via job details
                    job_detail_resp = await client.get(job_link, headers=headers)
                    job_detail_text = job_detail_resp.text.lower()

                    if ("no longer accepting applications" in job_detail_text or
                        "over 100 people clicked apply" in job_detail_text):
                        print(f"Skipping expired/high-engagement job: {job_link}")
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

                except Exception as e:
                    print(f"Error parsing job: {e}")
                    continue

    # Save results to CSV
    with open("filtered_linkedin_jobs.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Title", "Company", "Location", "Date Posted", "Special Tag", "Job Link", "Mobile Link"])
        writer.writeheader()
        writer.writerows(job_postings)

    print(f"✅ Scraped {len(job_postings)} filtered jobs and saved to 'filtered_linkedin_jobs.csv'.")

    # Print results as plain text
    for job in job_postings:
        print("🧾 Title       :", job["Title"])
        print("🏢 Company     :", job["Company"])
        print("📍 Location    :", job["Location"])
        print("✨ Tag         :", job["Special Tag"])
        # print("💻 Web Link    :", job["Job Link"])
        # print("📱 Mobile Link :", job["Mobile Link"])
        print("🔗 Link :", job["Mobile Link"])
        print("-" * 80)

    print(f"✅ Scraped and printed {len(job_postings)} filtered jobs.")


# Run the scraper
if __name__ == "__main__":
    asyncio.run(main())
