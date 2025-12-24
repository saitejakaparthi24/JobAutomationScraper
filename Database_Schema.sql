CREATE TABLE job_master (
    job_id SERIAL PRIMARY KEY,
    job_title TEXT NOT NULL,
    company_name TEXT,
    location TEXT,
    job_url TEXT  NOT NULL,
	mobile_url TEXT  NOT NULL,
    source_portal TEXT,
    posted_date DATE,
    keyword TEXT NOT NULL,

    -- Audit fields
    create_date TIMESTAMP DEFAULT NOW(),
    create_id TEXT DEFAULT 'SCRAPER',
    update_date TIMESTAMP DEFAULT NOW(),
    update_id TEXT DEFAULT 'SCRAPER'
);


CREATE TABLE IF NOT EXISTS job_daily_history (
    history_id SERIAL PRIMARY KEY,       -- unique ID for history row
    job_id BIGINT,                       -- original job ID from master
    snapshot_date DATE NOT NULL,         -- the date this snapshot was taken
    keyword TEXT NOT NULL,
    job_title TEXT NOT NULL,
    company_name TEXT NOT NULL,
    location TEXT,
    posted_date DATE,
    job_url TEXT NOT NULL,
    mobile_url TEXT,
    source_portal TEXT NOT NULL,
    create_date TIMESTAMP DEFAULT NOW(),
    create_id TEXT DEFAULT 'SCRAPER',
    update_date TIMESTAMP,
    update_id TEXT
);


CREATE TABLE scraper_run_log (
    run_id SERIAL PRIMARY KEY,
    run_date DATE NOT NULL DEFAULT CURRENT_DATE,
    start_time TIMESTAMP NOT NULL DEFAULT NOW(),
    end_time TIMESTAMP,
    total_jobs_scraped INT DEFAULT 0,
	source_portal TEXT,
    keyword TEXT,

    -- Audit fields
    create_date TIMESTAMP DEFAULT NOW(),
    create_id TEXT DEFAULT 'SCRAPER',
    update_date TIMESTAMP DEFAULT NOW(),
    update_id TEXT DEFAULT 'SCRAPER'
);

