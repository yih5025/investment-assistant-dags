CREATE TABLE IF NOT EXISTS public.sp500_top50 (
    symbol VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(255),
    sector VARCHAR(255),
    industry VARCHAR(255)
);