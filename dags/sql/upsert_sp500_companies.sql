INSERT INTO sp500_companies (
    symbol,
    company_name,
    gics_sector,
    gics_sub_industry,
    headquarters,
    date_added,
    cik,
    founded,
    updated_at
) VALUES (
    %(symbol)s,
    %(company_name)s,
    %(gics_sector)s,
    %(gics_sub_industry)s,
    %(headquarters)s,
    %(date_added)s::DATE,
    %(cik)s,
    %(founded)s,
    NOW()
)
ON CONFLICT (symbol) 
DO UPDATE SET
    company_name = EXCLUDED.company_name,
    gics_sector = EXCLUDED.gics_sector,
    gics_sub_industry = EXCLUDED.gics_sub_industry,
    headquarters = EXCLUDED.headquarters,
    date_added = EXCLUDED.date_added,
    cik = EXCLUDED.cik,
    founded = EXCLUDED.founded,
    updated_at = NOW();