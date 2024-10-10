WITH recursive date_series AS (
    -- Generate dates for June, July, August, and September 2020
    SELECT 
        generate_series('2020-06-01'::date, '2020-09-30'::date, '1 day'::interval) AS dt_report
),
trades_with_users AS (
    -- Removed duplicates from users
	-- Join trades with users where enable = 1 and open_time < close_time
    -- Make sure the trade is finished - colse_time is not null
    SELECT 
        t.*,
        u.currency
    FROM 
        public.trades t
    JOIN 
        (select distinct * from public.users) u 
    ON 
        t.login_hash = u.login_hash 
        AND t.server_hash = u.server_hash
    WHERE 
        u.enable = 1
        AND t.open_time < t.close_time
        and t.close_time is not null
),
combined_data AS (
    -- Combine date_series with all distinct combinations of login_hash, server_hash, and symbol from trades
    SELECT 
        ds.dt_report,
        tu.*
    FROM 
        date_series ds
    CROSS JOIN 
    	trades_with_users tu
    	
),
-- Sum volume for the previous 7 days including the current dt_report
sum_volume_prev_7d as(
	select
		cd.dt_report,
	    cd.login_hash,
	    cd.server_hash,
	    cd.symbol,
	    COALESCE(SUM(cd.volume), 0) as sum_volume_prev_7d
	from
		combined_data cd
	where
		cd.close_time::date > cd.dt_report - interval '7 days'
		and cd.dt_report >= cd.close_time::date
	group by
		cd.dt_report,
	    cd.login_hash,
	    cd.server_hash,
	    cd.symbol
),
-- Sum volume for all previous trades including the current dt_report
sum_volume_prev_all as(
	select
		cd.dt_report,
	    cd.login_hash,
	    cd.server_hash,
	    cd.symbol,
	    COALESCE(SUM(cd.volume), 0) as sum_volume_prev_all
	from
		combined_data cd
	where
		cd.dt_report >= cd.close_time::date
	group by
		cd.dt_report,
	    cd.login_hash,
	    cd.server_hash,
	    cd.symbol
),
-- Dense rank for the most volume traded by login/symbol in the previous 7 days
sum_volume_symbol_prev_7d as(
	select
		cd.dt_report,
		cd.login_hash,
		cd.symbol,
		COALESCE(SUM(cd.volume), 0) as sum_volume_symbol_prev_7d
	from
		combined_data cd
	where
		cd.close_time::date > cd.dt_report - interval '7 days'
		and cd.dt_report >= cd.close_time::date
	group by
		cd.dt_report,
	    cd.login_hash,
	    cd.symbol
),
rank_volume_symbol_prev_7d as(
	SELECT 
    	s.dt_report,
		s.login_hash,
		s.symbol,
		s.sum_volume_symbol_prev_7d,
    	DENSE_RANK() OVER (PARTITION BY s.dt_report ORDER BY s.sum_volume_symbol_prev_7d DESC) AS rank_volume_symbol_prev_7d
FROM 
    sum_volume_symbol_prev_7d s
),
-- Dense rank for the most trade count by login in the previous 7 days
sum_count_prev_7d as(
	select
		cd.dt_report,
		cd.login_hash,
		COUNT(*) as sum_count_prev_7d
	from
		combined_data cd
	where
		cd.close_time::date > cd.dt_report - interval '7 days'
		and cd.dt_report >= cd.close_time::date
	group by
		cd.dt_report,
	    cd.login_hash
),
rank_count_prev_7d as(
	SELECT 
    	s.dt_report,
		s.login_hash,
		s.sum_count_prev_7d,
    	DENSE_RANK() OVER (PARTITION BY s.dt_report ORDER BY s.sum_count_prev_7d DESC) AS rank_count_prev_7d
FROM 
    sum_count_prev_7d s
),
-- Sum of volume traded in August 2020
sum_volume_2020_08 as(
	select
		cd.dt_report,
	    cd.login_hash,
	    cd.server_hash,
	    cd.symbol,
	    COALESCE(SUM(cd.volume), 0) as sum_volume_2020_08
	from
		combined_data cd
	where
		cd.close_time::date >= '2020-08-01'
        AND cd.close_time::date <= '2020-08-31'
        and cd.dt_report >= cd.close_time::date
	group by
		cd.dt_report,
	    cd.login_hash,
	    cd.server_hash,
	    cd.symbol
),
-- First trade by login/server/symbol up to and including the current dt_report
date_first_trade as(
	select
		cd.dt_report,
	    cd.login_hash,
	    cd.server_hash,
	    cd.symbol,
	    MIN(cd.close_time) as date_first_trade
	from
		combined_data cd
	where
		cd.dt_report >= cd.close_time::date
	group by
		cd.dt_report,
	    cd.login_hash,
	    cd.server_hash,
	    cd.symbol
),
aggregated_data as(
	select
	    cd.dt_report,
	    cd.login_hash,
	    cd.server_hash,
	    cd.symbol,
	    cd.currency,
	    COALESCE(svp7.sum_volume_prev_7d, 0)::DOUBLE PRECISION as sum_volume_prev_7d,
	    COALESCE(svpa.sum_volume_prev_all, 0)::DOUBLE PRECISION as sum_volume_prev_all,
	    COALESCE(rvsp7.rank_volume_symbol_prev_7d, 0)::int as rank_volume_symbol_prev_7d,
	    COALESCE(rcp7.rank_count_prev_7d, 0)::int as rank_count_prev_7d,
	    COALESCE(sv.sum_volume_2020_08, 0)::DOUBLE PRECISION as sum_volume_2020_08,
	    dft.date_first_trade::timestamp,
	    ROW_NUMBER() OVER (ORDER BY cd.dt_report, cd.login_hash, cd.server_hash, cd.symbol) AS row_number
	from
		(select
			distinct
			dt_report,
		    login_hash,
		    server_hash,
		    symbol,
		    currency
	    from
	    	combined_data) cd
	left join
		sum_volume_prev_7d svp7
	on
		cd.dt_report = svp7.dt_report
		and cd.login_hash = svp7.login_hash
	    and cd.server_hash = svp7.server_hash
	    and cd.symbol = svp7.symbol
	left join
		sum_volume_prev_all svpa
	on
		cd.dt_report = svpa.dt_report
		and cd.login_hash = svpa.login_hash
	    and cd.server_hash = svpa.server_hash
	    and cd.symbol = svpa.symbol
	left join
		rank_volume_symbol_prev_7d rvsp7
	on
		cd.dt_report = rvsp7.dt_report
		and cd.login_hash = rvsp7.login_hash
	    and cd.symbol = rvsp7.symbol
	left join
		rank_count_prev_7d rcp7
	on
		cd.dt_report = rcp7.dt_report
		and cd.login_hash = rcp7.login_hash
	left join
		sum_volume_2020_08 sv
	on
		cd.dt_report = sv.dt_report
		and cd.login_hash = sv.login_hash
	    and cd.server_hash = sv.server_hash
	    and cd.symbol = sv.symbol
	left join
		date_first_trade dft
	on
		cd.dt_report = dft.dt_report
		and cd.login_hash = dft.login_hash
	    and cd.server_hash = dft.server_hash
	    and cd.symbol = dft.symbol
)
-- Final select with independent serial ID and row number
SELECT 
    -- Generate a sequential ID (serial)
    ROW_NUMBER() OVER () AS id,
    TO_CHAR(dt_report, 'YYYY-MM-DD') as dt_report,
    login_hash,
    server_hash,
    symbol,
    currency,
    sum_volume_prev_7d,
    sum_volume_prev_all,
    rank_volume_symbol_prev_7d,
    rank_count_prev_7d,
    sum_volume_2020_08,
    date_first_trade,
    row_number
FROM 
    aggregated_data
ORDER BY 
    row_number DESC;