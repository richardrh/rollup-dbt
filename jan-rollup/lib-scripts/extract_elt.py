import polars as pl

connection_string = (
    "mssql+pyodbc://@pr0503-14002-00\\LMRMSINSURANCE/master"
    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)

query = """with rdm_results as (
	select
		distinct
			rp.anlsid,
			rp.id,
			rp.eventid,
			rp.perspcode,
			rp.perspvalue,
			rp.stddevi,
			rp.stddevc,
			rp.expvalue,
			ra.name, ra.description, ra.peril, ra.region,
			ae.eventid as ae_eventid, ae.rate
	from
	(select anlsid, id, eventid, perspcode, perspvalue, stddevi, stddevc, expvalue from dbo.rdm_port rp ) rp
    inner join (select id, name, description, peril, region from dbo.rdm_analysis ) ra on ra.id = rp.anlsid
	inner join (select anlsid, eventid, rate from dbo.rdm_anlsevent) ae on
								ae.anlsid = rp.anlsid
							and ae.anlsid = ra.id
							and ae.eventid = rp.eventid
	--where rp.perspcode = 'GR'

)

select anlsid, name, description, region, peril, eventid,  rate, perspvalue, sqrt(power(stddevi, 2) + power(stddevc, 2)) as stddev, expvalue
from rdm_results
where perspcode = 'RL'
and anlsid = 1;
"""

elt = pl.read_database(query, conn)
