import polars as pl

# TODO: Fix this to use dlt sources
connection_string = (
    "mssql+pyodbc://@pr0503-14002-00\\LMRMSINSURANCE/master"
    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)


def extract_elt_from_rdm(analsis_ids: [int], conn=None) -> pl.DataFrame:

    query = """

    with analysis as (
        select id, name, description, peril, region from dbo.rdm_analysis
        where id in ({})
        )

    , rdm_port as (
                select anlsid, id, eventid, perspcode, perspvalue, stddevi, stddevc, expvalue
                from
                dbo.rdm_port
                    )

    , rdm_results as (
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
            rdm_port rp
        inner join (select id, name, description, peril, region from dbo.rdm_analysis ) ra on ra.id = rp.anlsid
        inner join (select anlsid, eventid, rate from dbo.rdm_anlsevent) ae on
                                    ae.anlsid = rp.anlsid
                                and ae.anlsid = ra.id
                                and ae.eventid = rp.eventid
        -- where rp.perspcode = 'GR'

    )

        select
        -- ids and metadata
        anlsid, name, description, region, peril, eventid,  rate, perspcode

        -- calculate sd
        sqrt(power(stddevi, 2) + power(stddevc, 2)) as stddev,
        perspvalue,
        expvalue

        from rdm_results

    """

    elt = pl.read_database(query, conn)
    return elt
