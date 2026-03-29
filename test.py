print("🧨🧨🧨🧨🧨 Searching for company ready for person extraction...")
sql = """
    SELECT *
    FROM sn71_company
    WHERE
        (contact_info ->> 'employeesCount')::int BETWEEN 0 AND 1000
        AND flag1 IS NULL
        AND company_check = 1
        AND country = 'US'
    ORDER BY
            CASE
                WHEN source IN ('contactout-50') THEN 0
                ELSE 1
            END,
        resp_score DESC NULLS LAST
    LIMIT 1
"""
sql += " FOR UPDATE SKIP LOCKED"
with DB_POOL.connection() as conn:
    with conn.transaction():
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql)
            rows = cur.fetchone()
            
            # Person extraction workflow - use flag1
            update_sql = """
            UPDATE sn71_company
            SET flag1 = -1
            WHERE website = %s AND flag1 IS NULL
            """
            cur.execute(update_sql, (rows['website'],))
            
# if company process is successfully ended, set flag1 to 1, otherwise set flag1 to 0

            
            

