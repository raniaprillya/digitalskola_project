import psycopg2
from psycopg2.extras import execute_values

# Database connection parameters for Database A (source database)
db_a_params = {
    'host': 'localhost',
    'database': 'fp_rn_raw',
    'user': 'rani',
    'password': '123456',
}

# Database connection parameters for Database B (destination database)
db_b_params = {
    'host': 'localhost',
    'database': 'fp_rn_result',
    'user': 'rani',
    'password': '123456',
}

# Create connections to Database A and Database B
conn_a = psycopg2.connect(**db_a_params)
conn_b = psycopg2.connect(**db_b_params)

# Create cursors for Database A and Database B
cursor_a = conn_a.cursor()
cursor_b = conn_b.cursor()

try:
    # Replace 'table_master' with the actual master table name
    master_table_name = 'rekap_jabar'

    # Replace 'columnA' and 'columnB' with the desired column names to select
    select_query = f"SELECT kode_prov as province_id, nama_prov as province_name FROM {master_table_name};"
    select_query2 = f"SELECT kode_kab as district_id, kode_prov as province_id, nama_kab as district_name FROM {master_table_name};"
    select_query3 = f'''SELECT ROW_NUMBER() OVER () AS id, SPLIT_PART(status_name, '_', 1) AS status_name, SPLIT_PART(status_name, '_', 2) AS status_detail, status_name as status
    FROM (
        SELECT distinct(status_name)
        FROM (
            SELECT 'closecontact_dikarantina' AS status_name FROM {master_table_name}
            UNION ALL
            SELECT 'closecontact_discarded' AS status_name FROM {master_table_name}
            UNION ALL
            SELECT 'closecontact_meninggal' AS status_name FROM {master_table_name}
            UNION ALL
            SELECT 'confirmation_meninggal' AS status_name FROM {master_table_name}
            UNION ALL
            SELECT 'confirmation_sembuh' AS status_name FROM {master_table_name}
            UNION ALL
            SELECT 'probable_diisolasi' AS status_name FROM {master_table_name}
            UNION ALL
            SELECT 'probable_discarded' AS status_name FROM {master_table_name}
            UNION ALL
            SELECT 'probable_meninggal' AS status_name FROM {master_table_name}
            UNION ALL
            SELECT 'suspect_diisolasi' AS status_name FROM {master_table_name}
            UNION ALL
            SELECT 'suspect_discarded' AS status_name FROM {master_table_name}
            UNION ALL
            SELECT 'suspect_meninggal' AS status_name FROM {master_table_name}
        ) AS subquery_alias
        order by status_name
    ) AS final_subquery;'''
    select_query4 = f"SELECT tanggal as date, kode_kab as disctrict_id, kode_prov as province_id, status, total from total_status;"


    # Execute the SELECT query on Database A
    cursor_a.execute(select_query)
    rows = cursor_a.fetchall()

    cursor_a.execute(select_query2)
    rows2 = cursor_a.fetchall()

    cursor_a.execute(select_query3)
    rows3 = cursor_a.fetchall()  

    cursor_a.execute(select_query4)
    rows4 = cursor_a.fetchall()   
    

    # Close the cursor for Database A
    cursor_a.close()

    # Replace 'table_child1' with the desired name for the child table in Database B
    table_child1_name = 'province_table'
    table_child2_name = 'district_table'
    table_child3_name = 'case_table'
    table_child4_name = 'total_status'
    table_child5_name = 'Province_Daily_Table'
    table_child6_name = 'District_Daily_Table'

    # Generate the SQL query to create Table_child1 in Database B
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_child1_name} (province_id int4, province_name text);"
    create_table_query2 = f"CREATE TABLE IF NOT EXISTS {table_child2_name} (district_id int4, province_id int4, district_name text);"
    create_table_query3 = f"CREATE TABLE IF NOT EXISTS {table_child3_name} (id int4, status_name text, status_detail text, status text);"
    create_table_query4 = f"CREATE TABLE IF NOT EXISTS {table_child4_name} (date date, disctrict_id int4, province_id int4, status text, total int4);"
    create_table_query5 = f"CREATE TABLE IF NOT EXISTS {table_child5_name} (id int4, province_id int4, case_id text, date date, total int4);"
    create_table_query6 = f"CREATE TABLE IF NOT EXISTS {table_child6_name} (id int4, disctrict_id int4, case_id text, date date, total int4);"

    # Execute the DDL query to create Table_child1 in Database B
    cursor_b.execute(create_table_query)
    print(f"Table '{table_child1_name}' has been successfully created in Database B.")
    cursor_b.execute(create_table_query2)
    print(f"Table '{table_child2_name}' has been successfully created in Database B.")
    cursor_b.execute(create_table_query3)
    print(f"Table '{table_child3_name}' has been successfully created in Database B.")
    cursor_b.execute(create_table_query4)
    print(f"Table '{table_child4_name}' has been successfully created in Database B.")
    cursor_b.execute(create_table_query5)
    print(f"Table '{table_child5_name}' has been successfully created in Database B.")
    cursor_b.execute(create_table_query6)
    print(f"Table '{table_child6_name}' has been successfully created in Database B.")

    # Generate the SQL query to insert data into Table_child1
    insert_query = f"INSERT INTO {table_child1_name} (province_id, province_name) VALUES %s"
    # Insert the data from the master table into Table_child1 in Database B
    psycopg2.extras.execute_values(cursor_b, insert_query, rows)
    print("Data has been successfully inserted into Table_child1 in Database B.")

    # Generate the SQL query to insert data into Table_child1
    insert_query = f"INSERT INTO {table_child2_name} (district_id, province_id, district_name) VALUES %s"
    # Insert the data from the master table into Table_child1 in Database B
    psycopg2.extras.execute_values(cursor_b, insert_query, rows2)
    print("Data has been successfully inserted into Table_child2 in Database B.")

    # Generate the SQL query to insert data into Table_child1
    insert_query = f"INSERT INTO {table_child3_name} (id, status_name, status_detail, status) VALUES %s"
    # Insert the data from the master table into Table_child1 in Database B
    psycopg2.extras.execute_values(cursor_b, insert_query, rows3)
    print("Data has been successfully inserted into Table_child3 in Database B.")

    insert_query = f"INSERT INTO {table_child4_name} (date, disctrict_id, province_id, status, total) VALUES %s"
    # Insert the data from the master table into Table_child1 in Database B
    psycopg2.extras.execute_values(cursor_b, insert_query, rows4)
    print("Data has been successfully inserted into Table_child4 in Database B.")

    select_query2_1 = f'''SELECT ROW_NUMBER() OVER () AS id, a.province_id, a.date, b.id as case_id, total
    FROM total_status a
    LEFT JOIN case_table b ON a.status=b.status
    '''
    # Execute the SELECT query on Database A
    cursor_b.execute(select_query2_1)
    rows_2_1 = cursor_b.fetchall()
    insert_query = f"INSERT INTO {table_child5_name} (id, province_id, date, case_id, total) VALUES %s"
    # Insert the data from the master table into Table_child1 in Database B
    psycopg2.extras.execute_values(cursor_b, insert_query, rows_2_1)
    print("Data has been successfully inserted into Table_child5 in Database B.")

    select_query2_2 = f'''SELECT ROW_NUMBER() OVER () AS id, a.disctrict_id, a.date, b.id as case_id, total
    FROM total_status a
    LEFT JOIN case_table b ON a.status=b.status
    '''
    # Execute the SELECT query on Database A
    cursor_b.execute(select_query2_2)
    rows_2_2 = cursor_b.fetchall()
    insert_query = f"INSERT INTO {table_child6_name} (id, disctrict_id, date, case_id, total) VALUES %s"
    # Insert the data from the master table into Table_child1 in Database B
    psycopg2.extras.execute_values(cursor_b, insert_query, rows_2_2)
    print("Data has been successfully inserted into Table_child6 in Database B.")

    # Commit the changes in Database B
    conn_b.commit()

except Exception as e:
    print(f"Error: {e}")
    conn_b.rollback()

# Close the cursor and connection for Database B
cursor_b.close()
conn_b.close()
