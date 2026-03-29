import psycopg2
import os


def get_conn():

    host = os.getenv("POSTGRES_HOST", "postgres")
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    database = os.getenv("POSTGRES_DB", "airflow")
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")

    return psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


def insert_rows(table, rows):

    conn = get_conn()
    cur = conn.cursor()

    if "." not in table:
        table = f"billboard.{table}"

    query = f"""
        INSERT INTO {table} (chart_date, song_name, artist, rank, last_week, peak_position, weeks_in_chart, image_url)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (chart_date, rank) DO NOTHING
    """

    print(f"Inserting {len(rows)} rows into {table}:")
    for r in rows:
        print(f"  {r}")

    try:
        cur.executemany(query, rows)
    except Exception as e:
        conn.rollback()
        raise e

    conn.commit()

    cur.close()
    conn.close()


def export_table_to_csv(table, output_path):
    conn = get_conn()
    cur = conn.cursor()

    if "." not in table:
        table = f"billboard.{table}"

    copy_query = f"""
        COPY (
            SELECT 
                chart_date AS "Date",
                song_name AS "Song",
                artist AS "Artist",
                rank AS "Rank",
                last_week AS "Last Week",
                peak_position AS "Peak Position",
                weeks_in_chart AS "Weeks in Charts",
                image_url AS "Image URL"
            FROM {table}
            ORDER BY chart_date, rank
        )
        TO STDOUT WITH CSV HEADER
    """
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            cur.copy_expert(copy_query, f)
        
        print(f"Table {table} exportée vers {output_path}")
        
    except Exception as e:
        raise Exception(f"Erreur lors de l'export de {table}: {str(e)}")
    finally:
        cur.close()
        conn.close()
    
    return output_path