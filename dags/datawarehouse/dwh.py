from datawarehouse.data_utils import (
    get_conn_cursor,
    close_conn_cursor,
    create_schema,
    create_table,
    get_video_ids
)

from datawarehouse.data_loading import load_data
from datawarehouse.data_modification import insert_rows, update_rows
from datawarehouse.data_transformation import transform_data

import logging
logging.basicConfig(level=logging.INFO)

from airflow.decorators import task

logger = logging.getLogger(__name__)
table = "yt_api"

@task(task_id="update_staging")
def staging_table():
    
    schema = "staging"

    conn,cur = None, None

    try:
        conn, cur = get_conn_cursor()

        YT_data = load_data()

        if not YT_data:
            raise ValueError("JSON file is empty or not found")

        print(f"DEBUG: rows loaded = {len(YT_data)}")
        logger.info(f"Number of rows loaded from JSON: {len(YT_data)}")


        logger.info(f"Number of rows loaded from JSON: {len(YT_data)}")

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        for row in YT_data:

            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)

            else:
                if row["video_id"] in table_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)
                        
        conn.commit()
        logger.info(f"{schema} table update completed")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"An error occured during the update of {schema} table: {e}")
        raise e
    
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)


@task(task_id="update_core")
def core_table():
    
    schema = "core"

    conn, cur = None, None

    try:

        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_ids(cur, schema)

        current_video_ids = set()

        cur.execute(f"SELECT * FROM staging.{table};")
        rows = cur.fetchall()

        for row in rows:

            current_video_ids.add(row["Video_ID"])

            if len(table_ids) == 0:

                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)

            else:
                transformed_row = transform_data(row)

                if transformed_row["Video_ID"] in table_ids:
                    update_rows(cur, conn, schema, transformed_row)

                else:
                    insert_rows(cur, conn, schema, transformed_row)

        conn.commit()
        logger.info(f"{schema} table update completed")
    
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"An error occured during the update of {schema} table: {e}")
        raise e
    
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)


