import clickhouse_connect

if __name__ == "__main__":
    ch = clickhouse_connect.get_client(
        host='localhost', 
        username='user', 
        password='password', 
        database='logs_db'
    )
    ch.command("ALTER TABLE dataset_chunks DELETE WHERE 1")


