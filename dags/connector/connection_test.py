from sqlalchemy import create_engine

class ConnectionDB:
    def __init__(self, postgres_host, postgres_db, postgres_user, 
                 postgres_password, postgres_port) :
        self.postgres_host = postgres_host
        self.postgres_db = postgres_db
        self.postgres_user = postgres_user
        self.postgres_password = postgres_password
        self.postgres_port = postgres_port

    
    def postgres_connection(self):
        try:
            # membuat koneksi database postgreSQL
            conn_str = f'postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}'
            engine = create_engine(conn_str)
            print("koneksi ke postgreSQL berhasil!")
        except Exception as error:
            print("gagal koneksi ke postgreSQL:", error)
            engine = None
        return engine
