import sqlalchemy as db

# Database connection details
db_host = 'mysql-analytics-master.wwmibprivate.com'
db_name = 'analytics'
db_user = 'analytics'
db_password = 'wwmib3112'
db_port = '32010'

def db_connection(sql_username, sql_password, sql_host, sql_port, sql_db_name):
    try:
        sql_engine = db.create_engine(
            f"mysql+pymysql://{sql_username}:{sql_password}@{sql_host}:{sql_port}/{sql_db_name}",
            connect_args={'connect_timeout': 3000},
            pool_recycle=1800,  # Recycle connections every 30 minutes
            pool_pre_ping=True
        )
        sql_conn = sql_engine.connect()
        # logger.info("SQL connection established")
        return sql_conn, sql_engine
    except Exception as e:
        # logger.error(f"Error connecting to MySQL: {e}")
        return None, None
    


# Establish connection
sql_conn, sql_engine = db_connection(db_user, db_password, db_host, db_port, db_name)
