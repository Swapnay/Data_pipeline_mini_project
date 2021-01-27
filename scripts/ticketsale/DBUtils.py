# specify database configurations
import sqlalchemy as db
from sqlalchemy import event
import numpy as np

config = {
    'host': 'airflow_mysql',
    'port': 3306,
    'user': 'root',
    'password': 'Mi4man11',
    'database': 'ticket_sale'
}
db_user = config.get('user')
db_pwd = config.get('password')
db_host = config.get('host')
db_port = config.get('port')
db_name = config.get('database')
# specify connection string
connection_str = f'mysql+pymysql://{db_user}:{db_pwd}@{db_host}:{db_port}/{db_name}'
engine = db.create_engine(connection_str)
'''metadata.create_all(engine)'''


def add_own_encoders(conn, cursor, query, *args):
    cursor.connection.encoders[np.float64] = lambda value, encoders: float(value)
    cursor.connection.encoders[np.int64] = lambda value, encoders: int(value)


event.listen(engine, "before_cursor_execute", add_own_encoders)
