import psycopg2


def connect_postgresql(host='localhost', port='5432', dbname='cer', user='postgres', password='welcome1'):

    try:
        conn = psycopg2.connect('host={} port={} dbname={} user={} password={}'.format(host, port, dbname, user, password))
    except:
        print("Error: Cannot connect to postgreSQL")
    return conn


def access_postgresql(conn, query):

    # create a cursor
    cur = conn.cursor()

    cur.execute(query)

    rows = cur.fetchall()
    return rows


def disconnect_postgresql(conn):
    conn.close()
