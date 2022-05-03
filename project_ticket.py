import mysql.connector
from mysql.connector import Error
import pandas as pd

def get_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(user="root",
                                             password='AdeleElon5',
                                             host="localhost",
                                             port='3306',
                                             database='Sale_third_party')
    except Exception as error:
        print("Error while connecting to database for job tracker", error)
    return connection


def load_third_party(conn, file_path_csv):
     cursor = conn.cursor()
     empdata = pd.read_csv(file_path_csv, index_col=False, delimiter=',',header=None)
     for i, row in empdata.iterrows():
         sql = "insert INTO Sale_third_party.third_party_sales1 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
         cursor.execute(sql, tuple(row))
         print("Record inserted")
     conn.commit()
     cursor.close()



def query_popular_tickets(connection):

    # Get the most popular ticket in the past month
    sql_statement = "select event_name,sum(num_tickets)as sold_tickets \
                 from Sale_third_party.third_party_sales1\
                 group by event_name \
                 order by  sold_tickets desc \
                 limit 10"
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    print('Here are the most popular tickets in the past month:')
    return records


def print_tuples(records):
    """
    Prints tuple of records in an easy to read format
    Arguments:
        records -- query results (tuple)
    Returns:
        prints records
    """
    for i in records:
        print(*i)
    print('\n')


if __name__ == '__main__':
    file = '/Users/raniabadr/Documents/mini_projet_ticket/third_party_sales.csv'
    connection = get_db_connection()
    load_third_party(conn=connection, file_path_csv=file)
    records = query_popular_tickets(connection)
    print_tuples(records)


