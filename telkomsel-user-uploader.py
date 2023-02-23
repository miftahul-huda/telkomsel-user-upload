import sys, getopt
import argparse
from dotenv import load_dotenv
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime
import psycopg2.extras as extras



def main():

    load_dotenv()

    parser = argparse.ArgumentParser(
                    prog = 'telkomsel-user-uploader',
                    description = 'Upload user and store data to database',
                    epilog = 'Devoteam')

    parser.add_argument('-f', '--file', help='The csv file to upload', required=True)
    args = parser.parse_args()

    print("Uploading")
    upload_data(args.file)


def get_connection(dbhost, dbname, dbuser, dbpassword):
    conn = psycopg2.connect(
        database=dbname, user=dbuser, password=dbpassword, 
        host=dbhost, port='5432'
    )
    return conn


def upload_data(csvfile):
    df = pd.read_csv(csvfile)
    tag = datetime.today().strftime('%d-%B-%Y')
    df["tag"] = tag.lower()
    df["branch"] = df["branch"].str.upper()
    df["city"] = df["city"].str.upper()
    df["region"] = df["region"].str.upper()
    df["area"] = df["area"].str.upper()
    df["cluster"] = df["cluster"].str.upper()
    df["\"createdAt\""] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    process_users(df, tag)
    process_stores(df, tag)
    process_store_users(df, tag)




def process_users(df, tag):
    print("Processing users....")
    df_users = get_users(df)
    print("Users")
    print(df_users)
    env = os.environ
    conn = get_connection( env["DBHOST"], env["AUTHDB"], env["DBUSER"], env["DBPASSWORD"]  )
    insert_data(conn, df_users, env["AUTHTABLE"])
    execute_sql(conn, "update \"{}\" set email = create_username(sfcode), \"password\" = random_string(8) where tag like '{}'".format(env["AUTHTABLE"], tag.lower()))
    sql = """update \"{}\" set regional = csa.region 
            from cityregionarea csa
            where \"{}\".city like csa.city
            and tag like '{}'""".format(env["AUTHTABLE"], env["AUTHTABLE"], tag.lower())
    execute_sql(conn, sql)
    sql = """update \"{}\" set area = csa.area 
            from cityregionarea csa
            where \"{}\".regional like csa.region
            and tag like '{}'""".format(env["AUTHTABLE"], env["AUTHTABLE"], tag.lower())
    execute_sql(conn, sql)
    print("Processing users done....")
    conn.close()

def process_stores(df, tag):
    print("Processing stores....")

    df_stores = get_stores(df)
    print("Stores")
    print(df_stores)
    env = os.environ
    conn = get_connection( env["DBHOST"], env["DBNAME"], env["DBUSER"], env["DBPASSWORD"]  )
    insert_data(conn, df_stores, "store")
    sql = """update store set store_region = csa.region 
            from cityregionarea csa
            where store.store_city like csa.city
            and tag like '{}'""".format(tag.lower())
    execute_sql(conn, sql)
    sql = """update store set store_area = csa.area 
            from cityregionarea csa
            where store.store_region like csa.region
            and tag like '{}'""".format(tag.lower())
    execute_sql(conn, sql)

    conn.close()
    print("Processing stores done....")

def process_store_users(df, tag):
    print("Processing store users....")

    df_stores = get_store_users(df)
    print("Store  users")
    print(df_stores)
    env = os.environ
    conn = get_connection( env["DBHOST"], env["DBNAME"], env["DBUSER"], env["DBPASSWORD"]  )
    insert_data(conn, df_stores, "store_user")
    sql = """update "store_user" set username = u.email
            from
            (
                select * from dblink('dbname={} user={} password={}',
                                    'select sfcode, email from "{}" where tag like ''{}'''
                                    )
                as t1(sfcode varchar(255), email varchar(255))
            ) u
            where
            "store_user".sfcode like u.sfcode
            and 
            "store_user".tag like '{}'
            """.format( env["AUTHDB"], env["DBUSER"], env["DBPASSWORD"], env["AUTHTABLE"], tag.lower(), tag.lower())

    execute_sql(conn, sql)

    conn.close()
    print("Processing store users done....")


def get_users(df):

    if 'result' in df.columns:
        df = df.drop(["result"], axis=1)
    env = os.environ
    conn = get_connection( env["DBHOST"], env["AUTHDB"], env["DBUSER"], env["DBPASSWORD"]  )

    db_users = pd.read_sql_query("select sfcode from \"{}\" where tag is not null and tag <> '' and sfcode is not null and email is not null".format(env["AUTHTABLE"]), conn)

    #print(db_users)

    #user_df = df.loc[df.apply(lambda x: not db_users["sfcode"].str.lower().str.contains(x["sfcode"].lower()).any(), axis=1) ]

    df["result"] = df.apply(lambda x: db_users["sfcode"].str.lower().str.contains(x["sfcode"].lower()).any(), axis=1)
    user_df = df.loc[ df.apply(lambda x: bool(x["result"]) == False, axis=1  ) ]
    #print(df)
    #print(user_df)
    
    user_df = user_df.drop(["storeid", "store_name"], 1)
    user_df = user_df.drop_duplicates()
    user_df["\"isActive\""] = 1
    user_df["firstname"] = user_df["name"]
    user_df["regional"] = user_df["region"]

    if 'cluster' in user_df.columns:
        user_df["\"CLUSTER\""] = user_df["cluster"]

    user_df = user_df.drop(["name", "cluster", "region", "result"], 1)
    
    return user_df

def get_stores(df):

    if 'result' in df.columns:
        df = df.drop(["result"], axis=1)

    env = os.environ
    conn = get_connection( env["DBHOST"], env["DBNAME"], env["DBUSER"], env["DBPASSWORD"]  )

    db_stores = pd.read_sql_query("select storeid from \"store\" where tag is not null and tag <> '' and storeid is not null", conn)
    df["result"] = df.apply(lambda x: db_stores["storeid"].str.lower().str.contains(str(x["storeid"]).lower()).any(), axis=1)
    store_df = df.loc[ df.apply(lambda x: bool(x["result"]) == False, axis=1  ) ]
 


    store_df = store_df.drop(["sfcode", "name"], 1)
    store_df = store_df.drop_duplicates()
    store_df["store_city"] = store_df["city"]
    store_df["store_region"] = store_df["region"]
    store_df["store_branch"] = store_df["branch"]
    store_df["store_area"] = store_df["area"]

    if 'cluster' in store_df.columns:
        store_df["store_cluster"] = store_df["cluster"]
        store_df = store_df.drop(["cluster"], 1)

    #store_df["store_province"] = store_df["province"]
    store_df = store_df.drop(["city", "region", "branch", "area", "result"], 1)
    return store_df

def get_store_users(df):
    if 'result' in df.columns:
        df = df.drop(["result"], axis=1)

    env = os.environ
    conn = get_connection( env["DBHOST"], env["DBNAME"], env["DBUSER"], env["DBPASSWORD"]  )

    db_stores = pd.read_sql_query("select storeid||sfcode AS code from \"store_user\" where tag is not null and tag <> '' and storeid is not null and sfcode is not null", conn)
    df["result"] = df.apply(lambda x: db_stores["code"].str.lower().str.contains((str(x["storeid"]) + x["sfcode"]).lower()).any(), axis=1)
    store_df = df.loc[ df.apply(lambda x: bool(x["result"]) == False, axis=1  ) ]
 
    store_user_df = pd.DataFrame()
    store_user_df["sfcode"] = store_df["sfcode"]
    store_user_df["storeid"] = store_df["storeid"]
    store_user_df["store_name"] = store_df["store_name"]
    store_user_df["tag"] = store_df["tag"]
    store_user_df["\"createdAt\""] = store_df["\"createdAt\""]


    store_user_df = store_user_df.drop_duplicates()
    return store_user_df

def execute_sql(conn, sql):
    try:
        records = None
        cursor = conn.cursor()
        postgreSQL_select_Query = sql

        print("execute... ")
        print(postgreSQL_select_Query)
        cursor.execute(postgreSQL_select_Query)
        #print("Selecting rows from  table using cursor.fetchall")
        if not cursor.description is None:
            records = cursor.fetchall()
        else:
            conn.commit()

        return records

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
        conn.rollback()

    """
    finally:
        # closing database connection.
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed")
    """

def insert_data(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()



if __name__ == "__main__":
    main()



