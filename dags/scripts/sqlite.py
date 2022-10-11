import pandas as pd
import sqlite3

# definição da função da PRIMEIRA atividade da DAG: criação de arquivo CSV


def sqlite_to_csv():
    con = sqlite3.connect('data/Northwind_small.sqlite')  # connection
    df = pd.read_sql_query('select * from "order"',
                           con)
    df.to_csv(path_or_buf='data/output_orders.csv')
    con.close()

# definição da função da SEGUNDA atividade da DAG: criação de arquivo data/count.txt


def sqlite_join():
    con = sqlite3.connect('data/Northwind_small.sqlite')
    df = pd.read_sql_query("""
    select SUM(OrderDetail.quantity)
    from "Order"
    LEFT join OrderDetail
    on "order"."Id" = OrderDetail.orderid
    where "order"."shipcity" = "Rio de Janeiro";
    """, con)
    f = open("data/count.txt", "w+")
    f.write(str(df.values).strip('[]'))
    con.close()
