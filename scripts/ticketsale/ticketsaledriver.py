from sqlalchemy import text
import numpy as np
import datetime
import pandas as pd
from scripts.ticketsale.DBUtils import engine


class TicketSales:

    st = text("""INSERT INTO ticket_sales(ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city,event_addr, customer_id, price, num_tickets)
     VALUES (:ticket_id, :trans_date, :event_id, :event_name,:event_date, :event_type, :event_city, :event_addr,:customer_id, :price, :num_tickets)  ON DUPLICATE KEY UPDATE ticket_id=ticket_id""")

    def load_third_party(self,file_path):
        #file_path = "../data/third_party_sales_1.csv"
        df = pd.read_csv(file_path)

        df = df.replace(np.nan, 0, regex=True)
        json_list = list()
        try:
            with engine.connect() as conn:
                with conn.begin():
                    for i in range(df.shape[0]):
                            ticket_id = df.iloc[i][0]
                            trans_date1 = df.iloc[i][1]
                            event_id = df.iloc[i][2]
                            event_name = df.iloc[i][3]
                            event_date1 = df.iloc[i][4]
                            event_type = df.iloc[i][5]
                            event_city = df.iloc[i][6]
                            event_addr = df.iloc[i][7]
                            customer_id = df.iloc[i][8]
                            price = df.iloc[i][9]
                            num_tickets = df.iloc[i][10]
                            trans_date = datetime.datetime.strptime(trans_date1, '%Y-%M-%d')
                            event_date =datetime.datetime.strptime(event_date1, '%Y-%M-%d')

                            args = {"ticket_id": ticket_id, "trans_date": trans_date, "event_id": event_id, "event_name": event_name, "event_date": event_date,
                                    "event_type": event_type, "event_city": event_city, "event_addr":event_addr, "customer_id":customer_id, "price":price, "num_tickets":num_tickets}
                            json_list.append(args)
                    if len(json_list) > 0:
                        data_save = tuple(json_list)
                        conn.execute(self.st, data_save)
        except Exception as ex:
            print(ex)
    def best_selling_event(self):

        sql_statement = "SELECT event_name, SUM(num_tickets) "\
                         "FROM ticket_sales "\
                         "GROUP BY event_name "\
                         "ORDER BY SUM(num_tickets) DESC LIMIT 2 "
        with engine.connect() as conn:
            result =conn.execute(sql_statement)
            print("2 Most popular events with high ticket sales :\n")
            for row in result.fetchall():
                print(row)




#ticket = TicketSales()
#ticket.best_selling_event()
