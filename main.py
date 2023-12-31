from pathlib import Path
from typing import List
import pandas as pd
from datetime import datetime
import psycopg2
from psycopg2 import sql
from tabulate import tabulate


class WorkDataBase:
    def __init__(self):
        self.connection = psycopg2.connect(
            database="projectdb",
            host="localhost",
            user="projectdb",
            password="projectdb",
            port="5432",
        )

    def create_data_base(self, table: sql.SQL):
        cursor = self.connection.cursor()
        cursor.execute(table)
        self.connection.commit()

    def open_csv(self, csv_file_path: Path | str) -> pd.DataFrame:
        return pd.read_csv(csv_file_path)

    def parse_in_data_base(self, read_csv: pd.DataFrame, query: sql.SQL):
        csv_in_dict = read_csv.to_dict(orient="records")
        count = 0
        with self.connection.cursor() as cursor:
            for data in csv_in_dict:
                data['DATE'] = datetime.strptime(data['DATE'], '%d.%M.%Y').strftime('%Y-%M-%d')
                if pd.isna(data['IS_MODE']):
                    data['IS_MODE'] = None
                if pd.isna(data['IS_DEL']):
                    data['IS_DEL'] = None
                cursor.execute(query, tuple(data.values()))
                count += 1
                if count % 10 == 0:
                    self.connection.commit()
            self.connection.commit()

    def __del__(self):
        self.connection.close()


class TreatmentDataBaseInConsole(WorkDataBase):
    def __init__(self):
        super().__init__()
        self.cursor = self.connection.cursor()

    def __execute_and_print(self, select_query: sql.SQL, headers: List[str]):
        self.cursor.execute(select_query)
        rows = self.cursor.fetchall()
        table = tabulate(rows, headers=headers, tablefmt="simple")
        print(table, "\n")

    def print_expensive_project(self, select_query: sql.SQL):
        headers = ["Project", "Type House", "Beds", "Area", "Price", "Date"]
        self.__execute_and_print(select_query, headers)

    def print_big_square_project(self, select_query: sql.SQL):
        headers = ["Project", "Type House", "Beds", "Area", "Price", "Date"]
        self.__execute_and_print(select_query, headers)

    def print_all_villa_in_console(self, select_query: sql.SQL):
        headers = ["Project", "Count"]
        self.__execute_and_print(select_query, headers)

    def print_all_project_in_console(self, select_query: sql.SQL):
        headers = ["Quantity", "Project", "Type Project"]
        self.__execute_and_print(select_query, headers)

    def print_search_project(self, select_query: sql.SQL):
        headers = ["Project", "Type House", "Beds", "Area", "Price", "Date"]
        self.__execute_and_print(select_query, headers)


def main():
    create_table_query = sql.SQL('''
        CREATE TABLE IF NOT EXISTS project(
            id BIGSERIAL PRIMARY KEY,
            cid INT,
            unit VARCHAR(255),
            w_id INT UNIQUE ,
            utype VARCHAR(255),
            beds INT,
            area FLOAT,
            price INT,
            date DATE,
            is_mode BOOLEAN,
            is_del BOOLEAN
        );
        ''')
    query_insert_in_table = sql.SQL(f"""
                        INSERT INTO project (cid, unit, w_id, utype, beds, area, price, date, is_mode, is_del) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
                        ON CONFLICT (w_id) DO UPDATE SET 
                        cid = EXCLUDED.cid,
                        unit = EXCLUDED.unit,
                        utype = EXCLUDED.utype,
                        beds = EXCLUDED.beds,
                        area = EXCLUDED.area,
                        price = EXCLUDED.price,
                        date = EXCLUDED.date,
                        is_mode = EXCLUDED.is_mode,
                        is_del = EXCLUDED.is_del
                        """)
    select_expensive_project = sql.SQL('''
                                            SELECT unit, utype, beds, area, price, date
                                            FROM project
                                            WHERE price = (SELECT MAX(price) from project)
                                            ''')
    select_big_square_project = sql.SQL('''
                                            SELECT unit, utype, beds, area, price, date
                                            FROM project
                                            WHERE area = (SELECT MAX(area) FROM project)
                                            ''')
    select_all_villa = sql.SQL('''
                                SELECT unit, COUNT(*) as Count
                                FROM project
                                WHERE utype = 'Villa'
                                GROUP BY unit
                                ORDER BY Count DESC
                                ''')
    select_all_project_type_house = sql.SQL('''
                                            SELECT COUNT(utype) as cnt, unit, utype
                                            FROM project
                                            GROUP BY unit, utype
                                            ORDER BY cnt
                                            ''')

    work_db = WorkDataBase()
    work_db.create_data_base(create_table_query)
    csv_file_path = 'axcapital_09082023.csv'
    csv_data = work_db.open_csv(csv_file_path)
    work_db.parse_in_data_base(csv_data, query_insert_in_table)
    treatment = TreatmentDataBaseInConsole()
    treatment.print_expensive_project(select_expensive_project)
    treatment.print_big_square_project(select_big_square_project)
    treatment.print_all_villa_in_console(select_all_villa)
    treatment.print_all_project_in_console(select_all_project_type_house)
    choice = str(input("Enter project: "))
    search_all_project_by_project = sql.SQL(f"""SELECT unit, utype, beds, area, price, date 
                                                FROM project
                                                WHERE unit ilike '%{choice}%'""")
    treatment.print_search_project(search_all_project_by_project)


if __name__ == "__main__":
    main()
