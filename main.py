from pathlib import Path
from typing import List
import pandas as pd
from sqlalchemy import create_engine
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

    def parse_in_data_base(self, read_csv: pd.DataFrame):
        connection_string = f'postgresql://{"projectdb"}:{"projectdb"}@{"localhost"}:{"5432"}/{"projectdb"}'
        engine = create_engine(connection_string)
        read_csv.columns = map(str.lower, read_csv.columns)
        read_csv['date'] = read_csv['date'].apply(lambda value: datetime.strptime(value, '%d.%M.%Y').strftime('%Y-%M-%d'))
        read_csv.to_sql("project", engine, if_exists='append', index=False)

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
        headers = ["Project", "Villa", "Apartment", "Townhouse", "Penthouse"]
        self.__execute_and_print(select_query, headers)


def main():
    create_table_query = sql.SQL('''
        CREATE TABLE IF NOT EXISTS project(
            id BIGSERIAL PRIMARY KEY,
            cid INT,
            unit VARCHAR(255),
            w_id INT,
            utype VARCHAR(255),
            beds INT,
            area FLOAT,
            price INT,
            date DATE,
            is_mode BOOLEAN,
            is_del BOOLEAN
        );
        ''')
    select_expensive_project = sql.SQL('''SELECT unit, utype, beds, area, price, date
                                            FROM project
                                            WHERE price = (SELECT MAX(price) from project);''')
    select_big_square_project = sql.SQL('''SELECT unit, utype, beds, area, price, date
                                            FROM project
                                            WHERE area = (SELECT MAX(area) FROM project);''')
    select_all_villa = sql.SQL('''
                                SELECT unit, COUNT(*) as Count
                                FROM project
                                WHERE utype = 'Villa'
                                GROUP BY unit
                                ORDER BY Count DESC;
                                ''')
    select_all_project_type_house = sql.SQL('''
                                            SELECT
                                                unit as Project,
                                                COUNT(CASE WHEN utype = 'Villa' THEN 1 END) as Villa,
                                                COUNT(CASE WHEN utype = 'Apartment' THEN 1 END) as Apartment,
                                                COUNT(CASE WHEN  utype = 'Townhouse' THEN 1 END) as Townhouse,
                                                COUNT(CASE WHEN utype = 'Penthouse' THEN 1 END ) as Penthouse
                                            FROM project
                                            GROUP BY unit
                                            ORDER BY unit ASC;
                                            ''')
    work_db = WorkDataBase()
    work_db.create_data_base(create_table_query)
    csv_file_path = 'axcapital_09082023.csv'
    csv_data = work_db.open_csv(csv_file_path)
    work_db.parse_in_data_base(csv_data)

    treatment = TreatmentDataBaseInConsole()
    treatment.print_expensive_project(select_expensive_project)
    treatment.print_big_square_project(select_big_square_project)
    treatment.print_all_villa_in_console(select_all_villa)
    treatment.print_all_project_in_console(select_all_project_type_house)


if __name__ == "__main__":
    main()
