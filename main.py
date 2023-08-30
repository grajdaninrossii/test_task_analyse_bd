import sqlite3
import pandas as pd
import time
from datetime import datetime
from memory_profiler import profile


class Database:
    """ Класс для работы с базой данных
    (При наличие мультизадачности лучше вынести в отдельных файл)
    """

    # Соединение с базой данных и курсор
    connect: sqlite3.Connection
    cursor: sqlite3.Cursor

    # Имена файлов сервера и клиента
    csv_server_url: str
    csv_client_url: str

    # @profile
    def __init__(self, database_url, csv_server_url, csv_client_url):
        """ Создаем подключение к бд
        """

        self.connect = sqlite3.connect(database_url)
        self.cursor = self.connect.cursor()

        # Сохраняем имена файлов csv
        self.csv_server_url = csv_server_url
        self.csv_client_url = csv_client_url


    @profile
    def create_legal_user_table(self) -> str:
        """ Метод для создания таблицы, где будет сохраняться выборка

        Returns:
            str: сообщение о создании таблицы
        """

        # Создаем таблицу для данных пользователей.
        # Выбрал ключ новый id, ибо не уверен что ключ (timestamp, player_id) будет всегда уникальным
        self.cursor.execute("""CREATE TABLE IF NOT EXISTS legal_user (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER,
            player_id INTEGER ,
            event_id INTEGER,
            error_id INTEGER,
            json_server TEXT,
            json_client TEXT);
        """)
        self.connect.commit()

        self.__print_data_legal_users_in_table() # Посмотреть количество сохраненных нарушений
        return "Таблица создана!"


    # @profile
    def __uploading_data_from_csv_with_set_date(self) -> tuple[pd.DataFrame]:
        """Загружаем данные из csv

        Returns:
            tuple[pd.DataFrame]: кортеж с данными из cvs
        """
        return pd.read_csv(self.csv_server_url, delimiter=","), pd.read_csv(self.csv_client_url, delimiter=",")


    # @profile
    def __join_data_by_error(self, server: pd.DataFrame, client: pd.DataFrame) -> pd.DataFrame:
        """ Объединение датафреймов в один по error_id

        Args:
            server (pd.DataFrame): Датафрейм данных сервера
            client (pd.DataFrame): Датафрейм данных клиента

        Returns:
            pd.DataFrame: Объединенный датафрей через inner join
        """

        return server.join(client.set_index('error_id'), on='error_id', lsuffix="_server", rsuffix="_client", how='inner')


    # @profile
    def __exclude_cheaters_who_old_ban(self, dataframe: pd.DataFrame) -> list[pd.Series]:
        """ Метод для отчистки старых ошибок?

        Args:
            dataframe (pd.DataFrame): Объединенные записи

        Returns:
            list[pd.Series]: Отфильтрованные записи
        """

        # Выберем всех игроков с их временем из бд (Можно сделать вторым способом: по строчно делать запросы с проверкой и получинием данных об игроках)
        result = self.cursor.execute("""SELECT player_id, ban_time FROM cheaters""")

        player_in_cheaters = dict(result.fetchall())
        df_with_players = dataframe[dataframe["player_id"].isin(player_in_cheaters.keys())]
        timestamp_before_filter = time.time() - 24*60*60

        result = []
        for i, x in df_with_players.iterrows():

            # Преобразовываем строку к секундам от даты 1970, 1, 1
            date_time_obj = (datetime.strptime(player_in_cheaters.get(x["player_id"]), '%Y-%m-%d %H:%M:%S')-datetime(1970,1,1)).total_seconds()

            # предыдущие сутки или раньше относительно timestamp из server.csv
            if date_time_obj < timestamp_before_filter or date_time_obj < x['timestamp_server']:
                result.append(x)
        return result


    # @profile
    def __print_data_legal_users_in_table(self):
        """ Процедура для сверки данных в таблице
        """
        # self.cursor.execute("""PRAGMA table_info(legal_user);""")
        self.cursor.execute("""SELECT count(*) FROM legal_user;""")
        print(self.cursor.fetchall())


    @profile # Самая тяжелая функция. В процессе ее выполнения потребляется до 550 Mib оперативной памяти
    def save_new_cheaters_in_table(self) -> str:
        """ Метод для сохранения данных относительно новых нарушений

        Returns:
            str
        """

        # Загружаем файлы из csv файлов, после объединяем их по error_id
        df = self.__join_data_by_error(*self.__uploading_data_from_csv_with_set_date())
        filtered_df = self.__exclude_cheaters_who_old_ban(df)
        for x in filtered_df:
            self.cursor.execute("""INSERT INTO legal_user(player_id, timestamp, event_id, error_id, json_server, json_client)
                VALUES(?, ?, ?, ?, ?, ?)""",
                [x['player_id'], x['timestamp_server'], x['event_id'], x['error_id'], x['description_server'], x['description_client']])
        self.connect.commit()
        return "Данные сохранены!"


def main():
    """Основная функция для работы скрипта
    """

    action: str = input("Выгрузить данные ? (Y/n)")
    main_url_data: str = "./data" # общий путь к папке с файлами csv
    database: Database = Database(database_url=f"{main_url_data}/cheaters.db",
                                  csv_server_url=f"{main_url_data}/server.csv",
                                  csv_client_url=f"{main_url_data}/client.csv"
                                  )
    if action[0].lower() == "y":
        database.create_legal_user_table()
        database.save_new_cheaters_in_table() # Загрузить данные в бд
        print("Данные успешно сохранены!")
    return 0


# Начало выполнения скрипта
if __name__ == "__main__":
    main()