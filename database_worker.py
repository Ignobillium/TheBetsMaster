import sqlite3
import logging

import pandas as pd

from match_parser import MatchParser

class DataBaseWorker:
    """Посредник для удобной работы с базами данных.
    """
    def __init__(self, db_name, data_table_name, em_name, ended_matches_table_name):
        """
        Parameters:
        ----------
        db_name : `str`
            Имя базы данных, в которую будет вестись запись.
        data_table_name : `str`
            Имя таблицы, в которую будет записываться информация о матчах.
        em_name : `str`
            Имя базы данных, в которой будет вестись учёт завершённых матчей.
        ended_matche_table_name:
            Имя таблицы, в которую будет записываться информация о завершённых
            матчах.
        """
        self.db_conn = sqlite3.connect(db_name)
        self.em_conn = sqlite3.connect(em_name)

        self.db_name = db_name
        self.em_name = em_name

        self.data_table_name = data_table_name
        self.ended_matches_table_name = ended_matches_table_name

    def __del__(self):
        """Закрывает используемые (sqlite3.connection)s.
        """
        self.db_conn.close()
        self.em_conn.close()

    async def write_data(self, mp):
        """Записывает данные в БД.

        Parameters
        ----------
        data : ` MatchParser object ` либо ` pandas.DataFrame `
            Объект MatchParser с информацией о матче.
        """
        if isinstance(mp, MatchParser):
            logging.debug('Write data from match_parser to %s/%s' % (
                self.db_name, self.data_table_name))

            mp.match_table.to_sql(
                self.data_table_name,
                self.db_conn,
                if_exists='append',
                index=False)
        elif isinstance(mp, pd.DataFrame):
            logging.debug('Write data from pd.DataFrame to %s/%s' % (
                self.db_name, self.data_table_name))

            mp.to_sql(
                self.data_table_name,
                self.db_conn,
                if_exists='append',
                index=False)

    async def mark_match_as_ended(self, mp):
        """Помечает матч как завершённый.

        Parameters:
        ----------
        match : `str` or `MatchParser object`
            Имя матча либо объект MatchParser, из которого его можно извлечь.
        """
        def who_won(mp):
            score1 = mp.score1
            score2 = mp.score2

            ww = '1'
            if score1 == score2:
                ww = 'x'
            elif score1 < score2:
                ww = '2'

            return ww

        if isinstance(mp, MatchParser):
            df = pd.DataFrame({
                'match_name': mp.match_name,
                'who_won': who_won(mp)},
                index=[1])
            df.to_sql(
                self.ended_matches_table_name,
                self.em_conn,
                if_exists='append',
                index=False)
        elif isinstance(mp, str):
            df = pd.DataFrame({
                'match_name': mp,
                'who_won': who_won(mp)},
                index=[1])
            df.to_sql(
                self.ended_matches_table_name,
                self.em_conn,
                if_exists='append',
                index=False)
