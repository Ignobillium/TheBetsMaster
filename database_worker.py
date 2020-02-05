import asyncio
import sqlite3
import logging
import traceback

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
        try:
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
        except sqlite3.OperationalError as e:
            logging.exception(traceback.format_exc())
            txt = str(e)
            if 'no such column' in txt.lower():
                print(mp)
                stxt = txt.split(' ')
                column_name = stxt[-1]
                print('[!] sqlite3.OperationalError. No such column %s' % column_name)

                cursor = self.db_conn.cursor()
                req = 'ALTER TABLE %s ADD COLUMN %s' % (
                    self.data_table_name,
                    column_name)
                cursor.execute(req)
                self.db_conn.commit()

                logging.debug('create new write_data task after creating new column')
                asyncio.get_event_loop().create_task(self.write_data(mp))
            else:
                logging.error('Unexpected exception')
                logging.error(str(mp))
                print('\n\n', mp, '\n\n')


    async def mark_match_as_ended(self, mp):
        """Помечает матч как завершённый.

        Parameters:
        ----------
        match : `str` or `MatchParser object`
            Имя матча либо объект MatchParser, из которого его можно извлечь.
        """

        if isinstance(mp, MatchParser):
            df = pd.DataFrame({
                'match_name': mp.match_name},
                index=[1])
            df.to_sql(
                self.ended_matches_table_name,
                self.em_conn,
                if_exists='append',
                index=False)
        elif isinstance(mp, str):
            df = pd.DataFrame({
                'match_name': mp},
                index=[1])
            df.to_sql(
                self.ended_matches_table_name,
                self.em_conn,
                if_exists='append',
                index=False)
        else:
            raise TypeError("mp must be MatchParser or pd.DataFrame, not %s" % type(mp))
