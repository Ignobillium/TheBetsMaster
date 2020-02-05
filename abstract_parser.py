from datetime import datetime, timedelta
import inspect

from lxml import html

import pandas as pd
import numpy as np


class AbstractParser:
    """Match Parser Interface"""
    sep = ' - '

    @staticmethod
    def generateMatchName(home, away):
        return '%s%s%s' % (home, AbstractParser.sep, away)

    def __init__(self, raw_data):
        if isinstance(raw_data, bytes):
            self.data = raw_data.decode()
        elif isinstance(raw_data, str):
            self.data = raw_data
        else:
            raise TypeError("Given data is %s, not str or bytes" % type(raw_data))

        self._page = html.fromstring(self.data)

        self._home = None           # #
        self._away = None           # #

        self._datetime = None       # #
        self._time_shift = None     #

        self._current_score = None  #

        self._match_table = None    #

        # = = = = = = = = = = = = = = = = = = = =

        self._findHome = None           # #
        self._findAway = None           # #
        self._findCurrentScore = None   # #
        self._findDatetime = None       # #
        self._generateMatchTable = None # #

    @property
    def findHome(self):
        return self._findHome

    @findHome.setter
    def findHome(self, new_value):
        if str(type(new_value)) != "<class 'function'>":
            raise TypeError("findHome must be function, not %s" % type(new_value))
        elif len(inspect.getfullargspec(new_value).args) != 1:
            raise ValueError("Function findHome must take exactly one argument (AbstractParser obj aka self)")
        self._findHome = new_value

    @property
    def findAway(self):
        return self._findAway

    @findAway.setter
    def findAway(self, new_value):
        if str(type(new_value)) != "<class 'function'>":
            raise TypeError("findAway must be function, not %s" % type(new_value))
        elif len(inspect.getfullargspec(new_value).args) != 1:
            raise ValueError("Function findAway must take exactly one argument (AbstractParser obj aka self)")
        self._findAway = new_value

    @property
    def findCurrentScore(self):
        return self._findCurrentScore

    @findCurrentScore.setter
    def findCurrentScore(self, new_value):
        if str(type(new_value)) != "<class 'function'>":
            raise TypeError("findCurrentScore must be function, not %s" % type(new_value))
        elif len(inspect.getfullargspec(new_value).args) != 1:
            raise ValueError("Function findCurrentScore must take exactly one argument (AbstractParser obj aka self)")
        self._findCurrentScore = new_value

    @property
    def findDatetime(self):
        return self._findDatetime

    @findDatetime.setter
    def findDatetime(self, new_value):
        if str(type(new_value)) != "<class 'function'>":
            raise TypeError("findDatetime must be function, not %s" % type(new_value))
        elif len(inspect.getfullargspec(new_value).args) != 1:
            raise ValueError("Function findDatetime must take exactly one argument (AbstractParser obj aka self)")
        self._findDatetime = new_value


    @property
    def generateMatchTable(self):
        return self._generateMatchTable

    @generateMatchTable.setter
    def generateMatchTable(self, new_value):
        if str(type(new_value)) != "<class 'function'>":
            raise TypeError("generateMatchTable must be function, not %s" % type(new_value))
        elif len(inspect.getfullargspec(new_value).args) > 1:
            raise ValueError("Function generateMatchTable must take exactly one argument (AbstractParser obj aka self)")
        self._generateMatchTable = new_value

    @property
    def page(self):
        # page неизменяемый параметр класса, поэтому setter`а не будет
        return self._page

    @property
    def home(self):
        if self._home is None:
            self._home = self._findHome(self)
            if not isinstance(self._home, str):
                raise RuntimeError("Detected home is %s, not str" % type(self._home))
        return self._home

    @home.setter
    def home(self, new_home):
        if isinstance(new_home, str):
            self._home = new_home
        elif isinstance(new_home, bytes):
            self._home = new_home.decode()
        else:
            raise TypeError("Team name must be str or bytes, not %s" % type(new_home))

    @property
    def away(self):
        if self._away is None:
            self._away = self._findAway(self)
            if not isinstance(self._away, str):
                raise RuntimeError("Detected away is %s, not str" % type(self._away))
        return self._away

    @away.setter
    def away(self, new_away):
        if isinstance(new_away, str):
            self._away = new_away
        elif isinstance(new_away, bytes):
            self._away = new_away.decode()
        else:
            raise TypeError("Team name must be str or bytes, not %s" % type(new_away))

    @property
    def match_name(self):
        # т.к. ошибки типа обработаны в home и away,
        # этот метод не должен бросать отличные исключения
        return AbstractParser.generateMatchName(self.home, self.away)

    @property
    def current_score(self):
        """Возвращает текуший счёт в виде кортежа целых чисел"""
        if self._current_score is None:
            self._current_score = self._findCurrentScore(self)
            if not isinstance(self._current_score, tuple):
                raise RuntimeError("Detected current_score is %s, not tuple" % type(self._current_score))
            else:
                for j, i in enumerate(self._current_score):
                    if not isinstance(i, int):
                        raise RuntimeError("Detected current_score[%d] is %s, not int" % (j, type(i)))
        return self._current_score

    @property
    def datetime(self):
        if self._datetime is None:
            self._datetime = self._findDatetime(self)
            if not isinstance(datetime, self._datetime):
                raise RuntimeError("Detected datetime is not python.datetime # %s" % self.match_name)
        return self._datetime

    @datetime.setter
    def datetime(self, new_datetime):
        if not isinstance(new_datetime, datetime):
            raise TypeError("Datetime must be python.datetime, not %s" % type(new_datetime))
        self._datetime = new_datetime

    @property
    def shift(self):
        # аналогично с match_name
        return datetime.now() - self.datetime

    @property
    def match_table(self):
        if self._match_table is None:
            self._match_table = self._generateMatchTable()
            if not isinstance(self._match_table, pd.DataFrame):
                raise RuntimeError("Generated match_table is %s, not pd.DataFrame" % type(self._match_table))
            else:
                _err = []
                for col in ['match_name', 'time_shift', 'score1', 'score2',
                            'k_1_Fonbet', 'k_x_Fonbet', 'k_2_Fonbet',
                            'k_1_LigaStavok', 'k_x_LigaStavok','k_2_LigaStavo']:
                    if col not in self._match_table.columns:
                        _err.append(col)
                if len(_err) > 0:
                    raise RuntimeError("Columns %s are not presented in the generated match_table" % _err)
        return self._match_table
