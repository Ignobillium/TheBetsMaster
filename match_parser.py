from datetime import datetime, timedelta

from lxml import html
import numpy as np
import pandas as pd


class MatchParser:

    def __init__(self, raw_data, encoding='utf-8'):
        if isinstance(raw_data, bytes):
            self.page = html.fromstring(raw_data.decode(encoding))
        elif isinstance(raw_data, str):
            self.page = html.fromstring(raw_data)

        self.ratio_blocks = self.page.find_class('ratio-block')

        self._match_name = None

        # hda = home - draw - away, 1x2
        self._hda = None
        self._hda_k = None

        self._current_score = None
        self._score1 = None
        self._score2 = None

        self._team1 = None
        self._team2 = None

        self._match_datetime = None
        self._time_shift = None

        self._match_table = None

    @staticmethod
    def generate_match_name(team1, team2):
        """Генерирует из предоставленыз названий команд имя матча.
        Ф-ция введена для стандартизации этого говна, чтобы не вспоминать в процессе
        разработки: "А как я составлял имя матча?.."

        Parameters
        ----------
        team1 : `str`
            Название первой команды.
        team2 : `str`
            Название второй команды.

        Returns
        ----------
        match_name : имя матча (удивительно (восхитительно (как легко свою жизнь ...)))
        """
        return '%s vs %s' % (team1, team2)

    @staticmethod
    def get_current_score(page):
        """Возвращает текущий счёт в виде кортежа из двух элементов.

        Parameters
        ----------
        page : ` lxml.html.HtmlElement `
            Суть страница матча

        Returns
        ----------
        current_score : ` list of float `
            (score_1, score_2).
        """
        current_score_block = page.find_class('current_score')
        if len(current_score_block) == 0:
            #! error message
            print('[!] There is no current_score block!')
            return (None, None)

        return tuple(
            map(
                lambda x: float(x),
                current_score_block[0].text_content().split()[0].split(':')))

    @staticmethod
    def get_teams(page):
        """Возвращает кортеж из названий команд.

        Parameters
        ----------
        page : ` lxml.html.HtmlElement `
            Суть страница матча.

        Returns
        ----------
        current_score : ` tuple of string `
            (team1, team2).
        """
        return tuple(
            map(
                lambda x: x.text_content().replace('\n', ''),
                page.find_class('team-name')))

    @staticmethod
    def get_match_datetime(page):
        """Возвращает время начала матча объектом datetime.datetime.

        Parameters
        ----------
        page : ` lxml.html.HtmlElement `
            Суть страница матча.

        Returns
        ----------
        match_datetime : ` python datetime.datetime `
            Время начала матча.
        """
        datetime_block = page.find_class('date')
        if len(datetime_block) == 0:
            print('[!] cant detect datetime for match %s' %
                MatchParser.generate_match_name(
                    MatchParser.get_teams(page)[0],
                    MatchParser.get_teams(page)[1]))
            return

        datetime_string = datetime_block[0].text_content()
        datetime_list = [i.replace(',', '') for i in datetime_string.split('\n') if len(i) > 0]
        match_datetime = pd.to_datetime('%s %s' % (
            datetime_list[1], datetime_list[-1])).to_pydatetime() + timedelta(hours=3)

        return match_datetime

    @property
    def current_score(self):
        """Текущий счёт в виде кортежа float.
        """
        if self._current_score is None:
            self._current_score = MatchParser.get_current_score(self.page)
        return self._current_score

    @property
    def score1(self):
        """Счёт первой команды. float
        """
        if self._score1 is None:
            self._score1 = self.current_score[0]
        return self._score1

    @property
    def score2(self):
        """Счёт второй команды. float
        """
        if self._score2 is None:
            self._score2 = self.current_score[1]
        return self._score2

    @property
    def team1(self):
        """Название певой команды.
        """
        if self._team1 is None:
            self._team1 = MatchParser.get_teams(self.page)[0]
        return self._team1

    @property
    def team2(self):
        """Название второй команды.
        """
        if self._team2 is None:
            self._team2 = MatchParser.get_teams(self.page)[1]
        return self._team2

    @property
    def match_datetime(self):
        """Дата и время начала матча. Python datetime.datetime.
        """
        if self._match_datetime is None:
            self._match_datetime = MatchParser.get_match_datetime(self.page)
        return self._match_datetime

    @property
    def time_shift(self):
        """Время, прошедшее с начала матча, в минутах. ` float `
        """
        if self._time_shift is None:
            self._time_shift = datetime.now() - self.match_datetime
            self._time_shift = self._time_shift.total_seconds() / 60.
        return self._time_shift

    @property
    def match_name(self):
        """Название матча. ` str `
        """
        if self._match_name is None:
            self._match_name = MatchParser.generate_match_name(self.team1, self.team2)
        return self._match_name

    @property
    def hda(self):
        """Рассчитанные вероятности исходов. ` pandas DataFrame `
        """
        if self._hda is None:
            self.get_1x2()
        return self._hda

    @property
    def hda_k(self):
        """Коэффициенты, предлагаемые букмекерами. ` pandas DataFrame `
        """
        if self._hda_k is None:
            self.get_1x2()
        return self._hda_k

    @property
    def match_table(self):
        """Сведённая воедино информация о матче. ` pandas DataFrame `
        """
        if self._match_table is None:
            self._match_table = pd.DataFrame({
                'match_name': self.match_name,
                'time_shift': self.time_shift,
                'score1': self.score1,
                'score2': self.score2}, index=[1])

            for bookie in ['Fonbet', 'LigaStavok']:
                if bookie in self.hda_k.index:
                    cols = self.hda_k.loc[bookie].index.values
                    data = self.hda_k.loc[bookie].values.reshape((-1, len(cols)))
                    self._match_table[cols] = pd.DataFrame(columns=cols, data=data, index=[1])

            cols = self.hda.columns.values
            data = self.hda.values.reshape((-1, len(cols)))
            self._match_table[cols] = pd.DataFrame(columns=cols, data=data, index=[1])

        return self._match_table

    def get_1x2(self):
        """Из self.page получает таблицу с коэффициентами букмекеров.
        Заполняет поля self.hda и self.hda_k : вероятности и коэффициенты соот-нно.

        Returns
        ----------
        k : `pandas.DataFrame`
            Таблица с коэффициентами букмекеров;
                * columns = ['k_1', 'k_x', 'k_2']
                * index = названия контор
                * data = непосредственно коэфиициенты
        """
        block = self.ratio_blocks[0]

        odds = np.array(
            list(map(
                lambda x: float(x) if len(x) > 0 else np.nan,
                [i.text_content().replace('\n', '')
                for i in block.find_class('number')[3:]]
            ))).reshape((-1, 3))

        bookies = [i.text_content().replace('\n', '') for i in block.find_class('bookmaker-name')]

        # коэффициенты
        k = pd.DataFrame(data=odds, columns=['k_1', 'k_x', 'k_2'], index=bookies[3:])
        #print(k)
        # вероятности
        p = pd.DataFrame(data=np.nanmean(odds ** -1, axis=0).reshape((1,3)), columns=['1', 'x', '2'])

        if self._hda is None: self._hda = p
        if self._hda_k is None: self._hda_k = k

        return k
