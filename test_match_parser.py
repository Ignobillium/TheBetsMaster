from datetime import datetime, timedelta

import pytest
import pandas as pd
import numpy as np

from match_parser import MatchParser
from _config import config


_fnames = (
            'match1',
            'live_match1',
            'live_match2',
            'live_match3',
            'live_match4')


def get_data(fname):
    with open('data/for_tests/%s' % fname, 'r') as f:
        return f.read()


class TestMatchParser:
    mp = list(MatchParser(get_data('%s.html' % fname)) for fname in _fnames)

    def test_match_name(self):
        matches = (
            MatchParser.generate_match_name('Аталанта', 'СПАЛ'),
            MatchParser.generate_match_name('Хапоэль Рамат-Ган', 'F.C. Kafr Qasim'),
            MatchParser.generate_match_name('HAPOEL BEER SHEVA', 'ХАПОЭЛЬ ХАЙФА'),
            MatchParser.generate_match_name('ХАПОЭЛЬ АФУЛА', 'ХАПОЭЛЬ АШКЕЛОН'),
            MatchParser.generate_match_name('SBV EXCELSIOR (YOUTH)', 'ЙОНГ ТВЕНТЕ (МОЛ.)'))

        for i, match in enumerate(matches):
            assert TestMatchParser.mp[i].match_name.lower() == match.lower(),\
                'Match name <%s>\t!=\tresult <%s>' % (
                TestMatchParser.mp[i].match_name,
                match)

    def test_team1(self):
        teams = (
            'Аталанта',
            'Хапоэль Рамат-Ган',
            'HAPOEL BEER SHEVA',
            'ХАПОЭЛЬ АФУЛА',
            'SBV EXCELSIOR (YOUTH)')

        for i, team in enumerate(teams):
            assert TestMatchParser.mp[i].team1.lower() == team.lower(), \
                'Result <%s> != team_name <%s>' % (
                TestMatchParser.mp[i].team1,
                team)

    def test_team2(self):
        teams = (
            'СПАЛ',
            'F.C. Kafr Qasim',
            'ХАПОЭЛЬ ХАЙФА',
            'ХАПОЭЛЬ АШКЕЛОН',
            'ЙОНГ ТВЕНТЕ (МОЛ.)')

        for i, team in enumerate(teams):
            assert TestMatchParser.mp[i].team2.lower() == team.lower(),\
                'Result <%s> != team_name <%s>' % (
                TestMatchParser.mp[i].team2,
                team)

    def test_current_score(self):
        current_scores = (
            (None, None),
            (0, 0),
            (0, 0),
            (1, 1),
            (1, 1))

        for i, current_score in enumerate(current_scores):
            assert TestMatchParser.mp[i].current_score == current_score, 'Result <%s> != current_score <%s>' % (
                TestMatchParser.mp[i].current_score,
                current_score)

    def test_score1(self):
        scores = (
            None,
            0,
            0,
            1,
            1)

        for i, score in enumerate(scores):
            assert TestMatchParser.mp[i].score1 == score, 'Result <%s> != score1 <%s>' % (
                TestMatchParser.mp[i].score1,
                score)

    def test_score2(self):
        scores = (
            None,
            0,
            0,
            1,
            1)

        for i, score in enumerate(scores):
            assert TestMatchParser.mp[i].score2 == score, 'Result <%s> != score1 <%s>' % (
                TestMatchParser.mp[i].score2,
                score)

    def test_match_datetime(self):
        datetimes = (
            datetime.strptime('20 Jan 2020, 19:45', '%d %b %Y, %H:%M') + timedelta(hours=3),
            datetime.strptime('20 Jan 2020, 17:00', '%d %b %Y, %H:%M') + timedelta(hours=3),
            datetime.strptime('20 Jan 2020, 18:00', '%d %b %Y, %H:%M') + timedelta(hours=3),
            datetime.strptime('20 Jan 2020, 17:00', '%d %b %Y, %H:%M') + timedelta(hours=3),
            datetime.strptime('20 Jan 2020, 17:30', '%d %b %Y, %H:%M') + timedelta(hours=3)
        )

        for i, datetime_ in enumerate(datetimes):
            assert TestMatchParser.mp[i].match_datetime == datetime_, 'Result <%s> != datetime <%s>' % (
                TestMatchParser.mp[i].match_datetime,
                datetime_)

    def test_hda_k(self):
        odds = (
            np.array((1.22, 7.3, 14.5)),
            np.array(()), # k for live_match1 is unavailable to check
            np.array(()), # k for live_match2 is unavailable to check
            np.array((3.6, 1.65, 5.0)),
            np.array((3.25, 3.1, 2.25))
        )

        for i in (0,3,4):
            assert (TestMatchParser.mp[i].hda_k.loc['Fonbet'].values == odds[i]).all(),\
                'Result <%s> != hda_k <%s>' % (
                TestMatchParser.mp[i].hda_k.loc['Fonbet'].values,
                odds[i])

    def test_incomplete_data(self):
        incomplete_data = get_data('incomplete_data_page.html')

        mp = MatchParser(incomplete_data)
        mt = mp.match_table

        assert (mp.hda.columns.values == np.array(['x', '2'])).all()
        assert mp.hda_k.loc['LigaStavok']['k_x'] == 13.5
        assert mp.hda_k.loc['Skybet']['k_2'] == 101.
