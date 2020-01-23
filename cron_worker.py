from crontab import CronTab

class CronWorker:
    """Посредник для работы с crontab
    """

    def __init__(self, usr=True):
        """
        Parameters
        ----------
        usr : `bool` or `string`
            Суть username
            В случае True создаётся для текущего пользователя
            В случае False создаётся пустой crontab
        """
        self.usr = usr
        if usr:
            self.cron = CronTab(user=self.usr)
        else:
            self.cron = CronTab()
        self._current = {
            'task': '',
            'comment': '',
            'datetime': ''
        }

    def pushTask(self, task):
        """Устанавливает текущее значение task в переданное пользователем
        Parameters
        ----------
        task : `string`
            Суть команда к исполнению
        """
        self._current['task'] = task

    def pushComment(self, comment):
        """Устанавливает текущее значение comment в переданное пользователем
        Parameters
        ----------
        comment : `string`
            Суть комментарий к команде // для осуществления поиска если потребуется
        """
        self._current['comment'] = comment

    def pushDatetime(self, datetime_):
        """Устанавливает текущее значение datetime в переданное пользователем
        Parameters
        ----------
        datetime_ : `python datetime`
            Суть время, когда команда должа быть исполнена
        """
        self._current['datetime'] = datetime_

    def deltask(self, task):
        """Удаляет task из текущей сессии; суть delTaskByComment
        Parameters
        ----------
        task : `string`
            ...
        """
        self.cron.remove_all(comment=task)

    def commit(self):
        """Допускает текущую комбинацию к записи
        """
        job = self.cron.new(
            command=self._current['task'],
            comment=self._current['comment']
        )
        job.setall(self._current['datetime'])

    def write(self):
        """Записывает все коммитнутые комбинации в файл
        """
        self.cron.write()
