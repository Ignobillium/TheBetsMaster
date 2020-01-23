import pytest
from crontab import CronTab

from cron_worker import CronWorker

def test_commit():
    task = 'echo "test_commit 1, crontab_worker"'
    time = '* * * * *'
    comment = 'testing crontab_worker commit function'

    cron = CronWorker()
    cron.pushTask(task)
    cron.pushComment(comment)
    cron.pushDatetime(time)
    cron.commit()

    assert '%s %s # %s' % (time, task, comment) in map(str, list(cron.cron.find_comment(comment)))

    task = 'echo "test_commit 2, crontab_worker"'
    cron.pushTask(task)
    cron.commit()

    assert '%s %s # %s' % (time, task, comment) in map(str, list(cron.cron.find_comment(comment)))

def test_write():
    task = 'echo "test_write, crontab_worker"'
    time = '* * * * *'
    comment = 'testing crontab_worker write function'

    cron = CronWorker()
    cron.pushTask(task)
    cron.pushComment(comment)
    cron.pushDatetime(time)
    cron.commit()
    cron.write()
    del cron

    cron = CronWorker()
    assert '%s %s # %s' % (time, task, comment) in map(str, list(cron.cron.find_comment(comment)))

    cron.deltask(comment)
    cron.write()

def test_deltask():
    task = 'echo "test_deltask, crontab_worker"'
    time = '* * * * *'
    comment = 'testing crontab_worker commit function'

    cron = CronWorker()
    cron.pushTask(task)
    cron.pushComment(comment)
    cron.pushDatetime(time)
    cron.commit()
    cron.deltask(comment)
    assert '%s %s # %s' % (time, task, comment) not in map(str, list(cron.cron.find_comment(comment)))
