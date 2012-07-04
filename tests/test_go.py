

import datetime

import tfd.go


def test_guess_latest_full_release():
    assert '2011-12-01' == tfd.go.guess_latest_release(datetime.date(2012, 01, 15))
    assert '2012-06-01' == tfd.go.guess_latest_release(datetime.date(2012, 07, 03))


