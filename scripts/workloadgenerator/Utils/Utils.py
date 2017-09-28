from datetime import timedelta, datetime


class Utils(object):

    @staticmethod
    def iso8601(value):
        # split seconds to larger units
        seconds = value.total_seconds()
        minutes, seconds = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        days, hours = divmod(hours, 24)
        days, hours, minutes = map(int, (days, hours, minutes))
        seconds = round(seconds, 6)
    
        ## build date
        date = ''
        if days:
            date = '%sD' % days
    
        ## build time
        time = u'T'
        # hours
        bigger_exists = date or hours
        if bigger_exists:
            time += '{:02}H'.format(hours)
        # minutes
        bigger_exists = bigger_exists or minutes
        if bigger_exists:
          time += '{:02}M'.format(minutes)
        # seconds
        if seconds > 0:
            if seconds.is_integer():
                seconds = '{:02}'.format(int(seconds))
            else:
                # 9 chars long w/leading 0, 6 digits after decimal
                seconds = '%09.6f' % seconds
            # remove trailing zeros
            seconds = seconds.rstrip('0')
            time += '{}S'.format(seconds)
        return u'P' + date + time

    @staticmethod
    def round_time(dt=None, date_delta=timedelta(minutes=1), to='up'):
        """
        Round a datetime object to a multiple of a timedelta
        dt : datetime.datetime object, default now.
        dateDelta : timedelta object, we round to a multiple of this, default 1 minute.
        from:  http://stackoverflow.com/questions/3463930/how-to-round-the-minute-of-a-datetime-object-python
        """
        round_to = date_delta.total_seconds()
    
        if dt is None:
            dt = datetime.now()
        seconds = (dt - dt.min).seconds
    
        if to == 'up':
            # // is a floor division, not a comment on following line (like in javascript):
            rounding = (seconds + round_to) // round_to * round_to
        elif to == 'down':
            rounding = seconds // round_to * round_to
        else:
            rounding = (seconds + round_to / 2) // round_to * round_to
    
        return dt + timedelta(0, rounding - seconds, -dt.microsecond)
