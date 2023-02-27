from datetime import timedelta as time
from datetime import time as timeType
import select
import sys

RAZONES_SOCIALES = ['CIC RETAIL SPA', 'COMPAÑIAS CIC S.A.']

def timeout_input(timeout, prompt="", timeout_value=None):
    sys.stdout.write(prompt)
    sys.stdout.flush()
    ready, _, _ = select.select([sys.stdin], [], [], timeout)
    if ready:
        return sys.stdin.readline().rstrip('\n')
    else:
        sys.stdout.write('\n')
        sys.stdout.flush()
        return timeout_value

def hora_to_timedelta(hora):
    if type(hora) is timeType:
        return time(
            hours=hora.hour, minutes=hora.minute, seconds=hora.second
        )
    elif type(hora) is str:
        if hora == '':
            return time(0)
        elif len(hora) == 7 and hora[1] == ':':
            hora = '0' + hora
        return time(
            hours=int(hora[0:2]), minutes=int(hora[3:5]), seconds=int(hora[6:8])
        )
    else:
        return time(0)

def hora_to_time(hora):
    if type(hora) is timeType:
        return hora
    elif type(hora) is str:
        if hora == '':
            return timeType(hour=0, minute=0, second=0)
        elif len(hora) == 7 and hora[1] == ':':
            hora = '0' + hora

        return timeType(
            hour=int(hora[0:2]), minute=int(hora[3:5]), second=int(hora[6:8])
        )
    else:
        return timeType(hour=0, minute=0, second=0)

def timedelta_to_hora(tiempo):
    if type(tiempo) is not time:
        return '00:00:00'

    hours = tiempo.seconds // 3600
    minutes = (tiempo.seconds % 3600) // 60
    seconds = tiempo.seconds % 60

    hh = f"{0 if hours<10 else ''}{hours}"
    mm = f"{0 if minutes<10 else ''}{minutes}"
    ss = f"{0 if seconds<10 else ''}{seconds}"

    return f"{hh}:{mm}:{ss}"

def timedelta_to_number(tiempo):
    if type(tiempo) is time:
        return (tiempo.days * 24 + tiempo.seconds / 3600)
    else:
        return float(0)
