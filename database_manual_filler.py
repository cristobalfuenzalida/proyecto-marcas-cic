import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
from datetime import timedelta as time

def nan_to_null(f,
        _NULL=psycopg2.extensions.AsIs('NULL'),
        _Float=psycopg2.extensions.Float):
    if not np.isnan(f):
        return _Float(f)
    return _NULL

psycopg2.extensions.register_adapter(float, nan_to_null)

data = pd.read_csv('asistencia_diciembre.csv', sep=',')
data['colacion'].replace(to_replace='0', value='00:00:00', inplace=True)
data['fecha'] = pd.to_datetime(data['fecha'], format="%d-%m-%y")

HOLGURA = time(minutes=5)
DIA_COMPLETO = 'Día completo'
CONN = psycopg2.connect(
    database    = 'asistencias_cic',
    user        = 'dyatec',
    password    = 'dyatec2023',
    host        = 'localhost',
    port        = '5432'
)

def hora_to_timedelta(hora):
    if len(hora) == 7:
        hora = '0' + hora
    return time(
        hours=int(hora[0:2]), minutes=int(hora[3:5]), seconds=int(hora[6:8])
    )

def timedelta_to_hora(tiempo):
    if tiempo.days == 0:
        hours = tiempo.seconds // 3600
        minutes = (tiempo.seconds % 3600) // 60
        seconds = tiempo.seconds % 60

        hh = f"{0 if hours<10 else ''}{hours}"
        mm = f"{0 if minutes<10 else ''}{minutes}"
        ss = f"{0 if seconds<10 else ''}{seconds}"

        return f"{hh}:{mm}:{ss}"

def timedelta_to_number(tiempo):
    return tiempo.days * 24 + tiempo.seconds / 3600

def tiempo_asignado(entrada_turno, salida_turno, colacion, noche):
    if pd.isnull(entrada_turno) or pd.isnull(salida_turno):
        return time(0)
    tiempo_entrada = hora_to_timedelta(entrada_turno)
    tiempo_salida = hora_to_timedelta(salida_turno)
    tiempo_colacion = hora_to_timedelta(colacion)

    if noche == 1:
        tiempo_diferencia = (tiempo_salida + time(hours=24)) - tiempo_entrada
    else:
        tiempo_diferencia = abs(tiempo_salida - tiempo_entrada)
    
    t_asignado = tiempo_diferencia - tiempo_colacion
    
    return time(seconds=60*round(t_asignado.seconds/60))

def tiempo_entrada_atrasada(entrada_turno, entrada_real, detalle_permiso):
    if (
        pd.isnull(entrada_turno) or pd.isnull(entrada_real) or
        detalle_permiso == DIA_COMPLETO
    ):
        return time(0)

    tiempo_entrada_turno = hora_to_timedelta(entrada_turno)
    tiempo_entrada_real = hora_to_timedelta(entrada_real)

    if tiempo_entrada_real <= (tiempo_entrada_turno + HOLGURA):
        return time(0)
    else:
        tiempo_entrada_real = time(
            seconds=60*round(tiempo_entrada_real.seconds/60)
        )
        tiempo_entrada_turno = time(
            seconds=60*round(tiempo_entrada_turno.seconds/60)
        )
        return (tiempo_entrada_real - tiempo_entrada_turno)

def tiempo_salida_anticipada(salida_turno, salida_real, detalle_permiso):
    if (
        pd.isnull(salida_turno) or pd.isnull(salida_real) or
        detalle_permiso == DIA_COMPLETO
    ):
        return time(0)
    tiempo_salida_turno = hora_to_timedelta(salida_turno)
    tiempo_salida_real = hora_to_timedelta(salida_real)

    if tiempo_salida_real >= (tiempo_salida_turno - HOLGURA):
        return time(0)
    else:
        tiempo_salida_real = time(
            seconds=60*round(tiempo_salida_real.seconds/60)
        )
        tiempo_salida_turno = time(
            seconds=60*round(tiempo_salida_turno.seconds/60)
        )
        return (tiempo_salida_turno - tiempo_salida_real)

def tiempo_efectivo(
        entrada_turno, salida_turno,
        entrada_real=np.nan, salida_real=np.nan, colacion=np.nan, noche=0,
        permiso=np.nan, detalle_permiso=np.nan
    ):
    if pd.isnull(colacion):
        colacion = '00:00:00'
    if (
        (pd.isnull(entrada_real) or pd.isnull(salida_real)) or
        (pd.isnull(entrada_turno) or pd.isnull(salida_turno)) or
        detalle_permiso == DIA_COMPLETO or
        permiso in ['licencia_medica', 'dia_vacaciones', 'falta_injustificada']
    ):
        return time(0)

    t_asignado = tiempo_asignado(entrada_turno, salida_turno, colacion, noche)
    t_atraso = tiempo_entrada_atrasada(entrada_turno, entrada_real,
                                       detalle_permiso)
    t_anticipo = tiempo_salida_anticipada(salida_turno, salida_real,
                                          detalle_permiso)

    return t_asignado - (t_atraso + t_anticipo)

def tiempo_permiso_con_goce(
        entrada_turno, salida_turno, salida_real=np.nan,
        colacion=np.nan, noche=0, permiso=np.nan, detalle_permiso=np.nan
    ):
    if permiso == 'permiso_con_goce':
        if detalle_permiso == DIA_COMPLETO:
            return tiempo_asignado(entrada_turno, salida_turno, colacion, noche)
        elif pd.notnull(detalle_permiso) and (' hrs' in detalle_permiso):
            hh_mm = detalle_permiso[:detalle_permiso.index(' hrs')].split(':')
            return time(hours=int(hh_mm[0]), minutes=int(hh_mm[1]))
        elif pd.notnull(salida_turno) and pd.notnull(salida_real):
            t_salida_turno = hora_to_timedelta(salida_turno)
            t_salida_real = hora_to_timedelta(salida_real)
            if t_salida_turno > t_salida_real:
                return (t_salida_turno - t_salida_real)
            else:
                return time(0)
        else:
            return time(0)
    else:
        return time(0)

def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print(f"Dataframe has been inserted correctly into table {table}")
    cursor.close()

def clear_table(conn, table):
    query = f"DELETE FROM {table}"
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    print(f"Table {table} has been correctly cleared.")
    cursor.close()

def fill_results(dataframe, table=None, save=False):
    horas_asign_txt     = []
    horas_asign_num     = []
    horas_asist_txt     = []
    horas_asist_num     = []
    entr_tard           = []
    sal_antic           = []
    horas_perm_con_goce = []

    for index, row in dataframe.iterrows():
        t_asignado = tiempo_asignado(
            row.entrada_turno, row.salida_turno, row.colacion, row.noche
        )
        t_asistido = tiempo_efectivo(
            row.entrada_turno, row.salida_turno,
            row.entrada_real, row.salida_real, row.colacion, row.noche,
            row.permiso, row.detalle_permiso
        )
        t_entr_tard = tiempo_entrada_atrasada(
            row.entrada_turno, row.entrada_real, row.detalle_permiso
        )
        t_sal_antic = tiempo_salida_anticipada(
            row.salida_turno, row.salida_real, row.detalle_permiso
        )
        t_perm_con_goce = tiempo_permiso_con_goce(
            row.entrada_turno, row.salida_turno, row.salida_real,
            row.colacion, row.noche, row.permiso, row.detalle_permiso
        )
        horas_asign_txt.append(timedelta_to_hora(t_asignado))
        horas_asign_num.append(timedelta_to_number(t_asignado))
        horas_asist_txt.append(timedelta_to_hora(t_asistido))
        horas_asist_num.append(timedelta_to_number(t_asistido))
        entr_tard.append(timedelta_to_number(t_entr_tard))
        sal_antic.append(timedelta_to_number(t_sal_antic))
        horas_perm_con_goce.append(timedelta_to_number(t_perm_con_goce))

    dataframe.drop(columns=[
            'nombre', 'entrada_real', 'salida_real', 
            'entrada_turno', 'salida_turno', 'colacion', 'noche'
        ], inplace=True
    )
    dataframe['horas_asign_txt']     = horas_asign_txt
    dataframe['horas_asign_num']     = horas_asign_num
    dataframe['horas_asist_txt']     = horas_asist_txt
    dataframe['horas_asist_num']     = horas_asist_num
    dataframe['entr_tard']           = entr_tard
    dataframe['sal_antic']           = sal_antic
    dataframe['horas_perm_con_goce'] = horas_perm_con_goce

    dataframe.sort_values(by=['fecha', 'turno'], inplace=True)
    if save and table:
        execute_values(CONN, dataframe, table)
    else:
        print(dataframe)

to_save = None
while to_save not in ['y', 'n']:
    to_save = input('Guardar en bdd? (y/n): ')

if to_save == 'y':
    clear_table(CONN, 'ejemplo_datos')
    clear_table(CONN, 'ejemplo_resultados')
    data.sort_values(by=['fecha', 'entrada_real'], inplace=True)
    execute_values(CONN, data, 'ejemplo_datos')
    fill_results(data, 'ejemplo_resultados', save=True)
else:
    print(data)
    fill_results(data, save=False)
    print('Not saving. Program finished.')

CONN.close()