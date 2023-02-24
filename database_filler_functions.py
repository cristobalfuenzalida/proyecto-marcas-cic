import google_sheets_api as gsa
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
import requests
import json
from datetime import timedelta as time
from datetime import time as timeType
from datetime import date
from aux_functions import timeout_input
import sys

HOLGURA = time(minutes=5)
DIA_COMPLETO = 'Día completo'
QUIT_MESSAGE = "DataFrame was not inserted. Finishing program..."
DEFAULT_QUERY = "INSERT INTO %s(%s) VALUES %%s"
UPSERT_QUERY = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT (%s) DO NOTHING"
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1gR7VbHlI_n8HTc2DpfghRaTtwr5KuNTAhfNx4UeXJ3M'
PERMISOS_DIARIOS = [
    'licencia_medica',
    'licencia_maternal',
    'dia_vacaciones',
    'dia_administrativo',
    'falta_injustificada'
]

CONN = psycopg2.connect(
    database    = 'asistencias_cic',
    user        = 'dyatec',
    password    = 'dyatec2023',
    host        = 'localhost',
    port        = '5432'
)

def nan_to_null(f,
        _null=psycopg2.extensions.AsIs('NULL'),
        _float=psycopg2.extensions.Float):
    if not np.isnan(f):
        return _float(f)
    return _null

psycopg2.extensions.register_adapter(float, nan_to_null)

def hora_to_timedelta(hora):
    if type(hora) is timeType:
        return time(
            hours=hora.hour, minutes=hora.minute, seconds=hora.second
        )
    elif type(hora) is str:
        if hora == '':
            return time(0)
        elif len(hora) == 7:
            hora = '0' + hora
        return time(
            hours=int(hora[0:2]), minutes=int(hora[3:5]), seconds=int(hora[6:8])
        )
    else:
        return time(0)

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
    if (pd.isnull(entrada_turno) or pd.isnull(entrada_real)
            or detalle_permiso == DIA_COMPLETO):
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
    if (pd.isnull(salida_turno) or pd.isnull(salida_real)
            or detalle_permiso == DIA_COMPLETO):
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
        entrada_turno, salida_turno, entrada_real=np.nan, salida_real=np.nan,
        colacion=np.nan, noche=0, permiso=np.nan, detalle_permiso=np.nan):
    if ((pd.isnull(entrada_real) or pd.isnull(salida_real))
            or (pd.isnull(entrada_turno) or pd.isnull(salida_turno))
            or detalle_permiso == DIA_COMPLETO
            or permiso in PERMISOS_DIARIOS):
        return time(0)

    t_asignado = tiempo_asignado(entrada_turno, salida_turno, colacion, noche)
    t_atraso = tiempo_entrada_atrasada(entrada_turno, entrada_real,
                                       detalle_permiso)
    t_anticipo = tiempo_salida_anticipada(salida_turno, salida_real,
                                          detalle_permiso)

    return t_asignado - (t_atraso + t_anticipo)

def tiempo_permiso_con_goce(
        entrada_turno, salida_turno, salida_real=np.nan,
        colacion=np.nan, noche=0, permiso=np.nan, detalle_permiso=np.nan):
    if permiso not in ['permiso_con_goce', 'dia_administrativo']:
        return time(0)
    elif detalle_permiso == DIA_COMPLETO:
        return tiempo_asignado(
            entrada_turno, salida_turno, colacion, noche)
    elif pd.notnull(detalle_permiso) and (' hrs' in detalle_permiso):
        hh_mm = detalle_permiso[:detalle_permiso.index(' hrs')].split(':')
        return time(hours=int(hh_mm[0]), minutes=int(hh_mm[1]))
    elif pd.notnull(salida_turno) and pd.notnull(salida_real):
        t_salida_turno = hora_to_timedelta(salida_turno)
        t_salida_real = hora_to_timedelta(salida_real)
        if t_salida_turno > t_salida_real:
            return (t_salida_turno - t_salida_real)

    return time(0)

def insert_dataframe(dataframe, table_name=None):
    if not table_name:
        return 1
    tuples = [tuple(row) for row in dataframe.to_numpy()]
    columns = ','.join(list(dataframe.columns))

    # SQL query to execute
    insertion_query = DEFAULT_QUERY % (table_name, columns)
    cursor = CONN.cursor()
    try:
        extras.execute_values(cursor, insertion_query, tuples)
        CONN.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        CONN.rollback()
        cursor.close()
        return 1
    print(f"-> Dataframe has been inserted correctly into table {table_name}")
    cursor.close()
    return 0

def clear_marks(table_name, fecha_desde=None):
    if not fecha_desde:
        fecha_desde = str(date.today() - time(days=31))
    clear_query = f"DELETE FROM {table_name} WHERE fecha >= {fecha_desde}"
    set_index_to_max = (
        f"SELECT setval('{table_name}_id_seq',"
        + f"COALESCE((SELECT MAX(id)+1 FROM {table_name}), 1), FALSE)"
    )
    cursor = CONN.cursor()
    try:
        cursor.execute(clear_query)
        CONN.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        CONN.rollback()
        cursor.close()
        return 1
    print(f"\nData from table {table_name} has been correctly cleared "
          + f"for dates >= {fecha_desde}.")

    try:
        cursor.execute(set_index_to_max)
        CONN.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        CONN.rollback()
        cursor.close()
        return 1
    print(f"Primary index of {table_name} has been correctly reset to max+1.")
    cursor.close()
    return 0

def update_personas(dataframe, print_mode=True):
    personas = dataframe[['rut', 'nombre']]
    personas.drop_duplicates(inplace=True)
    personas.reset_index(drop=True, inplace=True)
    columns = 'rut,nombre'

    if print_mode:
        timeout_input(5, "\nDataFrame 'personas':", None)
        print(personas)
        return 0

    cursor = CONN.cursor()
    tuples = [
        (row.rut, row.nombre) for i, row in personas.iterrows()
    ]

    insertion_query = UPSERT_QUERY % ('personas', columns, 'rut')
    try:
        extras.execute_values(cursor, insertion_query, tuples)
        CONN.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        CONN.rollback()
        cursor.close()
        return 1

    print(f"-> Table 'personas' has been updated with new values")
    cursor.close()
    return 0

def update_sucursales(dataframe, print_mode=True):
    sucursales = dataframe.assign(id=np.nan)[['id', 'sucursal']]
    sucursales.drop_duplicates(inplace=True)
    sucursales.reset_index(drop=True, inplace=True)
    sucursales.dropna(inplace=True)
    columns = 'id,sucursal'

    if print_mode:
        timeout_input(5, "\nDataFrame 'sucursales':", None)
        print(sucursales)
        return 0

    cursor = CONN.cursor()
    tuples = []

    for index, row in sucursales.iterrows():
        response_api = requests.get(
            url=f'https://talana.com/es/api/sucursal/?nombre={row.sucursal}',
            auth=('integracion-empleados-asistencia-Bi@cic.cl',
                  'PresenteHilarante2969#')
        )
        sucursal_api = json.loads(response_api.text)
        sucursales.loc[index, 'id'] = int(sucursal_api[0]['id'])
        tuples.append((row.id, row.sucursal))
    
    insertion_query = UPSERT_QUERY % ('sucursales', columns, 'sucursal')
    try:
        extras.execute_values(cursor, insertion_query, tuples)
        CONN.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        CONN.rollback()
        cursor.close()
        return 1

    print(f"-> Table 'sucursales' has been updated with new values")
    cursor.close()
    return 0

def update_centros_de_costo(dataframe, print_mode=True):
    centros_de_costo = dataframe['centro'].assign(id=np.nan)[['id', 'centro']]
    centros_de_costo.drop_duplicates(inplace=True)
    centros_de_costo.reset_index(drop=True, inplace=True)
    centros_de_costo.dropna(inplace=True)
    tuples = [
        (row.centro,) for index, row in centros_de_costo.iterrows()
    ]
    columns = 'centro'

    if print_mode:
        timeout_input(5, "\nDataFrame 'centros_de_costo':", None)
        print(centros_de_costo)
        return 0

    cursor = CONN.cursor()
    insertion_query = UPSERT_QUERY % ('centros_de_costo', columns, 'centro')
    try:
        extras.execute_values(cursor, insertion_query, tuples)
        CONN.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        CONN.rollback()
        cursor.close()
        return 1

    print(f"-> Table 'centros_de_costo' has been updated with new values")
    cursor.close()
    return 0

#TODO enlazar a Google Sheets
def update_turnos(print_mode=True):
    data_to_pull = 'turnos!A1:E200'
    data = gsa.pull_sheet_data(SCOPES, SPREADSHEET_ID, data_to_pull)
    turnos = pd.DataFrame(data[1:], columns=data[0]).rename(columns={
        'Id'                : 'id',
        'Turno'             : 'turno',
        'Entrada turno'     : 'entrada_turno',
        'Salida turno'      : 'salida_turno',
        'Colacion'          : 'colacion'
    })
    turnos.drop_duplicates(inplace=True)
    turnos.reset_index(drop=True, inplace=True)
    turnos.dropna(inplace=True)
    columns = 'id,turno,entrada_turno,salida_turno,colacion'
    
    if print_mode:
        timeout_input(5, "\nDataFrame 'turnos':", None)
        print(turnos)
        return 0
    
    cursor = CONN.cursor()
    tuples = []

    for index, row in turnos.iterrows():
        tuples.append((row.id, row.turno, row.entrada_turno,
                       row.salida_turno, row.colacion))
    
    insertion_query = UPSERT_QUERY % ('turnos', columns, 'turno')
    try:
        extras.execute_values(cursor, insertion_query, tuples)
        CONN.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        CONN.rollback()
        cursor.close()
        return 1

    print(f"-> Table 'turnos' has been updated with new values")
    cursor.close()
    return 0

def fill_marks_table(dataframe, table_name=None, print_mode=True):
    if not table_name:
        return 1
    
    if print_mode:
        timeout_input(5, f"\nDataFrame '{table_name}' (Python):", None)
        data = get_marks_from_dataframe(dataframe)
        print(data)
        return 0

    queries = {
        'persona_id'    : {
            'prompt' : "SELECT id FROM personas WHERE rut='%s'",
            'field' : 'rut'
        },
        'sucursal_id'   : {
            'prompt' : "SELECT id FROM sucursales WHERE sucursal='%s'",
            'field' : 'sucursal'
        },
        'centro_id'     : {
            'prompt' : "SELECT id FROM centros_de_costo WHERE centro='%s'",
            'field' : 'centro'
        },
        'turno_id'      : {
            'prompt' : "SELECT id FROM turnos WHERE turno='%s'",
            'field' : 'turno'
        },
        'permiso_id'    : {
            'prompt' : "SELECT id FROM permisos WHERE tipo='%s'",
            'field' : 'permiso'
        }
    }
    cursor = CONN.cursor()
    marks = {
        'persona_id'        : [],
        'fecha'             : [],
        'sucursal_id'       : [],
        'centro_id'         : [],
        'entrada_real'      : [],
        'salida_real'       : [],
        'turno_id'          : [],
        'permiso_id'        : [],
        'detalle_permiso'   : []
    }

    for index, row in dataframe.iterrows():
        id_values = get_id_values(row, queries, cursor)
        if (pd.isnull(id_values['persona_id'])
                or pd.isnull(id_values['sucursal_id'])
                or pd.isnull(id_values['centro_id'])):
            continue
        for key in marks.keys():
            if key in queries.keys():
                marks[key].append(id_values[key])
            else:
                marks[key].append(row[key])
    marks_df = pd.DataFrame(marks)

    #! Change to option = None to allow user choice
    option = 'y'
    while option not in ['y', 'n']:
        option = timeout_input(
            10, f"\nInsert DataFrame into table '{table_name}' (y/n)?: ", 'n')
    if option == 'y':
        insert_dataframe(marks_df, table_name)
    else:
        print(QUIT_MESSAGE)
        sys.exit(0)
    cursor.close()
    return 0

def get_id_values(row, queries, cursor):
    id_values = {
        'persona_id'    : 0,
        'sucursal_id'   : 0,
        'centro_id'     : 0,
        'turno_id'      : 0,
        'permiso_id'    : 0
    }
    for key in id_values.keys():
        try:
            cursor.execute(queries[key]['prompt'] % row[queries[key]['field']])
            response = cursor.fetchone()
            if response:
                id_values[key] = response[0]
            else:
                id_values[key] = np.nan
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
            CONN.rollback()
            return id_values

    return id_values

def fill_results_table(dataframe, table_name=None, print_mode=True):
    if print_mode:
        data = get_marks_from_dataframe(dataframe)
    else:
        data = get_marks_from_database()

    daily_results = {
        'marca_turno_id'    : [],
        't_asignado'        : [],
        't_asistido'        : [],
        't_atraso'          : [],
        't_anticipo'        : [],
        't_permiso_cg'      : []
    }

    for index, row in data.iterrows():
        tiempos = {
            't_asignado'    : tiempo_asignado(
                row.entrada_turno, row.salida_turno, row.colacion, row.noche),
            't_asistido'    : tiempo_efectivo(
                row.entrada_turno, row.salida_turno,
                row.entrada_real, row.salida_real, row.colacion, row.noche,
                row.permiso, row.detalle_permiso),
            't_atraso'      : tiempo_entrada_atrasada(
                row.entrada_turno, row.entrada_real, row.detalle_permiso),
            't_anticipo'    : tiempo_salida_anticipada(
                row.salida_turno, row.salida_real, row.detalle_permiso),
            't_permiso_cg'  : tiempo_permiso_con_goce(
                row.entrada_turno, row.salida_turno, row.salida_real,
                row.colacion, row.noche, row.permiso, row.detalle_permiso),
        }
        if print_mode:
            daily_results['marca_turno_id'].append(row['rut'])
        else:
            daily_results['marca_turno_id'].append(row['marca_turno_id'])

        for key in tiempos.keys():
            daily_results[key].append(timedelta_to_number(tiempos[key]))

    daily_results_df = pd.DataFrame(daily_results)

    if print_mode:
        timeout_input(5, '\nTabla de resultados:', None)
        print(daily_results_df)
        print("\nNot saving marks into database...")
        return 0
    
    #! Change to option = None to allow user choice
    option = 'y'
    while option not in ['y', 'n']:
        option = timeout_input(
            10, f"\nInsert DataFrame into table '{table_name}' (y/n)?: ", 'n')
    if option == 'y':
        return insert_dataframe(daily_results_df, table_name)
    else:
        print(QUIT_MESSAGE)
        sys.exit(0)

def get_marks_from_database():
    mark_id_query = ("""
        SELECT      mt.id, mt.entrada_real, mt.salida_real,
                    t.entrada_turno, t.salida_turno, t.colacion, t.noche,
                    p.tipo, mt.detalle_permiso
        FROM        marcas_turnos       AS mt
        LEFT JOIN   turnos              AS t
        ON          mt.turno_id         = t.id
        LEFT JOIN   permisos            AS p
        ON          mt.permiso_id       = p.id
    """)
    cursor = CONN.cursor()

    try:
        cursor.execute(mark_id_query)
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        CONN.rollback()
        return pd.DataFrame()
    
    marks_tuples = cursor.fetchall()
    cursor.close()

    marks_df = pd.DataFrame(
        marks_tuples, 
        columns=[
            'marca_turno_id', 'entrada_real', 'salida_real',
            'entrada_turno', 'salida_turno', 'colacion', 'noche',
            'permiso', 'detalle_permiso'
        ]
    ).fillna(np.nan)
    
    return marks_df

def get_marks_from_dataframe(dataframe):
    return dataframe[[
        'rut',
        'entrada_real',
        'salida_real',
        'entrada_turno',
        'salida_turno',
        'colacion',
        'noche',
        'permiso',
        'detalle_permiso'
    ]]