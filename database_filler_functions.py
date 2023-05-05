import google_sheets_api as gsa
import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
import requests
import json
import datetime
from datetime import timedelta as time
from aux_functions import *
import sys

HOLGURA = time(minutes=5)
DIA_COMPLETO = 'Día completo'
QUIT_MESSAGE = "DataFrame was not inserted. Finishing program..."
DEFAULT_QUERY = "INSERT INTO %s(%s) VALUES %%s"
UPSERT_QUERY = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT (%s) DO NOTHING"
DOCUMENT_ID = '1gR7VbHlI_n8HTc2DpfghRaTtwr5KuNTAhfNx4UeXJ3M'
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


def tiempo_asignado(entrada_turno, salida_turno, colacion):
    if pd.isnull(entrada_turno) or pd.isnull(salida_turno):
        return time(0)

    tiempo_entrada = hora_to_timedelta(entrada_turno)
    tiempo_salida = hora_to_timedelta(salida_turno)
    tiempo_colacion = hora_to_timedelta(colacion)

    if entrada_turno <= salida_turno:
        tiempo_diferencia = abs(tiempo_salida - tiempo_entrada)
    else:
        tiempo_diferencia = (tiempo_salida + time(hours=24)) - tiempo_entrada
    
    t_asignado = tiempo_diferencia - tiempo_colacion
    
    return time(seconds=60*round(t_asignado.seconds/60))

'''
def tiempo_entrada_atrasada(entrada_turno, entrada_real, detalle_permiso):
    if (pd.isnull(entrada_turno) or pd.isnull(entrada_real)
            or detalle_permiso == DIA_COMPLETO):
        return time(0)

    tiempo_entrada_turno = hora_to_timedelta(entrada_turno)
    tiempo_entrada_real = hora_to_timedelta(entrada_real)

    if tiempo_entrada_real <= (tiempo_entrada_turno + HOLGURA):
        return time(0)
    else:
        # Se calcula resultado con tiempos redondeados al minuto
        tiempo_entrada_turno = time(
            seconds=60*round(tiempo_entrada_turno.seconds/60)
        )
        tiempo_entrada_real = time(
            seconds=60*round(tiempo_entrada_real.seconds/60)
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
        # Se calcula resultado con tiempos redondeados al minuto
        tiempo_salida_turno = time(
            seconds=60*round(tiempo_salida_turno.seconds/60)
        )
        tiempo_salida_real = time(
            seconds=60*round(tiempo_salida_real.seconds/60)
        )
        return (tiempo_salida_turno - tiempo_salida_real)

'''

def tiempo_efectivo(
        entrada_turno, salida_turno, entrada_real=np.nan, salida_real=np.nan,
        horas_atraso=np.nan, horas_anticipo=np.nan, colacion=np.nan,
        permiso=np.nan, detalle_permiso=np.nan):
    if ((pd.isnull(entrada_real) or pd.isnull(salida_real))
            or detalle_permiso == DIA_COMPLETO
            or permiso in PERMISOS_DIARIOS):
        return time(0)
    
    if (pd.isnull(entrada_turno) or pd.isnull(salida_turno)):
        tiempo_entrada = hora_to_timedelta(entrada_real)
        tiempo_salida = hora_to_timedelta(salida_real)
        if (entrada_real >= salida_real):
            return (tiempo_salida + time(hours=24)) - tiempo_entrada
        else:
            return abs(tiempo_salida - tiempo_entrada)

    t_asignado = tiempo_asignado(entrada_turno, salida_turno, colacion)
    t_atraso = time(hours=horas_atraso)
    t_permiso_cg = tiempo_permiso_con_goce(
        entrada_turno, salida_turno, salida_real,
        colacion, permiso, detalle_permiso
    )
    if t_permiso_cg > time(0):
        t_anticipo = time(0)
    else:
        t_anticipo = time(hours=horas_anticipo)
    return t_asignado - (t_atraso + t_anticipo + t_permiso_cg)

def tiempo_permiso_con_goce(
        entrada_turno, salida_turno, salida_real=np.nan,
        colacion=np.nan, permiso=np.nan, detalle_permiso=np.nan):
    if permiso not in ['permiso_con_goce', 'dia_administrativo']:
        return time(0)

    elif detalle_permiso == DIA_COMPLETO:
        return tiempo_asignado(entrada_turno, salida_turno, colacion)

    elif pd.notnull(detalle_permiso) and (' hrs' in detalle_permiso):
        hh_mm = detalle_permiso[:detalle_permiso.index(' hrs')].split(':')
        return time(hours=int(hh_mm[0]), minutes=int(hh_mm[1]))

    elif pd.notnull(salida_turno) and pd.notnull(salida_real):
        # Se calcula resultado con tiempos redondeados al minuto
        tiempo_salida_turno = hora_to_timedelta(salida_turno)
        tiempo_salida_real = hora_to_timedelta(salida_real)

        tiempo_salida_real = time(
            seconds=60*round(tiempo_salida_real.seconds/60)
        )
        tiempo_salida_turno = time(
            seconds=60*round(tiempo_salida_turno.seconds/60)
        )
        return (tiempo_salida_turno - tiempo_salida_real)

    else:
        return time(0)

def tiempo_permiso_sin_goce(
        entrada_turno, salida_turno, salida_real=np.nan, horas_anticipo=np.nan,
        colacion=np.nan, permiso=np.nan, detalle_permiso=np.nan):
    e_turno_str = str(entrada_turno)
    s_turno_str = str(salida_turno)

    if ((e_turno_str=='07:00:00' and s_turno_str=='17:45:00')
            or (e_turno_str=='21:15:00' and s_turno_str=='07:00:00')):
        if pd.notnull(horas_anticipo):
            return time(hours=horas_anticipo)

    if permiso not in ['permiso_sin_goce', 'falta_injustificada']:
        return time(0)

    elif detalle_permiso == DIA_COMPLETO:
        return tiempo_asignado(entrada_turno, salida_turno, colacion)

    elif pd.notnull(detalle_permiso) and (' hrs' in detalle_permiso):
        hh_mm = detalle_permiso[:detalle_permiso.index(' hrs')].split(':')
        return time(hours=int(hh_mm[0]), minutes=int(hh_mm[1]))

    elif pd.notnull(salida_turno) and pd.notnull(salida_real):
        # Se calcula resultado con tiempos redondeados al minuto
        tiempo_salida_turno = hora_to_timedelta(salida_turno)
        tiempo_salida_real = hora_to_timedelta(salida_real)

        tiempo_salida_real = time(
            seconds=60*round(tiempo_salida_real.seconds/60)
        )
        tiempo_salida_turno = time(
            seconds=60*round(tiempo_salida_turno.seconds/60)
        )
        return (tiempo_salida_turno - tiempo_salida_real)

    else:
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
    if not fecha_desde or type(fecha_desde) is not str:
        print("Value of 'fecha_desde' is either not present or not str.")
        fecha_desde = str(datetime.date.today() - time(days=31))
    clear_query = f"DELETE FROM {table_name} WHERE fecha >= '{fecha_desde}'"
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
          + f"for dates >= '{fecha_desde}'.")

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

def update_personas(dataframe, execution_mode='print'):
    personas = dataframe[['rut', 'nombre']].drop_duplicates().dropna()
    personas = personas.assign(id=np.nan)[['id', 'rut', 'nombre']]
    personas.sort_values(by=['nombre'], inplace=True)
    personas.reset_index(drop=True, inplace=True)
    columns = 'id,rut,nombre'

    cursor = CONN.cursor()
    response_api = requests.get(
        url='https://talana.com/es/api/persona/',
        auth=('integracion-empleados-asistencia-Bi@cic.cl',
              'PresenteHilarante2969#')
    )
    personas_api = json.loads(response_api.text)
    personas_ids = {per['rut'] : per['id'] for per in personas_api}
    tuples = []

    for index, row in personas.iterrows():
        if row.rut not in personas_ids:
            continue
        persona_id = int(personas_ids[row.rut])
        personas.loc[index, 'id'] = persona_id
        tuples.append((persona_id, row.rut, row.nombre))
    
    if execution_mode == 'print':
        timeout_input(5, "\nDataFrame 'personas':", None)
        print(personas)
        return 0
    
    insertion_query = UPSERT_QUERY % ('personas', columns, 'rut')
    try:
        extras.execute_values(cursor, insertion_query, tuples)
        CONN.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        CONN.rollback()
        cursor.close()
        return 1

    print("-> Table 'personas' has been updated with new values")
    cursor.close()
    return 0

def update_sucursales(dataframe, execution_mode='print'):
    sucursales = dataframe[['sucursal']].drop_duplicates().dropna()
    sucursales = sucursales.assign(id=np.nan)[['id', 'sucursal']]
    sucursales.sort_values(by=['sucursal'], inplace=True)
    sucursales.reset_index(drop=True, inplace=True)
    columns = 'id,sucursal'

    cursor = CONN.cursor()
    response_api = requests.get(
        url='https://talana.com/es/api/sucursal/',
        auth=('integracion-empleados-asistencia-Bi@cic.cl',
              'PresenteHilarante2969#')
    )
    sucursales_api = json.loads(response_api.text)
    sucursales_ids = {suc['nombre'] : suc['id'] for suc in sucursales_api}
    tuples = []

    for index, row in sucursales.iterrows():
        if row.sucursal not in sucursales_ids:
            continue
        sucursal_id = int(sucursales_ids[row.sucursal])
        sucursales.loc[index, 'id'] = sucursal_id
        tuples.append((sucursal_id, row.sucursal))

    if execution_mode == 'print':
        timeout_input(5, "\nDataFrame 'sucursales':", None)
        print(sucursales)
        return 0
    
    insertion_query = UPSERT_QUERY % ('sucursales', columns, 'sucursal')
    try:
        extras.execute_values(cursor, insertion_query, tuples)
        CONN.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        CONN.rollback()
        cursor.close()
        return 1

    print("-> Table 'sucursales' has been updated with new values")
    cursor.close()
    return 0

def update_centros_de_costo(dataframe, execution_mode='print'):
    centros_de_costo = dataframe[['centro']].drop_duplicates().dropna()
    centros_de_costo.sort_values(by=['centro'], inplace=True)
    centros_de_costo.reset_index(drop=True, inplace=True)

    tuples = [
        (row.centro,) for index, row in centros_de_costo.iterrows()
    ]
    columns = 'centro'

    if execution_mode == 'print':
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

    print("-> Table 'centros_de_costo' has been updated with new values")
    cursor.close()
    return 0

def update_turnos(dataframe, data_to_pull=None, execution_mode='print'):
    if data_to_pull == None:
        return 1
    raw_data = gsa.pull_sheet_data(DOCUMENT_ID, data_to_pull)
    turnos = pd.DataFrame(
        data=raw_data[1:], columns=raw_data[0]
    ).rename(columns={
        'Turno'             : 'turno',
        'Colacion'          : 'colacion'
    })
    turnos.drop_duplicates(inplace=True)
    turnos.reset_index(drop=True, inplace=True)
    turnos.dropna(inplace=True)
    turnos = turnos.assign(entrada_turno=np.nan, salida_turno=np.nan)[[
        'turno', 'entrada_turno', 'salida_turno', 'colacion'
    ]]
    columns = 'turno,entrada_turno,salida_turno,colacion'
    tuples = []
    for index, row in turnos.iterrows():
        horario = row.turno.split(' - ')
        entrada_turno = hora_to_time(horario[0])
        salida_turno = hora_to_time(horario[1])

        turnos.loc[index, 'entrada_turno'] = entrada_turno
        turnos.loc[index, 'salida_turno'] = salida_turno

        tuples.append((row.turno, entrada_turno, salida_turno, row.colacion))

    for index, row in dataframe.iterrows():
        colacion_filter = turnos.loc[
            (turnos.entrada_turno == row.entrada_turno)
            & (turnos.salida_turno == row.salida_turno),
            'colacion'
        ]
        if colacion_filter.empty:
            colacion = hora_to_time('00:00:00')
        else:
            colacion = colacion_filter.iat[0]
        dataframe.loc[index, 'colacion'] = colacion

    if execution_mode == 'print':
        timeout_input(5, "\nDataFrame 'turnos':", None)
        print(turnos)
        return 0
    
    cursor = CONN.cursor()
    
    insertion_query = UPSERT_QUERY % ('turnos', columns, 'turno')
    try:
        extras.execute_values(cursor, insertion_query, tuples)
        CONN.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        CONN.rollback()
        cursor.close()
        return 1

    print("-> Table 'turnos' has been updated from Google Sheets to DataBase")
    cursor.close()
    return 0

def get_id_values(row, queries, cursor):
    id_values = {
        'persona_id'    : np.nan,
        'sucursal_id'   : np.nan,
        'centro_id'     : np.nan,
        'permiso_id'    : np.nan
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
            id_values['turno_id'] = np.nan
            return id_values

    if pd.isnull(row.entrada_turno) or pd.isnull(row.salida_turno):
        id_values['turno_id'] = np.nan
        return id_values
    try:
        query = ("SELECT id FROM turnos "
                 "WHERE entrada_turno='%s' AND salida_turno='%s'")
        cursor.execute(query % (row.entrada_turno, row.salida_turno))
        response = cursor.fetchone()
        if response:
            id_values['turno_id'] = response[0]
        else:
            id_values['turno_id'] = np.nan
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        CONN.rollback()
        id_values['turno_id'] = np.nan
        return id_values

    return id_values

def fill_marks_table(dataframe, table_name=None, execution_mode='print'):
    if not table_name:
        return 1
    
    if execution_mode == 'print':
        timeout_input(5, f"\nDataFrame '{table_name}' (Python):", None)
        marks_df = dataframe[[
            'rut',
            'fecha',
            'razon_social',
            'sucursal',
            'centro',
            'entrada_real',
            'salida_real',
            'turno',
            'colacion',
            'permiso',
            'detalle_permiso'
        ]]
        print(marks_df)
        results = fill_results_dataframe(dataframe, execution_mode='print')
        columns_to_use = results.columns.difference(marks_df.columns)
        final_dataframe = pd.concat([marks_df, results[columns_to_use]],
                                    axis=1, join='inner')[[
            'rut', 'fecha', 'razon_social', 'sucursal', 'centro',
            'entrada_real', 'salida_real', 'turno', 'colacion',
            't_asignado', 't_asistido', 't_atraso', 't_anticipo',
            't_permiso_cg', 'permiso', 'detalle_permiso'
        ]]
        timeout_input(5, f"\nFull DataFrame '{table_name}' (Local):", None)
        print(final_dataframe)
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
        'permiso_id'    : {
            'prompt' : "SELECT id FROM permisos WHERE tipo='%s'",
            'field' : 'permiso'
        }
    }
    cursor = CONN.cursor()
    marks = {
        'persona_id'        : [],
        'fecha'             : [],
        'razon_social'      : [],
        'sucursal_id'       : [],
        'centro_id'         : [],
        'entrada_real'      : [],
        'salida_real'       : [],
        'entrada_turno'     : [],
        'salida_turno'      : [],
        'horas_atraso'      : [],
        'horas_anticipo'    : [],
        'colacion'          : [],
        'turno_id'          : [],
        'permiso_id'        : [],
        'permiso'           : [],
        'detalle_permiso'   : []
    }

    for index, row in dataframe.iterrows():
        id_values = get_id_values(row, queries, cursor)
        if (pd.isnull(id_values['persona_id'])
                or pd.isnull(id_values['sucursal_id'])
                or pd.isnull(id_values['centro_id'])):
            continue
        for key in marks.keys():
            if key in id_values.keys():
                marks[key].append(id_values[key])
            else:
                marks[key].append(row[key])
    marks_df = pd.DataFrame(marks)

    results = fill_results_dataframe(marks_df, execution_mode)
    columns_to_use = results.columns.difference(marks_df.columns)

    final_dataframe = pd.concat(
        [marks_df, results[columns_to_use]], axis=1, join='inner'
    )[[
        'persona_id', 'fecha',
        'razon_social', 'sucursal_id', 'centro_id',
        'entrada_real', 'salida_real', 'turno_id',
        't_asignado', 't_asistido', 't_atraso', 't_anticipo',
        't_permiso_cg', 't_permiso_sg', 'permiso_id', 'detalle_permiso'
    ]]

    option = 'y'
    while option not in ['y', 'n']:
        option = timeout_input(
            10, f"\nInsert DataFrame into table '{table_name}' (y/n)?: ", 'y')
    if option == 'y':
        insert_dataframe(final_dataframe, table_name)
    else:
        print(QUIT_MESSAGE)
        sys.exit(0)
    cursor.close()
    return 0

def fill_results_dataframe(dataframe, execution_mode='print'):
    daily_results = {
        'persona_id'        : [],
        'fecha'             : [],
        'turno_id'          : [],
        't_asignado'        : [],
        't_asistido'        : [],
        't_atraso'          : [],
        't_anticipo'        : [],
        't_permiso_cg'      : [],
        't_permiso_sg'      : []
    }

    for index, row in dataframe.iterrows():
        t_permiso_cg = tiempo_permiso_con_goce(
                row.entrada_turno, row.salida_turno, row.salida_real,
                row.colacion, row.permiso, row.detalle_permiso)
        if t_permiso_cg > time(0):
            t_anticipo = time(0)
        else:
            t_anticipo = time(hours=row.horas_anticipo)

        e_turno_str = str(row.entrada_turno)
        s_turno_str = str(row.salida_turno)

        if ((e_turno_str=='07:00:00' and s_turno_str=='17:45:00')
                or (e_turno_str=='21:15:00' and s_turno_str=='07:00:00')):
            t_anticipo = time(0)
            t_permiso_sg = time(hours=row.horas_anticipo)
        else:
            t_permiso_sg = tiempo_permiso_sin_goce(
                row.entrada_turno, row.salida_turno, row.salida_real,
                row.horas_anticipo,
                row.colacion, row.permiso, row.detalle_permiso)

        tiempos = {
            't_asignado'    : tiempo_asignado(
                row.entrada_turno, row.salida_turno, row.colacion),
            't_asistido'    : tiempo_efectivo(
                row.entrada_turno, row.salida_turno,
                row.entrada_real, row.salida_real,
                row.horas_atraso, row.horas_anticipo,
                row.colacion, row.permiso, row.detalle_permiso),
            't_atraso'      : time(hours=row.horas_atraso),
            't_anticipo'    : t_anticipo,
            't_permiso_cg'  : t_permiso_cg,
            't_permiso_sg'  : t_permiso_sg
        }
        if execution_mode == 'print':
            daily_results['persona_id'].append(row['rut'])
            daily_results['fecha'].append(row['fecha'])
            daily_results['turno_id'].append(row['turno'])
        else:
            daily_results['persona_id'].append(row['persona_id'])
            daily_results['fecha'].append(row['fecha'])
            daily_results['turno_id'].append(row['turno_id'])

        for key in tiempos.keys():
            daily_results[key].append(timedelta_to_number(tiempos[key]))

    daily_results_df = pd.DataFrame(daily_results)

    if execution_mode == 'print':
        timeout_input(5, '\nTabla de resultados:', None)
        print(daily_results_df)
        print("\nNot saving marks into database...")
    
    return daily_results_df

def get_marks_from_database():
    mark_id_query = ("""
        SELECT      mt.id, mt.entrada_real, mt.salida_real,
                    t.entrada_turno, t.salida_turno, t.colacion,
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
            'entrada_turno', 'salida_turno', 'colacion',
            'permiso', 'detalle_permiso'
        ]
    ).fillna(np.nan)
    
    return marks_df

def update_aux_tables(dataframe, execution_mode='print'):
    update_personas(dataframe, execution_mode)
    update_sucursales(dataframe, execution_mode)
    update_centros_de_costo(dataframe, execution_mode)
