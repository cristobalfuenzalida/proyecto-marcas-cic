import psycopg2
import psycopg2.extras as extras
import pandas as pd
import numpy as np
from datetime import timedelta as time
from datetime import time as timeType

def nan_to_null(f,
        _NULL=psycopg2.extensions.AsIs('NULL'),
        _Float=psycopg2.extensions.Float):
    if not np.isnan(f):
        return _Float(f)
    return _NULL

def hora_to_timedelta(hora):
    if type(hora) is timeType:
        return time(
            hours=hora.hour, minutes=hora.minute, seconds=hora.second
        )
    elif type(hora) is str:
        if len(hora) == 7:
            hora = '0' + hora
        return time(
            hours=int(hora[0:2]), minutes=int(hora[3:5]), seconds=int(hora[6:8])
        )
    else:
        return time(0)

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

def tiempo_efectivo(entrada_turno, salida_turno,
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

def tiempo_permiso_con_goce(entrada_turno, salida_turno, salida_real=np.nan,
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
        print(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 1
    print(f"-> Dataframe has been inserted correctly into table {table}")
    cursor.close()

def clear_table(conn, table, reset_index=True):
    clear_query = f"DELETE FROM {table}"
    reset_index_query = f"ALTER SEQUENCE {table}_id_seq RESTART WITH 1"
    cursor = conn.cursor()
    try:
        cursor.execute(clear_query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
        conn.rollback()
        cursor.close()
        return 1
    print(f"\nTable {table} has been correctly cleared.")

    if reset_index:
        try:
            cursor.execute(reset_index_query)
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error: {error}")
            conn.rollback()
            cursor.close()
            return 1
        print(f"Primary index of {table} has been correctly reset to 1.")

    cursor.close()

def fill_aux_tables(dataframe, tables=None, save=False):
    if tables == None or tables == []:
        return
    tables = {key: None for key in tables}

    if 'personas' in tables:
        tables['personas'] = dataframe[['rut', 'nombre']].copy()
        tables['personas'].drop_duplicates(inplace=True)
        tables['personas'].reset_index(drop=True, inplace=True)
        tables['personas'].dropna(inplace=True)
    if 'sucursales' in tables:
        tables['sucursales'] = pd.DataFrame(
            {'sucursal': dataframe['sucursal'].unique()}
        )
        tables['sucursales'].dropna(inplace=True)
    if 'centros_de_costo' in tables:
        tables['centros_de_costo'] = pd.DataFrame(
            {'centro': dataframe['centro'].unique()}
        )
        tables['centros_de_costo'].dropna(inplace=True)
    if 'turnos' in tables:
        tables['turnos'] = dataframe[[
            'turno', 'entrada_turno', 'salida_turno', 'colacion', 'noche'
        ]].copy()
        tables['turnos'].drop_duplicates(inplace=True)
        tables['turnos'].reset_index(drop=True, inplace=True)
        tables['turnos'].dropna(inplace=True)
        tables['turnos']['noche'] = tables['turnos']['noche'].astype(bool)
    if 'permisos' in tables:
        tables['permisos'] = pd.DataFrame({
            'tipo': dataframe['permiso'].unique()
        })
        tables['permisos'].dropna(inplace=True)
    if save:
        for key in tables.keys():
            option = input(f"\nContinue with table '{key}'?...")
            if option == 'x':
                break
            else:
                execute_values(CONN, tables[key], key)
    else:
        for key in tables.keys():
            input(f"\nTabla '{key}':")
            print(tables[key])

def fill_marks_table(dataframe, table=None, save=False):
    queries = {
        'persona_id'    : {
            'query' : "SELECT id FROM personas WHERE rut='%s'",
            'field' : 'rut'
        },
        'sucursal_id'   : {
            'query' : "SELECT id FROM sucursales WHERE sucursal='%s'",
            'field' : 'sucursal'
        },
        'centro_id'     : {
            'query' : "SELECT id FROM centros_de_costo WHERE centro='%s'",
            'field' : 'centro'
        },
        'turno_id'      : {
            'query' : "SELECT id FROM turnos WHERE turno='%s'",
            'field' : 'turno'
        },
        'permiso_id'    : {
            'query' : "SELECT id FROM permisos WHERE tipo='%s'",
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

    input(f"\nStart filling table '{table}'?...")
    for index, row in dataframe.iterrows():
        id_values = get_id_values(row, queries, cursor)
        if 0 in id_values.values():
            continue
        else:
            for key in marks.keys():
                if key in queries.keys():
                    marks[key].append(id_values[key])
                else:
                    marks[key].append(row[key])
    marks_df = pd.DataFrame(marks)

    if save:
        execute_values(CONN, marks_df, table)
    else:
        input('\nTabla de marcas (Python):')
        print(marks_df)
        print("\nNot saving marks into database...")
    cursor.close()

def get_id_values(row, queries, cursor):
    id_values = {
        'persona_id'    : 0,
        'sucursal_id'   : 0,
        'centro_id'     : 0,
        'turno_id'      : 0,
        'permiso_id'    : 0
    }
    for key in queries.keys():
        try:
            cursor.execute(queries[key]['query'] % row[queries[key]['field']])
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

def fill_results(table=None, save=False):

    input(f"\nStart filling table '{table}'?...")

    dataframe = get_marks_as_dataframe()

    daily_results = {
        'marca_turno_id'    : [],
        't_asignado'        : [],
        't_asistido'        : [],
        't_atraso'          : [],
        't_anticipo'        : [],
        't_permiso_cg'      : []
    }

    for index, row in dataframe.iterrows():
        t_asignado = tiempo_asignado(
            row.entrada_turno, row.salida_turno, row.colacion, row.noche
        )
        t_asistido = tiempo_efectivo(
            row.entrada_turno, row.salida_turno,
            row.entrada_real, row.salida_real, row.colacion, row.noche,
            row.permiso, row.detalle_permiso
        )
        t_atraso = tiempo_entrada_atrasada(
            row.entrada_turno, row.entrada_real, row.detalle_permiso
        )
        t_anticipo = tiempo_salida_anticipada(
            row.salida_turno, row.salida_real, row.detalle_permiso
        )
        t_permiso_cg = tiempo_permiso_con_goce(
            row.entrada_turno, row.salida_turno, row.salida_real,
            row.colacion, row.noche, row.permiso, row.detalle_permiso
        )
        daily_results['marca_turno_id'].append(row['marca_turno_id'])
        daily_results['t_asignado'].append(timedelta_to_number(t_asignado))
        daily_results['t_asistido'].append(timedelta_to_number(t_asistido))
        daily_results['t_atraso'].append(timedelta_to_number(t_atraso))
        daily_results['t_anticipo'].append(timedelta_to_number(t_anticipo))
        daily_results['t_permiso_cg'].append(timedelta_to_number(t_permiso_cg))

    daily_results_df = pd.DataFrame(daily_results)

    if save:
        execute_values(CONN, daily_results_df, table)
    else:
        input('\nTabla de marcas (PostgreSQL):')
        print(dataframe)
        input('\nTabla de resultados:')
        print(daily_results_df)
        print("\nNot saving marks into database...")

def get_marks_as_dataframe():
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

    marks_df = pd.DataFrame(marks_tuples, columns=[
        'marca_turno_id', 'entrada_real', 'salida_real',
        'entrada_turno', 'salida_turno', 'colacion', 'noche',
        'permiso', 'detalle_permiso'
    ]).fillna(np.nan)
    return marks_df

#!------------------------------------------------------------------------------

psycopg2.extensions.register_adapter(float, nan_to_null)

DATA = pd.read_csv('asistencias_enero_2023_(proved).csv', sep=',')[[
        'Rut', 'Nombre', 'Sucursal', 'Centro de costo', 'Fecha',
        'Entrada real', 'Salida real', 'Entrada turno', 'Salida turno',
        'Turno', 'bono_noche', 'Permiso', 'Detalle permisos'
    ]].rename(columns={
        'Rut'               : 'rut',
        'Nombre'            : 'nombre',
        'Sucursal'          : 'sucursal',
        'Centro de costo'   : 'centro',
        'Fecha'             : 'fecha',
        'Entrada real'      : 'entrada_real',
        'Salida real'       : 'salida_real',
        'Entrada turno'     : 'entrada_turno',
        'Salida turno'      : 'salida_turno',
        'Turno'             : 'turno',
        'bono_noche'        : 'noche',
        'Permiso'           : 'permiso',
        'Detalle permisos'  : 'detalle_permiso'
    }).assign(colacion='00:45:00')

HOLGURA = time(minutes=5)
DIA_COMPLETO = 'Día completo'

TABLES = ['personas', 'sucursales', 'centros_de_costo', 'turnos', 'permisos']

CONN = psycopg2.connect(
    database    = 'asistencias_cic',
    user        = 'dyatec',
    password    = 'dyatec2023',
    host        = 'localhost',
    port        = '5432'
)

print_mode = None
while print_mode not in ['y', 'n']:
    print_mode = input('Guardar en bdd? (y/n): ')
if print_mode == 'y':
    DATA.sort_values(by=['fecha', 'entrada_real'], inplace=True)

    clear_table(CONN, 'datos_calculados', reset_index=False)
    clear_table(CONN, 'marcas_turnos')
    for table in TABLES:
        clear_table(CONN, table)

    fill_aux_tables(DATA, TABLES, save=True)
    fill_marks_table(DATA, 'marcas_turnos', save=True)
    fill_results('datos_calculados', save=True)
else:
    print(DATA, '\n')
    fill_aux_tables(DATA, TABLES, save=False)
    fill_marks_table(DATA, 'marcas_turnos', save=False)
    fill_results('datos_calculados', save=False)
    print('Not saved. Program finished.')

CONN.close()