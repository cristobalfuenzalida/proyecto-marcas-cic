import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras
from datetime import date

CONTRATOS_FILENAME = 'contratos_2023-05-10.json'

DEFAULT_QUERY = "INSERT INTO %s(%s) VALUES %%s"
UPSERT_QUERY = ("INSERT INTO %s(%s) VALUES %%s "
                "ON CONFLICT (%s) DO UPDATE "
                "SET ")
RAZONES_SOCIALES = {
    9415    : 'CIC RETAIL SPA',
    9414    : 'COMPAÑIAS CIC S.A.',
    9425    : 'Externos'
}
TODAY = date.today()

CONN = psycopg2.connect(
    database    = 'dotacion_vigente',
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

def update_personas(dataframe):
    personas_dict = {
        'persona_id'    : [],
        'rut'           : [],
        'nombre'        : [],
    }
    for index, row in dataframe.iterrows():
        if pd.notnull(row['empleadoDetails']['id']):
            personas_dict['persona_id'].append(row['empleadoDetails']['id'])
            personas_dict['rut'].append(row['empleadoDetails']['rut'])
            nombres = row['empleadoDetails']['nombre'].split(' ')
            while '' in nombres:
                nombres.remove('')
            nombre_completo = ''
            for n in nombres:
                nombre_completo += n[0].upper() + n[1:].lower() + ' '
            apellido_caps = row['empleadoDetails']['apellidoPaterno']
            apellido = apellido_caps[0].upper() + apellido_caps[1:].lower()
            nombre_completo += apellido
            personas_dict['nombre'].append(nombre_completo)

    personas = pd.DataFrame(personas_dict).drop_duplicates().dropna()
    personas.sort_values(by=['nombre'], inplace=True)
    personas.reset_index(drop=True, inplace=True)
    columns = 'id,rut,nombre'
    tuples = []
    for index, row in personas.iterrows():
        tuples.append((row.persona_id, row.rut, row.nombre))
    cursor = CONN.cursor()
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

def calcular_antiguedad(fecha_inicio, fecha_fin):
    years_past = fecha_fin.year - fecha_inicio.year - 1
    if fecha_fin.month > fecha_inicio.month:
        years_past += 1
        if fecha_fin.day >= fecha_inicio.day:
            months_past = fecha_fin.month - fecha_inicio.month
            days_past = fecha_fin.day - fecha_inicio.day
        else:
            months_past = fecha_fin.month - fecha_inicio.month - 1
            if fecha_inicio.month == 2:
                days_past = (28-fecha_inicio.day) + fecha_fin.day
            elif fecha_inicio.month in [4, 6, 9, 11]:
                days_past = (30-fecha_inicio.day) + fecha_fin.day
            else:
                days_past = (31-fecha_inicio.day) + fecha_fin.day
    elif fecha_fin.month == fecha_inicio.month:
        if fecha_fin.day >= fecha_inicio.day:
            years_past += 1
            months_past = 0
            days_past = fecha_fin.day - fecha_inicio.day
        else:
            months_past = 11
            if fecha_inicio.month == 2:
                days_past = (28-fecha_inicio.day) + fecha_fin.day
            elif fecha_inicio.month in [4, 6, 9, 11]:
                days_past = (30-fecha_inicio.day) + fecha_fin.day
            else:
                days_past = (31-fecha_inicio.day) + fecha_fin.day
    else:
        months_past = 12-(fecha_inicio.month - fecha_fin.month) - 1
        if fecha_fin.day >= fecha_inicio.day:
            months_past += 1
            days_past = fecha_fin.day - fecha_inicio.day
        else:
            if fecha_inicio.month == 2:
                days_past = (28-fecha_inicio.day) + fecha_fin.day
            elif fecha_inicio.month in [4, 6, 9, 11]:
                days_past = (30-fecha_inicio.day) + fecha_fin.day
            else:
                days_past = (31-fecha_inicio.day) + fecha_fin.day
    antiguedad = f"{years_past} Años {months_past} Meses {days_past} Días"
    return antiguedad

paises = pd.read_json('paises.json')
ubicaciones = pd.read_json('ubicaciones_geograficas.json')

nacionalidades = {}

for index, row in paises.iterrows():
    codigo = row['codigo']
    gentilicio = row['gentilicio']
    nacionalidades[codigo] = gentilicio

contratos = pd.read_json(CONTRATOS_FILENAME)[[
    'id',
    'idContrato',
    'empleadoDetails',
    'tipoContratoDetails',
    'empleadorRazonSocial',
    'cargo',
    'sucursal',
    'centroCosto',
    'fechaContratacion',
    'hasta',
    'esPensionado',
    'tramoAsignacionPrevisional',
    'unidadOrganizacionalDetails',
    'motivoEgreso',
    'jornada',
    'horasDeLaJornada',
    'userDefinedFields'
]]

# update_personas(contratos)

data = {
    'id'                : [],
    'id_contrato'       : [],
    'rut'               : [],
    'nombre'            : [],
    'apellido_paterno'  : [],
    'apellido_materno'  : [],
    'razon_social'      : [],
    'fecha_nacimiento'  : [],
    'nacionalidad'      : [],
    'sexo'              : [],
    'cargo'             : [],
    'codigo_centro'     : [],
    'nombre_centro'     : [],
    'fecha_ingreso'     : [],
    'antiguedad'        : [],
    'direccion_ciudad'  : [],
    'direccion_comuna'  : [],
    'direccion_calle'   : [],
    'direccion_numero'  : [],
    'direccion_depto'   : [],
    'es_pensionado'     : [],
    'discapacidades'    : [],
    'contrato_hasta'    : [],
    'tipo_contrato'     : [],
    'motivo_egreso'     : [],
    'tramo_asignacion'  : [],
    'gerencia'          : [],
    'jornada'           : [],
    'horas_jornada'     : [],
    'nombre_sucursal'   : [],
    'tipo_categoria'    : [],
    'tipo_gasto'        : [],
    'vcto_plazo_fijo'   : [],
    'vigencia_pacto'    : []
}

for key in list(data.keys())[1:]:
    UPSERT_QUERY += f"{key}=EXCLUDED.{key}, "
UPSERT_QUERY = UPSERT_QUERY[:-2]

def insert_dataframe(dataframe, table_name=None):
    if not table_name:
        return 1
    tuples = [tuple(row) for row in dataframe.to_numpy()]
    columns = ','.join(list(dataframe.columns))

    # SQL query to execute
    insertion_query = UPSERT_QUERY % (table_name, columns, 'id')
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

for index, row in contratos.iterrows():
    if (pd.isnull(row.id)
            or pd.isnull(row.empleadoDetails)
            or pd.isnull(row.empleadorRazonSocial)
            or pd.isnull(row.fechaContratacion)
            ):
        continue
    data['id'].append(row['id'])
    data['id_contrato'].append(row['idContrato'])
    data['rut'].append(row['empleadoDetails']['rut'])
    data['nombre'].append(row['empleadoDetails']['nombre'])
    data['apellido_paterno'].append(row['empleadoDetails']['apellidoPaterno'])
    data['apellido_materno'].append(row['empleadoDetails']['apellidoMaterno'])
    data['razon_social'].append(RAZONES_SOCIALES[row['empleadorRazonSocial']])
    data['fecha_nacimiento'].append(row['empleadoDetails']['fechaNacimiento'])
    
    if pd.isnull(row['empleadoDetails']['nacionalidad']):
        data['nacionalidad'].append(np.nan)
    else:
        data['nacionalidad'].append(nacionalidades[
            row['empleadoDetails']['nacionalidad']
        ])
    data['sexo'].append(row['empleadoDetails']['sexo'])
    data['cargo'].append(row['cargo'])

    if pd.isnull(row['centroCosto']):
        data['codigo_centro'].append(np.nan)
        data['nombre_centro'].append(np.nan)
    else:
        data['codigo_centro'].append(row['centroCosto']['codigo'])
        data['nombre_centro'].append(row['centroCosto']['nombre'])
    
    data['fecha_ingreso'].append(row['fechaContratacion'])
    
    motivo_egreso_dict = row['motivoEgreso']
    fil = row['fechaContratacion'].split('-')
    fecha_ingreso = date(year=int(fil[0]), month=int(fil[1]), day=int(fil[2]))
    if pd.isnull(motivo_egreso_dict) or pd.isnull(row['hasta']):
        motivo_egreso = np.nan
        fecha_final = TODAY
    else:
        motivo_egreso = motivo_egreso_dict['nombre']
        ffl = row['hasta'].split('-')
        fecha_final = date(year=int(ffl[0]), month=int(ffl[1]), day=int(ffl[2]))
        if fecha_final >= TODAY:
            fecha_final = TODAY
    data['antiguedad'].append(calcular_antiguedad(fecha_ingreso, fecha_final))

    detalles = row['empleadoDetails']['detalles'][0]

    if pd.isnull(detalles['direccionCiudad']):
        data['direccion_ciudad'].append(np.nan)
    else:
        ciudad = ubicaciones.loc[ubicaciones.id==detalles['direccionCiudad']]
        if ciudad.empty:
            ciudad_texto = f"Ciudad [{detalles['direccionCiudad']}]"
            data['direccion_ciudad'].append(ciudad_texto)
        else:
            data['direccion_ciudad'].append(ciudad.iloc[0].nombre)

    if pd.isnull(detalles['direccionComuna']):
        data['direccion_comuna'].append(np.nan)
    else:
        comuna = ubicaciones.loc[ubicaciones.id==detalles['direccionComuna']]
        if comuna.empty:
            comuna_texto = f"Comuna [{detalles['direccionComuna']}]"
            data['direccion_comuna'].append(comuna_texto)
        else:
            data['direccion_comuna'].append(comuna.iloc[0].nombre)

    data['direccion_calle'].append(detalles['direccionCalle'])
    data['direccion_numero'].append(detalles['direccionNumero'])
    data['direccion_depto'].append(detalles['direccionDepartamento'])
    data['es_pensionado'].append(row['esPensionado'])
    data['discapacidades'].append(detalles['discapacidades'])
    data['contrato_hasta'].append(row['hasta'])

    if pd.isnull(row['tipoContratoDetails']):
        data['tipo_contrato'].append(np.nan)
    else:
        data['tipo_contrato'].append(row['tipoContratoDetails']['nombre'])

    data['motivo_egreso'].append(motivo_egreso)
    data['tramo_asignacion'].append(row['tramoAsignacionPrevisional'])

    if pd.isnull(row['unidadOrganizacionalDetails']):
        data['gerencia'].append(np.nan)
    else:
        data['gerencia'].append(row['unidadOrganizacionalDetails']['nombre'])

    if pd.isnull(row['jornada']):
        data['jornada'].append(np.nan)
    else:
        data['jornada'].append(row['jornada']['nombre'])

    data['horas_jornada'].append(row['horasDeLaJornada'])
    
    if pd.isnull(row['sucursal']):
        data['nombre_sucursal'].append(np.nan)
    else:
        data['nombre_sucursal'].append(row['sucursal']['nombre'])

    otros = row['userDefinedFields']

    if pd.isnull(otros):
        data['tipo_categoria'].append(np.nan)
        data['tipo_gasto'].append(np.nan)
        data['vcto_plazo_fijo'].append(np.nan)
        data['vigencia_pacto'].append(np.nan)
    else:
        data['tipo_categoria'].append(otros['tipocategoria'])
        data['tipo_gasto'].append(otros['TIPOGASTO'])
        data['vcto_plazo_fijo'].append(otros['VctoPlazoF'])
        data['vigencia_pacto'].append(otros['Vigenciapacto'])

for key in data.keys():
    for i in range(len(data[key])):
        if data[key][i] == '' or data[key][i] == None:
            data[key][i] = np.nan

DATA = pd.DataFrame(data)

def max_len(string_list):
    max_len = -1
    index = -1
    for i in range(len(string_list)):
        if type(string_list[i]) is str and len(string_list[i]) > max_len:
            max_len = len(string_list[i])
            index = i
    if max_len == -1:
        return '', -1, ''
    else:
        return max_len, index, string_list[index]

# for key in data:
#     print(f"{key}{' ' * (20 - len(key))} : {max_len(data[key])[0]}")

insert_dataframe(DATA, table_name='contratos')

CONN.close()