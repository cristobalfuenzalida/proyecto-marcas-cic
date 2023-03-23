import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras as extras

DEFAULT_QUERY = "INSERT INTO %s(%s) VALUES %%s"
UPSERT_QUERY = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT (%s) DO NOTHING"

CONN = psycopg2.connect(
    database    = 'asistencias_cic',
    user        = 'dyatec',
    password    = 'dyatec2023',
    host        = 'localhost',
    port        = '5432'
)

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

contratos = pd.read_json('contratos_vigentes_2023-03-14.json')[[
    'id', 'idContrato', 'empleadoDetails', 'tipoContratoDetails',
    'empleadorRazonSocial', 'cargo', 'sucursal', 'centroCosto',
    'unidadOrganizacional', 'motivoEgreso', 'sueldoBase', 'rolPrivado'
]]

update_personas(contratos)

data = {
    'id'                : [],
    'contrato'          : [],
    'persona_id'        : [],
    'tipo_contrato'     : [],
    'razon_social_id'   : [],
    'cargo'             : [],
    'sucursal_id'       : [],
    'centro_id'         : [],
    'unidad_org_id'     : [],
    'vigente'           : [],
    'sueldo_bool'       : [],
    'rol_privado'       : []
}

for index, row in contratos.iterrows():
    if (pd.isnull(row.id) or pd.isnull(row.empleadoDetails)
            or pd.isnull(row.tipoContratoDetails)
            or pd.isnull(row.sucursal) or pd.isnull(row.centroCosto)
            or pd.isnull(row.empleadorRazonSocial)
            or pd.isnull(row.unidadOrganizacional)
            ):
        continue

    data['id'].append(row.id)
    data['persona_id'].append(row.empleadoDetails['id'])
    data['contrato'].append(row.idContrato)
    data['tipo_contrato'].append(row.tipoContratoDetails['nombre'])
    data['razon_social_id'].append(row.empleadorRazonSocial)
    data['cargo'].append(row.cargo)
    data['sucursal_id'].append(int(row.sucursal['id']))
    data['centro_id'].append(int(row.centroCosto['id']))
    data['unidad_org_id'].append(int(row.unidadOrganizacional))
    data['vigente'].append(pd.isnull(row.motivoEgreso))
    data['sueldo_bool'].append(
        pd.notnull(row.sueldoBase) and row.sueldoBase > 0
    )
    data['rol_privado'].append(row.rolPrivado)

DATA = pd.DataFrame(data)

insert_dataframe(DATA, table_name='contratos')