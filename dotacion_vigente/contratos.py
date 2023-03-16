import pandas as pd
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

contratos = pd.read_json('contratos_vigentes_2023-03-14.json')[[
    'id', 'contrato', 'empleado', 'tipoContrato', 'empleadorRazonSocial',
    'cargo', 'sucursal', 'centroCosto', 'sueldoBase'
]].rename(columns={
    'empleado'              : 'persona_id',
    'tipoContrato'          : 'tipo_contrato',
    'empleadorRazonSocial'  : 'razon_social_id'
})

contratos.dropna(subset="empleado", inplace=True)

