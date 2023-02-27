import datetime
import pandas as pd
import numpy as np
import sys
import os

inicio_ejecucion = datetime.datetime.now()

# import scraper_bot as sb
import aux_functions as af
import database_filler_functions as dff

TURNOS_CELL_RANGE = 'Turnos!A1:B200'

dataframes = {}

for razon_social in af.RAZONES_SOCIALES:
    rs_file_format = razon_social.replace(' ', '_').replace('.', '_')
    filename = f"Reporte_{rs_file_format}.xlsx"

    if not os.path.exists(filename):
        print(f"File {filename} not found...")
        print('The file you are trying to use as data source does not exist')
        print('Program finished unsuccessfully')
        sys.exit(1)

    dataframes[razon_social] = pd.read_excel(filename)[[
            'Rut', 'Nombre', 'Sucursal', 'Centro de costo', 'Fecha',
            'Entrada real', 'Salida real', 'Entrada turno', 'Salida turno',
            'Turno', 'Permiso', 'Detalle permisos'
        ]].rename(columns={
            'Rut'               : 'rut',
            'Nombre'            : 'nombre',
            'Fecha'             : 'fecha',
            'Sucursal'          : 'sucursal',
            'Centro de costo'   : 'centro',
            'Entrada real'      : 'entrada_real',
            'Salida real'       : 'salida_real',
            'Entrada turno'     : 'entrada_turno',
            'Salida turno'      : 'salida_turno',
            'Turno'             : 'turno',
            'Permiso'           : 'permiso',
            'Detalle permisos'  : 'detalle_permiso'
        }).assign(colacion=np.nan).assign(razon_social=razon_social)

DATA = pd.concat(list(dataframes.values()))[[
    'rut', 'nombre', 'fecha', 'razon_social', 'sucursal', 'centro',
    'entrada_real', 'salida_real', 'entrada_turno', 'salida_turno',
    'turno', 'colacion', 'permiso', 'detalle_permiso'
]]

DATA.sort_values(by=['fecha', 'entrada_real'], inplace=True)

DATA.reset_index(drop=True, inplace=True)

# execution_mode = None
# while execution_mode not in ['print', 'save']:
#     execution_mode = af.timeout_input(
#         10, 'Print-Only or save into DataBase? (print/save): ', 'print')

# dff.update_aux_tables(DATA, execution_mode=execution_mode)
# dff.update_turnos(DATA, TURNOS_CELL_RANGE, execution_mode)

# if execution_mode == 'save':
#     # Clear tables 'marcas_turnos' and 'datos_calculados' from a date onwards
#     fecha_desde = DATA.fecha.min()
#     fecha_hasta = DATA.fecha.max()
#     dff.clear_marks('marcas_turnos', fecha_desde)
#     dff.clear_marks('datos_calculados', fecha_desde)

# dff.fill_marks_table(DATA, 'marcas_turnos', execution_mode)

# #TODO Register latest execution to log

# fin_ejecucion = datetime.datetime.now()

# if execution_mode == 'save':
#     cursor = dff.CONN.cursor()

#     log = pd.DataFrame({
#         'fecha'             : [inicio_ejecucion.strftime("%Y-%m-%d")],
#         'hora_ejecucion'    : [inicio_ejecucion.strftime("%H:%M:%S")],
#         'hora_termino'      : [fin_ejecucion.strftime("%H:%M:%S")],
#         'rango_inicio'      : [fecha_desde],
#         'rango_fin'         : [fecha_hasta]
#     })
#     dff.insert_dataframe(log, 'log_ejecuciones')

#     cursor.close()

# print("Closing connection...")
# dff.CONN.close()
# print("Program finished successfully")
