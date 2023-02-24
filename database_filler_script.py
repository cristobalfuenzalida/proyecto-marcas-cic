import scraper_bot as sb
import database_filler_functions as dff
import aux_functions as af
import pandas as pd
import numpy as np
import sys
import os

filename = 'ReporteAvanzado.xlsx'

if not os.path.exists(filename):
    print("The file you are trying to use as data source does not exist")
    print("Program finished unsuccessfully")
    sys.exit(1)

DATA = pd.read_excel(filename)[[
        'Rut', 'Nombre', 'Sucursal', 'Centro de costo', 'Fecha',
        'Entrada real', 'Salida real', 'Entrada turno', 'Salida turno',
        'Turno', 'Noche', 'Permiso', 'Detalle permisos'
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
        'Permiso'           : 'permiso',
        'Detalle permisos'  : 'detalle_permiso'
    }).assign(colacion='00:45:00')

DATA.loc[pd.isnull(DATA['turno']), 'colacion'] = np.nan

DATA.loc[DATA['noche'] == 1, 'noche'] = 0

DATA.loc[DATA['entrada_turno'] > DATA['salida_turno'], 'noche'] = 1

DATA.sort_values(by=['fecha', 'entrada_real'], inplace=True)

AUX_TABLES = [
    'personas', 'sucursales', 'centros_de_costo', 'turnos', 'permisos'
]

#! Change to option = 's' to make whole process automatic and interrumpted
option = 'p'
while option not in ['p', 's']:
    option = af.timeout_input(
        5, 'Print-Only or save into DataBase? (p/s): ', 'p')
print_mode = (option == 'p')
if print_mode:
    print(DATA)
else:
    dff.clear_marks('datos_calculados', reset_index=False,
                    fecha_desde=sb.DAYS)
    dff.clear_marks('marcas_turnos', reset_index=True,
                    fecha_desde=sb.DAYS)

dff.fill_aux_tables(DATA, AUX_TABLES, print_mode)
dff.fill_marks_table(DATA, 'marcas_turnos', print_mode)
dff.fill_results_table(DATA, 'datos_calculados', print_mode)

print("Closing connection...")
dff.CONN.close()
print("Program finished successfully")

status_file = open('status.txt', 'w')
status_file.write('Cron task worked successfully.')
status_file.close()