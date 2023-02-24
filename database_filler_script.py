import scraper_bot as sb
import database_filler_functions as dff
import aux_functions as af
from datetime import timedelta as time
from datetime import date
import pandas as pd
import numpy as np
import sys
import os

dataframes = {}

for razon_social in sb.RAZONES_SOCIALES:
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
        }).assign(colacion='00:45:00').assign(razon_social=razon_social)

DATA = pd.concat(list(dataframes.values()))[[
    'rut', 'nombre', 'razon_social', 'sucursal', 'centro', 'fecha',
    'entrada_real', 'salida_real', 'entrada_turno', 'salida_turno',
    'turno', 'colacion', 'permiso', 'detalle_permiso'
]]

DATA.loc[pd.isnull(DATA['turno']), 'colacion'] = np.nan

DATA.sort_values(by=['fecha', 'entrada_real'], inplace=True)

option = 'p'
while option not in ['p', 's']:
    option = af.timeout_input(
        10, 'Print-Only or save into DataBase? (p/s): ', 'p')
print_mode = (option == 'p')
if print_mode:
    print(DATA)
else:
    fecha_desde = str(date.today() - time(days=sb.DAYS))
    dff.clear_marks('marcas_turnos', fecha_desde)

dff.fill_marks_table(DATA, 'marcas_turnos', print_mode)

print("Closing connection...")
dff.CONN.close()
print("Program finished successfully")

status_file = open('status.txt', 'w')
status_file.write('Cron task worked successfully.')
status_file.close()