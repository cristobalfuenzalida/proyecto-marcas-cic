import datetime
import pandas as pd
import numpy as np
import sys
import os

TZ_ADJUST = datetime.timedelta(seconds=-10800)
inicio_ejecucion = datetime.datetime.now() + TZ_ADJUST

# import scraper_bot
import aux_functions as af
import database_filler_functions as dff

TURNOS_CELL_RANGE = 'Turnos!A1:B1000'

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
            'Llegada tardia. corr.', 'Salida temp. corr.', 'Turno',
            'Permiso', 'Detalle permisos'
        ]].rename(columns={
            'Rut'                   : 'rut',
            'Nombre'                : 'nombre',
            'Fecha'                 : 'fecha',
            'Sucursal'              : 'sucursal',
            'Centro de costo'       : 'centro',
            'Entrada real'          : 'entrada_real',
            'Salida real'           : 'salida_real',
            'Entrada turno'         : 'entrada_turno',
            'Salida turno'          : 'salida_turno',
            'Llegada tardia. corr.' : 'horas_atraso',
            'Salida temp. corr.'    : 'horas_anticipo',
            'Turno'                 : 'turno',
            'Permiso'               : 'permiso',
            'Detalle permisos'      : 'detalle_permiso'
        }).assign(colacion=np.nan).assign(razon_social=razon_social)

DATA = pd.concat(list(dataframes.values()))[[
    'rut', 'nombre', 'fecha', 'razon_social', 'sucursal', 'centro',
    'entrada_real', 'salida_real', 'entrada_turno', 'salida_turno',
    'horas_atraso', 'horas_anticipo', 'turno', 'colacion',
    'permiso', 'detalle_permiso'
]]

DATA.sort_values(by=['fecha', 'entrada_real'], inplace=True)

DATA.reset_index(drop=True, inplace=True)