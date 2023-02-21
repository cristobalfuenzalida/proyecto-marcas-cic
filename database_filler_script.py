import scraper_bot
import database_filler_functions as dff
import pandas as pd

DATA = pd.read_excel('ReporteAvanzado.xlsx')[[
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
        'Noche'             : 'noche',
        'Permiso'           : 'permiso',
        'Detalle permisos'  : 'detalle_permiso'
    }).assign(colacion='00:45:00')

DATA.sort_values(by=['fecha', 'entrada_real'], inplace=True)

AUX_TABLES = [
    'personas', 'sucursales', 'centros_de_costo', 'turnos', 'permisos'
]

option = None
while option not in ['p', 's']:
    option = input('Print-Only or save into DataBase? (p/s): ')
print_mode = (option == 'p')
if print_mode:
    print(DATA)
else:
    option = None
    while option not in ['y', 'n']:
        option = input('Clear database tables first? (y/n): ')
    if option == 'y':
        dff.clear_table('datos_calculados', reset_index=False)
        dff.clear_table('marcas_turnos', reset_index=True)
        for table_name in AUX_TABLES:
            dff.clear_table(table_name, reset_index=True)

dff.fill_aux_tables(DATA, AUX_TABLES, print_mode)
dff.fill_marks_table(DATA, 'marcas_turnos', print_mode)
dff.fill_results_table('datos_calculados', print_mode)

print("Closing connection...")
dff.CONN.close()
print("Program finished successfully")
