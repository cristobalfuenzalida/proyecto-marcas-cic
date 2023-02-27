import pandas as pd
import json

asignaciones_file = open('asignacion_contratos.json')

data = json.load(asignaciones_file)['results']

colaciones = {
    'rut'       : [],
    'colacion'  : []
}

for element in data:
    colacion_minutos = element['schedule']['numberSnackMinutes']

    hours = int(colacion_minutos) // 60
    minutes = int(colacion_minutos) % 60
    seconds = int(colacion_minutos * 60) % 60

    hh = f"{0 if hours<10 else ''}{hours}"
    mm = f"{0 if minutes<10 else ''}{minutes}"
    ss = f"{0 if seconds<10 else ''}{seconds}"

    t_colacion = f"{hh}:{mm}:{ss}"
    
    colaciones['rut'].append(element['person']['rut'])
    colaciones['colacion'].append(t_colacion)

dataframe = pd.DataFrame(colaciones)

print(dataframe)