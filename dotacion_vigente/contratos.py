import pandas as pd

contratos = pd.read_json('contratos_vigentes_2023-03-14')

contratos_gerentes = contratos.dropna(subset=['cargo']).loc[
    contratos.cargo.str.contains("gerente", case=False)
]

sueldos = contratos.sueldoBase
sueldos_gerentes = contratos_gerentes.sueldoBase

