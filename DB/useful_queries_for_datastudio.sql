--* Query para extraer marcas de PostgreSQL desde Python
-- WITH    raw_data    AS (
--     SELECT      mt.id, mt.entrada_real, mt.salida_real,
--                 t.entrada_turno, t.salida_turno, t.colacion, t.noche,
--                 p.tipo, mt.detalle_permiso
--     FROM        marcas_turnos       AS mt
--     LEFT JOIN   turnos              AS t
--     ON          mt.turno_id         = t.id
--     LEFT JOIN   permisos            AS p
--     ON          mt.permiso_id       = p.id
-- ) SELECT * FROM raw_data;

-- Obtener los tiempos asociados a todos los empleados en un rango de fechas
WITH resultados_agregados AS (
    SELECT  rut,
        SUM(t_asignado)     AS suma_t_asignado,
        SUM(t_asistido)     AS suma_t_asistido,
        SUM(t_vacaciones)   AS suma_t_vacaciones,
        SUM(t_licencia)     AS suma_t_licencias,
        SUM(t_ausencia)     AS suma_t_ausencias,
        SUM(t_permiso_cg)   AS suma_t_permiso_cg
    FROM    resultados_diarios
    WHERE   fecha   BETWEEN '2023-02-12' AND '2023-03-15'
    GROUP BY rut
)   SELECT  *
    FROM    resultados_agregados
    WHERE   suma_t_vacaciones > 0
            AND suma_t_licencias > 0
            AND suma_t_permiso_cg > 0;

-- Obtener relación entre centro de costo y sucursal a la que pertenece
SELECT  DISTINCT sucursal, centro
FROM    marcas_turnos       AS mt
JOIN    sucursales          AS suc
ON      mt.sucursal_id  = suc.id
JOIN    centros_de_costo    AS cdc
ON      mt.centro_id    = cdc.id
ORDER BY    sucursal ASC;