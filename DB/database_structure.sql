-- Eliminación de tablas para re-crearlas en la base
DROP VIEW resultados_diarios;
DROP TABLE marcas_turnos;
DROP TABLE log_ejecuciones;
DROP TABLE razones_sociales;
DROP TABLE personas;
DROP TABLE sucursales;
DROP TABLE centros_de_costo;
DROP TABLE turnos;
DROP TABLE permisos;

-- Creación de tabla de log de descargas
CREATE TABLE log_ejecuciones (
    id              SERIAL          PRIMARY KEY,
    fecha           DATE            NOT NULL,
    hora_ejecucion  TIME            NOT NULL,
    hora_termino    TIME            NOT NULL,
    rango_inicio    DATE            NOT NULL,
    rango_fin       DATE            NOT NULL
);

-- Creación de tabla razones sociales con datos ingresados manualmente
CREATE TABLE razones_sociales (
    id              PRIMARY KEY,
    razon_social    VARCHAR(50)     NOT NULL            UNIQUE
);
-- Inserción de valores predeterminados 
INSERT INTO razones_sociales (id, razon_social)
VALUES
    (9415, 'CIC RETAIL SPA'),
    (9414, 'COMPAÑIAS CIC S.A.')
    (9425, 'Externos')
ON CONFLICT (razon_social) DO NOTHING;

-- Creación de tabla personas directamente desde API
CREATE TABLE personas (
    id              INT             PRIMARY KEY,
    rut             VARCHAR(10)     NOT NULL            UNIQUE,
    nombre          VARCHAR(80)     NOT NULL
);

-- Creación de tabla sucursales directamente desde API
CREATE TABLE sucursales (
    id              INT             PRIMARY KEY,
    sucursal        VARCHAR(50)     NOT NULL            UNIQUE
);

-- Creación de tabla centros_de_costo directamente desde Excel
CREATE TABLE centros_de_costo (
    id              PRIMARY KEY,
    codigo          VARCHAR(5)
    centro          VARCHAR(50)     NOT NULL            UNIQUE
);

-- Creación de tabla turnos con datos de Google Sheets
CREATE TABLE turnos (
    id              SERIAL          PRIMARY KEY,
    turno           VARCHAR(80)     NOT NULL            UNIQUE,
    entrada_turno   TIME            NOT NULL,
    salida_turno    TIME            NOT NULL,
    colacion        TIME
);

-- Creación de tabla permisos con datos ingresados manualmente
CREATE TABLE permisos (
    id              SERIAL          PRIMARY KEY,
    tipo            VARCHAR(30)     NOT NULL            UNIQUE
);

-- Inserción de valores predeterminados 
INSERT INTO permisos (id, tipo)
VALUES
    (DEFAULT, 'dia_vacaciones'),
    (DEFAULT, 'licencia_medica'),
    (DEFAULT, 'licencia_maternal'),
    (DEFAULT, 'permiso_con_goce'),
    (DEFAULT, 'permiso_sin_goce'),
    (DEFAULT, 'falta_injustificada'),
    (DEFAULT, 'dia_administrativo')
ON CONFLICT (tipo) DO NOTHING;

-- Creación de tabla marcas_turnos con datos de Excel
CREATE TABLE marcas_turnos (
    id              SERIAL      PRIMARY KEY,
    persona_id      INT         NOT NULL        REFERENCES personas(id),
    fecha           DATE        NOT NULL,
    razon_social    VARCHAR(50) NOT NULL,
    sucursal_id     INT         NOT NULL        REFERENCES sucursales(id),
    centro_id       INT         NOT NULL        REFERENCES centros_de_costo(id),
    entrada_real    TIME,
    salida_real     TIME,
    turno_id        INT         REFERENCES turnos(id),
    t_asignado      REAL,
    t_asistido      REAL,
    t_atraso        REAL,
    t_anticipo      REAL,
    t_permiso_cg    REAL,
    permiso_id      INT         REFERENCES permisos(id),
    detalle_permiso VARCHAR(40)
);

-- Creación de tabla contratos con datos de API Talana
CREATE TABLE contratos (
    id                  INT             PRIMARY KEY,
    id_contrato         VARCHAR(40)     NOT NULL,
    rut                 VARCHAR(10)     NOT NULL,
    nombre              VARCHAR(40)     NOT NULL,
    apellido_paterno    VARCHAR(20)     NOT NULL,
    apellido_materno    VARCHAR(20),
    razon_social        VARCHAR(20)     NOT NULL,
    fecha_nacimiento    DATE,
    nacionalidad        VARCHAR(20),
    sexo                CHAR,
    cargo               VARCHAR(70),
    codigo_centro       VARCHAR(5),
    nombre_centro       VARCHAR(60),
    fecha_ingreso       DATE,
    antiguedad          VARCHAR(30),
    direccion_ciudad    VARCHAR(20),
    direccion_comuna    VARCHAR(30),
    direccion_calle     VARCHAR(60),
    direccion_numero    VARCHAR(40),
    direccion_depto     VARCHAR(30),
    es_pensionado       CHAR,
    discapacidades      VARCHAR(30),
    contrato_hasta      DATE,
    tipo_contrato       VARCHAR(30),
    motivo_egreso       VARCHAR(60),
    tramo_asignacion    INT,
    gerencia            VARCHAR(50),
    jornada             VARCHAR(25),
    horas_jornada       INT,
    nombre_sucursal     VARCHAR(60),
    tipo_categoria      VARCHAR(50),
    tipo_gasto          VARCHAR(20),
    vcto_plazo_fijo     VARCHAR(30),
    vigencia_pacto      VARCHAR(20)
);

-- Creación de vista resultados_diarios
CREATE OR REPLACE VIEW resultados_diarios (
    id, rut, fecha, sucursal, centro_costo, t_asignado, t_asistido,
    t_atraso, t_anticipo, t_vacaciones, t_licencia, t_permiso_sg, t_permiso_cg
) AS
--! ---------------------------------------------------------------------------
WITH    tiempos_permisos (id, t_vacaciones, t_licencia, t_permiso_sg) AS (
    SELECT  mt.id,
        CASE
            WHEN    p.tipo  = 'dia_vacaciones'
            THEN    mt.t_asignado
            ELSE    0
        END AS  t_vacaciones,
        CASE
            WHEN    p.tipo  IN ('licencia_medica', 'licencia_maternal')
            THEN    mt.t_asignado
            ELSE    0
        END AS  t_licencia,
        CASE
            WHEN    p.tipo  IN ('permiso_sin_goce', 'falta_injustificada')
            THEN    mt.t_asignado
            ELSE    0
        END AS  t_permiso_sg
    FROM        marcas_turnos       AS mt
    LEFT JOIN   permisos            AS p
    ON          mt.permiso_id       = p.id)
-----------------------------------------------------------------------
SELECT  mt.id, pers.rut, mt.fecha, suc.sucursal, cdc.centro,
        mt.t_asignado, mt.t_asistido, mt.t_atraso, mt.t_anticipo,
        tp.t_vacaciones, tp.t_licencia, tp.t_permiso_sg, mt.t_permiso_cg
FROM        marcas_turnos       AS mt
LEFT JOIN   personas            AS pers
ON          mt.persona_id   = pers.id
LEFT JOIN   sucursales          AS suc
ON          mt.sucursal_id  = suc.id
LEFT JOIN   centros_de_costo    AS cdc
ON          mt.centro_id    = cdc.id
LEFT JOIN   turnos              AS tur
ON          mt.turno_id     = tur.id
LEFT JOIN   tiempos_permisos    AS tp
ON          mt.id           = tp.id
ORDER BY    mt.id ASC;
