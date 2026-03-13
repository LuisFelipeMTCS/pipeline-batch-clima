/*
  stg_clima.sql
  =============
  O QUE FAZ:
    - Lê os dados horários do INMET direto da camada bronze (PostgreSQL)
    - Confirma/padroniza tipos de colunas
    - Extrai ano e mês para facilitar agregações futuras
    - Filtra registros com data nula ou temperatura fisicamente impossível
    - Remove duplicatas exatas

  POR QUE É UMA VIEW (não tabela)?
    - Staging é uma camada leve de "limpeza de entrada"
    - Views não ocupam espaço extra e sempre refletem os dados mais recentes do bronze
    - dbt materializa como view por padrão neste projeto

  PRÓXIMOS PASSOS:
    - int_clima_mensal.sql agrega esses dados por mês
    - int_clima_sazonal.sql agrega por estação do ano
*/

with source as (

    select * from "datalake"."bronze"."clima"

),

cleaned as (

    select
        -- Identificação da estação
        estacao_codigo,
        estacao_nome,
        uf,

        -- Dimensão temporal
        CAST(data AS date)                          as data,
        hora_utc,
        CAST(extract(year  from CAST(data AS date)) AS int)       as ano,
        CAST(extract(month from CAST(data AS date)) AS int)       as mes,

        -- Medições climáticas (cast garante tipo float, nulo permanece nulo)
        CAST(precipitacao_mm     AS float)          as precipitacao_mm,
        CAST(temperatura_c       AS float)          as temperatura_c,
        CAST(temp_max_c          AS float)          as temp_max_c,
        CAST(temp_min_c          AS float)          as temp_min_c,
        CAST(umidade_pct         AS float)          as umidade_pct,
        CAST(pressao_mb          AS float)          as pressao_mb,
        CAST(radiacao_kj_m2      AS float)          as radiacao_kj_m2,
        CAST(vento_velocidade_ms AS float)          as vento_velocidade_ms

    from source

    where
        -- Exige pelo menos data válida
        data is not null
        -- Temperatura fisicamente possível (-40 a 60°C)
        and (temperatura_c is null or temperatura_c between -40 and 60)

),

deduplicated as (

    select distinct * from cleaned

)

select * from deduplicated