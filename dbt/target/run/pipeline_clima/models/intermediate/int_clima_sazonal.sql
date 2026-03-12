
  
    

  create  table "datalake"."silver"."int_clima_sazonal__dbt_tmp"
  
  
    as
  
  (
    /*
  int_clima_sazonal.sql
  =====================
  O QUE FAZ:
    - Agrega os dados MENSAIS para o nível de ESTAÇÃO DO ANO
    - Usa as estações do hemisfério sul (Brasil):
        Verão:    Dezembro, Janeiro, Fevereiro
        Outono:   Março, Abril, Maio
        Inverno:  Junho, Julho, Agosto
        Primavera: Setembro, Outubro, Novembro

  POR QUE ISSO É ÚTIL?
    Culturas são plantadas por estação. Para analisar risco climático
    precisamos saber: "No inverno, qual é a temperatura mínima média?"
    ou "No verão, quanta chuva cai?"

  NOTA: Dezembro pertence ao verão do ano seguinte. Para simplificar,
        agrupamos dezembro com o mesmo ano dos demais meses de verão
        (o risco agrícola é analisado pela safra, não pelo ano-calendário).
*/

with mensal as (

    select * from "datalake"."silver"."int_clima_mensal"

),

com_estacao as (

    select
        *,
        case
            when mes in (12, 1, 2)  then 'Verao'
            when mes in (3, 4, 5)   then 'Outono'
            when mes in (6, 7, 8)   then 'Inverno'
            when mes in (9, 10, 11) then 'Primavera'
        end as estacao_ano

    from mensal

),

sazonal as (

    select
        -- Dimensões
        estacao_codigo,
        estacao_nome,
        uf,
        ano,
        estacao_ano,

        -- Temperatura sazonal (°C)
        ROUND(CAST(AVG(temp_media_c)    AS numeric), 1) as temp_media_c,
        ROUND(CAST(MAX(temp_max_c)      AS numeric), 1) as temp_max_c,
        ROUND(CAST(MIN(temp_min_c)      AS numeric), 1) as temp_min_c,
        ROUND(CAST(AVG(temp_max_media_c) AS numeric), 1) as temp_max_media_c,
        ROUND(CAST(AVG(temp_min_media_c) AS numeric), 1) as temp_min_media_c,

        -- Precipitação sazonal (mm)
        ROUND(CAST(SUM(precipitacao_total_mm) AS numeric), 1) as precipitacao_total_mm,
        SUM(dias_com_chuva)                                   as dias_com_chuva,

        -- Umidade sazonal (%)
        ROUND(CAST(AVG(umidade_media_pct) AS numeric), 1) as umidade_media_pct,
        ROUND(CAST(MIN(umidade_min_pct)   AS numeric), 1) as umidade_min_pct,

        -- Número de meses agregados (deve ser 3 para estações completas)
        COUNT(*) as meses_no_periodo

    from com_estacao

    group by
        estacao_codigo,
        estacao_nome,
        uf,
        ano,
        estacao_ano

)

select * from sazonal
  );
  