
  
    

  create  table "datalake"."silver"."int_clima_mensal__dbt_tmp"
  
  
    as
  
  (
    /*
  int_clima_mensal.sql
  ====================
  O QUE FAZ:
    - Agrega os dados horários do staging para o nível MENSAL
    - Calcula estatísticas de temperatura, chuva, umidade e vento por mês
    - Uma linha por (estacao, ano, mes)

  POR QUE ISSO É ÚTIL?
    Dados horários são difíceis de analisar. Com dados mensais conseguimos
    responder perguntas como:
      - "Qual mês tem mais risco de seca?"
      - "Quando ocorrem as temperaturas extremas?"
      - "Qual período é o mais chuvoso?"

  MATERIALIZAÇÃO: table
    Intermediários são tabelas físicas porque têm agregação pesada.
    Criar uma tabela evita recalcular toda vez que o mart for consultado.
*/

with stg as (

    select * from "datalake"."silver"."stg_clima"

),

mensal as (

    select
        -- Dimensões
        estacao_codigo,
        estacao_nome,
        uf,
        ano,
        mes,

        -- Temperatura (°C)
        ROUND(CAST(AVG(temperatura_c)  AS numeric), 1)  as temp_media_c,
        ROUND(CAST(MAX(temp_max_c)     AS numeric), 1)  as temp_max_c,
        ROUND(CAST(MIN(temp_min_c)     AS numeric), 1)  as temp_min_c,
        ROUND(CAST(AVG(temp_max_c)     AS numeric), 1)  as temp_max_media_c,
        ROUND(CAST(AVG(temp_min_c)     AS numeric), 1)  as temp_min_media_c,

        -- Precipitação (mm) - SUM porque é acumulado no mês
        ROUND(CAST(SUM(precipitacao_mm) AS numeric), 1) as precipitacao_total_mm,
        COUNT(CASE WHEN precipitacao_mm > 0 THEN 1 END) as dias_com_chuva,

        -- Umidade (%)
        ROUND(CAST(AVG(umidade_pct)    AS numeric), 1)  as umidade_media_pct,
        ROUND(CAST(MIN(umidade_pct)    AS numeric), 1)  as umidade_min_pct,

        -- Vento (m/s)
        ROUND(CAST(AVG(vento_velocidade_ms) AS numeric), 1) as vento_medio_ms,

        -- Total de medições no mês (para qualidade dos dados)
        COUNT(*) as total_medicoes

    from stg

    group by
        estacao_codigo,
        estacao_nome,
        uf,
        ano,
        mes

)

select * from mensal
  );
  