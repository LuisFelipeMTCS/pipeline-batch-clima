
  create view "datalake"."silver"."stg_culturas__dbt_tmp"
    
    
  as (
    /*
  stg_culturas.sql
  ================
  O QUE FAZ:
    - Lê as culturas agrícolas da camada bronze
    - Seleciona e renomeia colunas relevantes para análise de risco
    - As tolerâncias (tolerante_seca, sensivel_frio, etc.) já foram extraídas
      pelo Spark na camada bronze, então aqui apenas confirmamos os tipos

  COLUNAS IMPORTANTES:
    - temp_min_ideal / temp_max_ideal: faixa de temperatura em que a cultura
      cresce bem. Quando a temperatura real sair dessa faixa → risco!
    - necessidade_agua_score: 3=Alta, 2=Média, 1=Baixa.
      Culturas com score 3 sofrem mais quando chove pouco.
    - tolerante_seca: se True, a cultura aguenta períodos sem chuva
    - sensivel_frio: se True, geada ou frio intenso mata a lavoura
*/

with source as (

    select * from "datalake"."bronze"."culturas"

),

cleaned as (

    select
        -- Identificação da cultura
        cultura,
        categoria,

        -- Clima ideal de crescimento
        clima_ideal,
        CAST(temp_min_ideal AS int)   as temp_min_ideal,
        CAST(temp_max_ideal AS int)   as temp_max_ideal,

        -- Necessidade hídrica
        necessidade_de_agua,
        CAST(necessidade_agua_score AS int)  as necessidade_agua_score,

        -- Tolerâncias e sensibilidades (booleanos extraídos no bronze)
        CAST(tolerante_seca  AS boolean)     as tolerante_seca,
        CAST(tolerante_frio  AS boolean)     as tolerante_frio,
        CAST(sensivel_frio   AS boolean)     as sensivel_frio,
        CAST(sensivel_calor  AS boolean)     as sensivel_calor

    from source

    where cultura is not null

)

select * from cleaned
  );