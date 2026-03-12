/*
  fct_risco_climatico.sql
  =======================
  O QUE FAZ:
    - É o modelo final (Gold) do projeto
    - Cruza dados climáticos SAZONAIS com cada CULTURA agrícola
    - Calcula 3 scores de risco por estação/ano/cultura:
        1. risco_seca:   precipitação baixa + cultura precisa de muita água
        2. risco_frio:   temperatura mínima baixa + cultura sensível ao frio
        3. risco_calor:  temperatura máxima alta + cultura sensível ao calor
    - Calcula um risco_total (média ponderada dos 3)
    - Classifica o risco em categoria: Baixo / Médio / Alto / Crítico

  COMO OS SCORES FUNCIONAM (0 a 10):

    RISCO DE SECA (peso maior para culturas que precisam de muita água):
      - precipitacao_total_mm < 50mm no período → seco
      - score aumenta se necessidade_agua_score é alto (3=Alta)
      - score reduzido se cultura é tolerante_seca

    RISCO DE FRIO:
      - temp_min_c < temp_min_ideal da cultura → risco de frio
      - score aumenta se cultura é sensivel_frio
      - score zero se cultura é tolerante_frio

    RISCO DE CALOR:
      - temp_max_c > temp_max_ideal da cultura → risco de calor
      - score aumenta se cultura é sensivel_calor

  MATERIALIZAÇÃO: table
    Este é o modelo mais consultado. Tabela física para performance máxima.
*/

with sazonal as (

    select * from {{ ref('int_clima_sazonal') }}

),

culturas as (

    select * from {{ ref('stg_culturas') }}

),

-- Produto cartesiano: cada cultura × cada período sazonal
cruzamento as (

    select
        -- Identificação
        s.estacao_codigo,
        s.estacao_nome,
        s.uf,
        s.ano,
        s.estacao_ano,
        c.cultura,
        c.categoria,

        -- Clima observado no período
        s.temp_media_c,
        s.temp_max_c,
        s.temp_min_c,
        s.precipitacao_total_mm,
        s.dias_com_chuva,
        s.umidade_media_pct,

        -- Limites ideais da cultura
        c.temp_min_ideal,
        c.temp_max_ideal,
        c.necessidade_agua_score,
        c.tolerante_seca,
        c.tolerante_frio,
        c.sensivel_frio,
        c.sensivel_calor

    from sazonal s
    cross join culturas c

),

-- Calcula os scores de risco
com_scores as (

    select
        *,

        -- -------------------------------------------------------
        -- RISCO DE SECA (0-10)
        -- Lógica: chuva abaixo de 100mm no período já é sinal de seca.
        -- Multiplica pela necessidade de água da cultura.
        -- Reduz 30% se a cultura é tolerante à seca.
        -- -------------------------------------------------------
        ROUND(
            CAST(
                GREATEST(0,
                    LEAST(10,
                        (1 - (precipitacao_total_mm / 300.0))   -- 0mm=10pts, 300mm=0pts
                        * necessidade_agua_score                 -- multiplica por 1, 2 ou 3
                        * (CASE WHEN tolerante_seca THEN 0.7 ELSE 1.0 END)
                    )
                ) * 10 AS numeric
            ), 1
        ) as risco_seca,

        -- -------------------------------------------------------
        -- RISCO DE FRIO (0-10)
        -- Lógica: quanto a temperatura mínima ficou abaixo do ideal.
        -- -------------------------------------------------------
        ROUND(
            CAST(
                CASE
                    WHEN tolerante_frio THEN 0  -- cultura tolera frio, sem risco
                    WHEN temp_min_ideal IS NULL THEN 0  -- sem dado, não avalia
                    WHEN temp_min_c < temp_min_ideal THEN
                        LEAST(10,
                            (temp_min_ideal - temp_min_c)   -- graus abaixo do ideal
                            * (CASE WHEN sensivel_frio THEN 1.5 ELSE 1.0 END)
                        )
                    ELSE 0
                END AS numeric
            ), 1
        ) as risco_frio,

        -- -------------------------------------------------------
        -- RISCO DE CALOR (0-10)
        -- Lógica: quanto a temperatura máxima ficou acima do ideal.
        -- -------------------------------------------------------
        ROUND(
            CAST(
                CASE
                    WHEN temp_max_ideal IS NULL THEN 0  -- sem dado, não avalia
                    WHEN temp_max_c > temp_max_ideal THEN
                        LEAST(10,
                            (temp_max_c - temp_max_ideal)   -- graus acima do ideal
                            * (CASE WHEN sensivel_calor THEN 1.5 ELSE 1.0 END)
                        )
                    ELSE 0
                END AS numeric
            ), 1
        ) as risco_calor

    from cruzamento

),

-- Calcula o risco total e classifica
com_risco_total as (

    select
        *,

        -- Risco total: média dos 3 riscos (pesos iguais)
        ROUND(
            CAST((risco_seca + risco_frio + risco_calor) / 3.0 AS numeric),
            1
        ) as risco_total,

        -- Classificação qualitativa
        case
            when (risco_seca + risco_frio + risco_calor) / 3.0 < 2.0  then 'Baixo'
            when (risco_seca + risco_frio + risco_calor) / 3.0 < 4.5  then 'Medio'
            when (risco_seca + risco_frio + risco_calor) / 3.0 < 7.0  then 'Alto'
            else                                                             'Critico'
        end as classificacao_risco

    from com_scores

)

select
    -- Chave do registro
    estacao_codigo,
    estacao_nome,
    uf,
    ano,
    estacao_ano,
    cultura,
    categoria,

    -- Clima observado
    temp_media_c,
    temp_max_c,
    temp_min_c,
    precipitacao_total_mm,
    dias_com_chuva,
    umidade_media_pct,

    -- Referência da cultura
    temp_min_ideal,
    temp_max_ideal,
    necessidade_agua_score,
    tolerante_seca,
    tolerante_frio,
    sensivel_frio,
    sensivel_calor,

    -- Scores de risco (0-10)
    risco_seca,
    risco_frio,
    risco_calor,
    risco_total,
    classificacao_risco

from com_risco_total

order by risco_total desc
