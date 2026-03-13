WITH cte_base AS (
    SELECT
        sk_produto,
        review_id,
        avaliacao,
        votos_uteis
    FROM {{ source('external_source', 'ft_avaliacoes') }}
),

cte_dim_produto AS (
    SELECT
        sk_produto,
        product_id,
        nome_produto,
        categoria
    FROM {{ source('external_source', 'dim_produto') }}
    WHERE flag_atual = true
),

join_base_produto AS (
    SELECT
        bs.*,
        pr.product_id,
        pr.nome_produto,
        pr.categoria
    FROM cte_base bs 
    LEFT JOIN cte_dim_produto pr ON pr.sk_produto = bs.sk_produto
),

cte_agregacoes AS (
    SELECT
        product_id,
        nome_produto,
        categoria,
        
        COUNT(avaliacao) AS qtd_avaliacoes,
        
        ROUND(AVG(avaliacao), 2) AS avaliacao_media,
        
        COUNT(avaliacao) FILTER (WHERE avaliacao = 5) AS qtd_avaliacoes_5_estrelas,
        COUNT(avaliacao) FILTER (WHERE avaliacao = 4) AS qtd_avaliacoes_4_estrelas,
        COUNT(avaliacao) FILTER (WHERE avaliacao = 3) AS qtd_avaliacoes_3_estrelas,
        COUNT(avaliacao) FILTER (WHERE avaliacao = 2) AS qtd_avaliacoes_2_estrelas,
        COUNT(avaliacao) FILTER (WHERE avaliacao = 1) AS qtd_avaliacoes_1_estrela,
    
        COUNT(avaliacao) FILTER (WHERE avaliacao >= 4) AS qtd_avaliacoes_positivas,
        COUNT(avaliacao) FILTER (WHERE avaliacao <= 2) AS qtd_avaliacoes_negativas,
        COUNT(avaliacao) FILTER (WHERE avaliacao = 3) AS qtd_avaliacoes_neutras,
        
        SUM(votos_uteis) AS total_votos_uteis
        
    FROM join_base_produto
    WHERE product_id IS NOT NULL
    GROUP BY
        product_id,
        nome_produto,
        categoria
),

cte_metricas AS (
    SELECT
        *,
        CASE 
            WHEN qtd_avaliacoes = 0 THEN NULL
            ELSE ROUND((qtd_avaliacoes_positivas * 100.0 / qtd_avaliacoes), 1)
        END AS percentual_positivas,
        
        CASE 
            WHEN qtd_avaliacoes = 0 THEN NULL
            ELSE ROUND((qtd_avaliacoes_negativas * 100.0 / qtd_avaliacoes), 1)
        END AS percentual_negativas,
        
        CASE 
            WHEN qtd_avaliacoes = 0 THEN NULL
            ELSE ROUND((qtd_avaliacoes_neutras * 100.0 / qtd_avaliacoes), 1)
        END AS percentual_neutras,
        
        CASE 
            WHEN qtd_avaliacoes = 0 THEN NULL
            ELSE ROUND(((qtd_avaliacoes_5_estrelas - qtd_avaliacoes_negativas) * 100.0 / qtd_avaliacoes), 2)
        END AS nps_score
        
    FROM cte_agregacoes
)

SELECT
    product_id,
    nome_produto,
    categoria,
    qtd_avaliacoes,
    avaliacao_media,

    qtd_avaliacoes_5_estrelas,
    qtd_avaliacoes_4_estrelas,
    qtd_avaliacoes_3_estrelas,
    qtd_avaliacoes_2_estrelas,
    qtd_avaliacoes_1_estrela,
    
    qtd_avaliacoes_positivas,
    qtd_avaliacoes_negativas,
    qtd_avaliacoes_neutras,
    
    percentual_positivas,
    percentual_negativas,
    percentual_neutras,

    nps_score,
    
    total_votos_uteis,
    
    CASE 
        WHEN qtd_avaliacoes = 0 THEN 'Sem Avaliações'
        WHEN avaliacao_media >= 4.5 THEN 'Excelente'
        WHEN avaliacao_media >= 4.0 THEN 'Muito Bom'
        WHEN avaliacao_media >= 3.5 THEN 'Bom'
        WHEN avaliacao_media >= 3.0 THEN 'Regular'
        ELSE 'Ruim'
    END AS classificacao_produto

FROM cte_metricas
ORDER BY qtd_avaliacoes DESC, avaliacao_media DESC