WITH cte_base AS (
    SELECT
        sk_produto,
        review_id,
        avaliacao,
        votos_uteis
    FROM
        {{ source('external_source', 'ft_avaliacoes') }}

),
cte_dim_produto AS (
    SELECT
        sk_produto,
        product_id,
        nome_produto,
        categoria
    FROM
        {{ source('external_source', 'dim_produto') }}
),
join_base_produto AS (
    SELECT
        bs.*,
        product_id,
        nome_produto,
        categoria
    FROM
        cte_base bs 
    LEFT JOIN cte_dim_produto pr ON pr.sk_produto = bs.sk_produto
),
cte_agregacoes AS (
    SELECT
        product_id,
        nome_produto,
        categoria,
        ROUND(AVG(avaliacao),2) AS avaliacao_media,
        COUNT(avaliacao) FILTER ( WHERE  avaliacao = 5) AS qtd_avaliacoes_5_estrelas,
        COUNT(avaliacao) FILTER ( WHERE  avaliacao = 1) AS qtd_avaliacoes_1_estrelas,
        COUNT(avaliacao) FILTER ( WHERE  avaliacao IN(4,5)) AS qtd_avaliacoes_positivas,
        COUNT(avaliacao) FILTER ( WHERE  avaliacao IN(1,2)) AS qtd_avaliacoes_negativas,
        SUM(votos_uteis) AS total_votos_uteis,
        COUNT(avaliacao) AS qtd_avaliacoes
    FROM
        join_base_produto
    GROUP BY
        product_id,
        nome_produto,
        categoria, 
),
cte_nps AS (
    SELECT
        *,
        ROUND((qtd_avaliacoes_positivas / qtd_avaliacoes) * 100,0) AS percentual_positivas,
        ROUND((qtd_avaliacoes_negativas / qtd_avaliacoes) * 100,0) AS percentual_negativas,
        ROUND(((qtd_avaliacoes_positivas - qtd_avaliacoes_negativas) / qtd_avaliacoes) * 100,2) AS nps_score
    FROM
        cte_agregacoes
)
SELECT
    *
FROM
    cte_nps