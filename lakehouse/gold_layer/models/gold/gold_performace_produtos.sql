WITH cte_base AS (
    SELECT
        sk_produto,
        order_id,
        quantidade,
        preco_unitario,
        desconto,
        subtotal,
        margem_bruta
    FROM
        {{ source('external_source','ft_vendas') }}
),
cte_dim_produto AS (
    SELECT
        sk_produto,
        product_id,
        nome_produto,
        categoria,
        fornecedor,
        custo
    FROM
        {{ source('external_source','dim_produto') }}
    where flag_atual = True
),
join_produto_base AS (
    SELECT
        bs.*,
        pr.product_id,
        pr.nome_produto,
        pr.categoria,
        pr.fornecedor,
        pr.custo
    FROM
        cte_base bs
    LEFT JOIN cte_dim_produto pr ON bs.sk_produto = pr.sk_produto
),
cte_agregacoes AS (
    SELECT
        product_id,
        nome_produto,
        categoria,
        fornecedor,
        COUNT( DISTINCT order_id) AS qtd_pedidos,
        SUM(quantidade) AS unidades_vendidas,
        ROUND(SUM(subtotal),2) AS receita_total,
        custo AS custo_unitario_atual,
        preco_unitario as preco_unitario_atual,
        ROUND((quantidade * custo),2) AS custo_total_vendido,
        margem_bruta,
        ROUND(AVG(preco_unitario),2) AS preco_medio_vendido,
        ROUND((SUM(desconto) / SUM(subtotal)) * 100,2) AS desconto_medio_percentual
    FROM
        join_produto_base
    GROUP BY
        product_id,
        nome_produto,
        categoria,
        fornecedor,
        custo,
        preco_unitario,
        custo_total_vendido,
        margem_bruta

),
cte_ranking AS (
    SELECT
        *,
        ROUND(((receita_total - custo_total_vendido) / receita_total) * 100,2) AS margem_percentual,
        RANK() OVER(ORDER BY receita_total DESC) AS ranking_receita,
        RANK() OVER(ORDER BY unidades_vendidas DESC) AS ranking_volume
    FROM
        cte_agregacoes
)
SELECT
    *
FROM
    cte_ranking