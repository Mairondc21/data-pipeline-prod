WITH cte_base AS (
    SELECT
        sk_vendas,
        sk_produto,
        order_id,
        quantidade,
        preco_unitario,
        desconto,
        frete,
        subtotal,
        total,
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
        preco_unitario,
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

)
SELECT product_id, qtd_pedidos, unidades_vendidas, receita_total, custo_total_vendido, preco_medio_vendido,desconto_medio_percentual  FROM cte_agregacoes order by product_id
