WITH cte_vendas_90_dias AS (
    SELECT
        vd.sk_produto,
        SUM(vd.quantidade) AS quantidade_vendida_90d
    FROM 
        {{ source('external_source', 'ft_vendas') }} vd
    JOIN {{ source('external_source', 'dim_data') }} dt ON dt.sk_date = vd.sk_data_pedido
    WHERE
        dt.data_completa BETWEEN TODAY() - INTERVAL 90 DAY AND TODAY()
    GROUP BY
        vd.sk_produto
),
cte_dim_produto AS (
    SELECT
        sk_produto,
        product_id,
        nome_produto,
        categoria,
        fornecedor,
        custo,
        preco
    FROM {{ source('external_source', 'dim_produto') }}
    WHERE flag_atual = true
),
cte_estoque_atual AS (
    SELECT
        et.sk_produto,
        et.quantidade_em_estoque,
        et.custo_total_estoque,
        et.valor_total_estoque,
        dt.data_completa AS data_snapshot,
        ROW_NUMBER() OVER(partition by sk_produto ORDER BY data_snapshot DESC) AS rn
    FROM
        {{ source('external_source', 'ft_estoque') }} et
    JOIN {{ source('external_source', 'dim_data') }} dt ON dt.sk_date = et.sk_date

)
SELECT
    e.data_snapshot,
    p.product_id,
    p.nome_produto,
    p.categoria,
    p.fornecedor,
    
    e.quantidade_em_estoque,
    p.custo AS custo_unitario,
    p.preco AS preco_unitario,
    e.custo_total_estoque,
    e.valor_total_estoque,
    ROUND(e.valor_total_estoque - e.custo_total_estoque, 2) AS margem_potencial,
    
    COALESCE(v.quantidade_vendida_90d, 0) AS vendas_ultimos_90d,
    
    ROUND(COALESCE(v.quantidade_vendida_90d, 0) / 90.0,2) AS media_vendas_diarias_90d,

    CASE 
        WHEN COALESCE(v.quantidade_vendida_90d, 0) = 0 THEN NULL
        ELSE ROUND(e.quantidade_em_estoque / (v.quantidade_vendida_90d / 90.0), 1)
    END AS dias_estoque,
    
    CASE 
        WHEN COALESCE(v.quantidade_vendida_90d, 0) = 0 THEN 'Sem Vendas'
        WHEN e.quantidade_em_estoque / (v.quantidade_vendida_90d / 90.0) < 7 THEN 'Crítico'
        WHEN e.quantidade_em_estoque / (v.quantidade_vendida_90d / 90.0) < 15 THEN 'Baixo'
        WHEN e.quantidade_em_estoque / (v.quantidade_vendida_90d / 90.0) < 30 THEN 'Normal'
        ELSE 'Alto'
    END AS status_estoque

FROM cte_estoque_atual e
LEFT JOIN cte_dim_produto p ON e.sk_produto = p.sk_produto
LEFT JOIN cte_vendas_90_dias v ON e.sk_produto = v.sk_produto

