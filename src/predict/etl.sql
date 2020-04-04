DROP TABLE IF EXISTS PRE_ABT_{stage}_CHURN;
CREATE TABLE PRE_ABT_{stage}_CHURN AS

        SELECT t1.seller_id,
                MAX( t1.dt_venda ) AS dt_ultima_venda,
                
                /* Totais */
                SUM( t1.price ) AS receita_total,
                COUNT( DISTINCT t1.order_id ) AS qtde_vendas,
                COUNT(t1.product_id ) AS qtde_itens_total,
                COUNT( DISTINCT  t1.product_id ) AS qtde_itens_dsc_total,
                SUM( t1.freight_value ) AS frete_total,

                /* Médias por pedido (venda) */
                SUM( t1.price ) / COUNT( DISTINCT t1.order_id ) AS receita_por_venda,
                COUNT(t1.product_id ) / COUNT( DISTINCT t1.order_id )AS qtde_itens_por_venda,
                SUM( t1.freight_value ) / COUNT( DISTINCT t1.order_id )AS frete_por_venda,
                SUM( t1.freight_value ) / COUNT( t1.product_id  )AS frete_por_item,

                COUNT( DISTINCT strftime( "%m", dt_venda) ) / 6. AS prop_ativacao

        FROM(
                SELECT t1.order_id,
                        t1.order_purchase_timestamp AS dt_venda,
                        CASE WHEN t1.order_delivered_customer_date > t1.order_estimated_delivery_date THEN 1 ELSE 0 END AS FLAG_ATRASO,
                        t2.seller_id,
                        t2.product_id,
                        t2.price,
                        t2.freight_value,
                        t3.seller_state,
                        t4.product_category_name

                FROM tb_orders AS t1 -- Tabela de pedidos

                LEFT JOIN tb_order_items as t2 -- Tabela de pedido/item
                ON t1.order_id = t2.order_id

                LEFT JOIN tb_sellers AS t3 -- Tabela de vendedores
                ON t2.seller_id = t3.seller_id

                LEFT JOIN tb_products as t4 -- Tabela de produtos(item)
                ON t2.product_id = t4.product_id

                WHERE t1.order_purchase_timestamp < '{date}'
                AND t1.order_purchase_timestamp >= date( '{date}', '-6 month' )
                AND t1.order_status = 'delivered'
        ) AS t1

        GROUP BY t1.seller_id

        HAVING MAX( t1.dt_venda ) >=  date( '{date}', '-3 month' )
;