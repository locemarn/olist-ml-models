WITH tb_join AS (
  SELECT DISTINCT
          t1.idPedido,
          t1.idCliente,
          t2.idVendedor,
          t3.descUF

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.cliente as t3
  ON t1.idCliente = t3.idCliente

  WHERE dtPedido > '{date}'
  AND dtPedido >= add_months('{date}', -6)
),

tb_group AS (

  SELECT 
    idVendedor,
    
    count(distinct descUF) as qtdUFsPedidos,
    
    count(distinct case when descUF = 'AC'then idPedido end) / count(distinct idPedido) AS pctPedidoAC,
    count(distinct case when descUF = 'AL'then idPedido end) / count(distinct idPedido) AS pctPedidoAL,
    count(distinct case when descUF = 'AM'then idPedido end) / count(distinct idPedido) AS pctPedidoAM,
    count(distinct case when descUF = 'AP'then idPedido end) / count(distinct idPedido) AS pctPedidoAP,
    count(distinct case when descUF = 'BA'then idPedido end) / count(distinct idPedido) AS pctPedidoBA,
    count(distinct case when descUF = 'CE'then idPedido end) / count(distinct idPedido) AS pctPedidoCE,
    count(distinct case when descUF = 'DF'then idPedido end) / count(distinct idPedido) AS pctPedidoDF,
    count(distinct case when descUF = 'ES'then idPedido end) / count(distinct idPedido) AS pctPedidoES,
    count(distinct case when descUF = 'GO'then idPedido end) / count(distinct idPedido) AS pctPedidoGO,
    count(distinct case when descUF = 'MA'then idPedido end) / count(distinct idPedido) AS pctPedidoMA,
    count(distinct case when descUF = 'MG'then idPedido end) / count(distinct idPedido) AS pctPedidoMG,
    count(distinct case when descUF = 'MS'then idPedido end) / count(distinct idPedido) AS pctPedidoMS,
    count(distinct case when descUF = 'MT'then idPedido end) / count(distinct idPedido) AS pctPedidoMT,
    count(distinct case when descUF = 'PA'then idPedido end) / count(distinct idPedido) AS pctPedidoPA,
    count(distinct case when descUF = 'PB'then idPedido end) / count(distinct idPedido) AS pctPedidoPB,
    count(distinct case when descUF = 'PE'then idPedido end) / count(distinct idPedido) AS pctPedidoPE,
    count(distinct case when descUF = 'PI'then idPedido end) / count(distinct idPedido) AS pctPedidoPI,
    count(distinct case when descUF = 'PR'then idPedido end) / count(distinct idPedido) AS pctPedidoPR,
    count(distinct case when descUF = 'RJ'then idPedido end) / count(distinct idPedido) AS pctPedidoRJ,
    count(distinct case when descUF = 'RN'then idPedido end) / count(distinct idPedido) AS pctPedidoRN,
    count(distinct case when descUF = 'RO'then idPedido end) / count(distinct idPedido) AS pctPedidoRO,
    count(distinct case when descUF = 'RR'then idPedido end) / count(distinct idPedido) AS pctPedidoRR,
    count(distinct case when descUF = 'RS'then idPedido end) / count(distinct idPedido) AS pctPedidoRS,
    count(distinct case when descUF = 'SC'then idPedido end) / count(distinct idPedido) AS pctPedidoSC,
    count(distinct case when descUF = 'SE'then idPedido end) / count(distinct idPedido) AS pctPedidoSE,
    count(distinct case when descUF = 'SP'then idPedido end) / count(distinct idPedido) AS pctPedidoSP,
    count(distinct case when descUF = 'TO'then idPedido end) / count(distinct idPedido) AS pctPedidoTO

  FROM tb_join

  GROUP BY idVendedor  
)


SELECT
      '{date}' as dtReference,
      *

FROM tb_group