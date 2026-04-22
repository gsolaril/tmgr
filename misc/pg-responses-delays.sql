SELECT
    nzmq, t_recv,
    avg(t_send - t_sign)  AS delay_send,
    avg(t_recv - t_send) AS delay_recv,
    avg(t_exec - t_recv) AS delay_exec,
    avg(t_resp - t_exec) AS delay_resp,
    avg(price_open - price_sign) AS slippage
FROM public.tc_responses
GROUP BY nzmq, t_recv order by t_recv