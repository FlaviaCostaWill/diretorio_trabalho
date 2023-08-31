with chats_agg as (
	select 
	date_trunc('month', cast(c.dt_chat as date)) as dt_mes
	, c.cpf_customer
	-- problema?
	, max(case when tipo_de_tag = 'Problema' then 1 else 0 end) as flag_problema
	--finalizado por automacao
	, max(is_resolvido_bot) as is_resolvido_bot
	, max(is_finalizacao_automacao) as is_finalizacao_automacao
	, max(nr_tpr) as nr_tpr
	, max(nr_tmr) as nr_tmr
	, max(nr_tma) as nr_tma
	, max(nr_tma_resolucao) as nr_tma_resolucao
	, max(nr_tmat) as nr_tmat --tr
	, min(nr_csat) as nr_csat
	FROM cx_curated_zone.cx_contact_rate c
	left join cx_curated_zone.helpshift_issues AS issues_macro ON c.id_issue = issues_macro.id_issue
	left join cx_curated_zone.helpshift_tag_niveis AS helpshift_tag ON c.id_issue = helpshift_tag.id_issue
	left join processed_zone_api_cxm_tags.tags_hierarquia_gerencial as novas_tags ON trim(lower(n3)) = trim(lower(nm_tag_n3))
	where dt_chat >= to_date('2022-03-01','yyyy-mm-dd') and dt_chat < to_date('2023-03-01','yyyy-mm-dd')
	and c.id_issue is not null
	group by 1,2
)
, de_para as (
select distinct
dt_mes 
, date_add('day', -91, dt_mes) as periodo_antes_inicio
, date_add('day', -1, dt_mes) as periodo_antes_fim
, date_add('month', 1, dt_mes) as periodo_depois_inicio
, date_add('month', 3, dt_mes) as periodo_depois_fim
, date_add('month', 2, dt_mes) as periodo_depois30_fim
from chats_agg
)
, spending_antes_total as (
select
s.cpf
, c.dt_mes
, periodo_antes_inicio
, periodo_antes_fim
, periodo_depois_inicio
, periodo_depois_fim
, periodo_depois30_fim
, sum(s.vl_transacao) as vl_compra_antes_90d
, count(distinct s.id_autorizacao) as qt_compra_antes_90d
, min(bc.dt_criacao_conta) as dt_criacao_conta
, max(s.dt_dia) as dt_last_use_credit
, min(a.dt_first_use_credit) as dt_first_use_credit
, max(gc.vl_current_limit) as vl_current_limit
, count(distinct substring(cast(ap.dt_event as varchar), 1, 10)) as dias_uso_app
from platform_curated_zone.transaction_dedicada_spending s 
inner join  de_para c
			on (cast(s.dt_dia as date) >= c.periodo_antes_inicio and --IGUAL OU MAIOR E IGUAL
				cast(s.dt_dia as date) <= c.periodo_antes_fim
				)
left join (
		select
		cpf
		, id_customer  
		, min(cd_yearmonth) as dt_criacao_conta
		from customer_curated_zone.ca_book_cliente
		group by 1,2
	)	bc 
	on (bc.cpf = s.cpf)			
left join customer_curated_zone.ca_book_status_cliente a 
					on (a.id_customer = bc.id_customer)				
--limite
left join customer_curated_zone.ca_book_gestao_carteira gc 
		on (s.cpf = gc.cpf 
		and cast(replace(substring(cast(c.dt_mes as varchar),1,7),'-','') as double)  = gc.cd_yearmonth)
--uso app
left join customer_curated_zone.ca_analitico_geoloc ap on (
				ap.cpf = s.cpf and				
				ap.dt_event >= c.periodo_antes_inicio and
				ap.dt_event <= c.periodo_antes_fim)
where s.vl_transacao > 0
and bc.dt_criacao_conta is not null
and gc.vl_current_limit is not null
group by 1,2,3,4,5,6,7
order by cpf
)
, nps_antes as (
select cd_cpf as cpf
, c.dt_mes
, sum(case when ds_lealdade_nps = 'promotor' then 1 else 0 end) as cont_promotor_antes
, sum(case when ds_lealdade_nps = 'neutro' then 1 else 0 end) as cont_neutro_antes
, sum(case when ds_lealdade_nps = 'detrator' then 1 else 0 end) as cont_detrator_antes
from cx_curated_zone.indecx_nps n
inner join  de_para c
			on (cast(n.dt_resposta as date) >= c.periodo_antes_inicio and --IGUAL OU MAIOR E IGUAL
				cast(n.dt_resposta as date) <= c.periodo_antes_fim
				)
group by 1,2
)
, nps_depois as (
select cd_cpf as cpf
, c.dt_mes
, sum(case when ds_lealdade_nps = 'promotor' then 1 else 0 end) as cont_promotor_depois
, sum(case when ds_lealdade_nps = 'neutro' then 1 else 0 end) as cont_neutro_depois
, sum(case when ds_lealdade_nps = 'detrator' then 1 else 0 end) as cont_detrator_depois
from cx_curated_zone.indecx_nps n
inner join  de_para c
			on (cast(n.dt_resposta as date) >= c.periodo_depois_inicio and
				cast(n.dt_resposta as date) <= c.periodo_depois_fim
				)
group by 1,2
)
, base_total_add_chats as (
select 
s.cpf
, s.dt_mes as mes_ref
, s.periodo_antes_inicio
, s.periodo_antes_fim
, s.periodo_depois_inicio
, s.periodo_depois_fim
, s.periodo_depois30_fim
, s.vl_compra_antes_90d
, s.qt_compra_antes_90d
, s.dt_last_use_credit
, s.dt_first_use_credit
, s.dt_criacao_conta
, s.vl_current_limit
, s.dias_uso_app
, c.*
, case when c.cpf_customer is null then 0 else 1 end as usou_chat
from 
spending_antes_total s
left join chats_agg c on (s.cpf = c.cpf_customer and c.dt_mes = s.dt_mes)
)
--incluindo ordenaçao aleatoria
, base_total_ord_rand as (
select 
*
, row_number() over (partition by dt_mes, usou_chat order by rand()) as sorteio
from base_total_add_chats
)
-- 10mil com e sem chats
, base_amostra as (
select *
from base_total_ord_rand
where sorteio <= 10000000
)
, spending_depois60d as (
select
c.cpf
, c.dt_mes
, sum(s.vl_transacao) as vl_compra_depois_60d
, count(distinct s.id_autorizacao) as qt_compras_depois_60d
from base_amostra c    
left join platform_curated_zone.transaction_dedicada_spending s
			on (c.cpf = s.cpf and
				cast(s.dt_dia as date) >= c.periodo_depois_inicio and
				cast(s.dt_dia as date) <= c.periodo_depois_fim
				)
where s.vl_transacao > 0
group by 1,2
)
, spending_depois30d as (
select
c.cpf
, c.dt_mes
, sum(s.vl_transacao) as vl_compra_depois_30d
, count(distinct s.id_autorizacao) as qt_compras_depois_30d
from base_amostra c    
left join platform_curated_zone.transaction_dedicada_spending s
			on (c.cpf = s.cpf and
				cast(s.dt_dia as date) >= c.periodo_depois_inicio and
				cast(s.dt_dia as date) <= c.periodo_depois30_fim
				)
where s.vl_transacao > 0
group by 1,2
)
, base_final as (
select 
a.*
, n1.cont_promotor_antes
, n1.cont_neutro_antes
, n1.cont_detrator_antes
, n2.cont_promotor_depois
, n2.cont_neutro_depois
, n2.cont_detrator_depois
, d1.vl_compra_depois_30d
, d1.qt_compras_depois_30d
, d2.vl_compra_depois_60d
, d2.qt_compras_depois_60d
from base_amostra a
left join spending_depois30d d1 on (a.cpf = d1.cpf and a.mes_ref = d1.dt_mes)
left join spending_depois60d d2 on (a.cpf = d2.cpf and a.mes_ref = d2.dt_mes)
left join nps_antes n1 on (a.cpf = n1.cpf and a.mes_ref = n1.dt_mes)
left join nps_depois n2 on (a.cpf = n2.cpf and a.mes_ref = n2.dt_mes)
)
--select * from base_final
--limit 5
select
usou_chat
, count(distinct cpf) as cpf
, sum(cont_promotor_antes) as cont_promotor_antes
, sum(cont_neutro_antes) as cont_neutro_antes
, sum(cont_detrator_antes) as cont_detrator_antes
, sum(cont_promotor_depois) as cont_promotor_depois
, sum(cont_neutro_depois) as cont_neutro_depois
, sum(cont_detrator_depois) as cont_detrator_depois
from base_final
group by 1





select * from cx_curated_zone.indecx_nps
limit 5


--------------------------------------- CONTAR CHURN DOS SEM ATENDIMENTO ------------------------


WITH chats AS (SELECT cpf_customer, count(DISTINCT id_issue) qtd_chats FROM cx_curated_zone.helpshift_issues  
                 WHERE dt_criacao_chat >= to_date('2022-03-01','yyyy-mm-dd') AND dt_criacao_chat < to_date('2023-03-01','yyyy-mm-dd')
                 GROUP BY 1)
, chats_agg AS (
SELECT date_trunc('month', cast(c.dt_chat as date)) as dt_mes
                          , c.cpf_customer
                          -- problema?
                          , max(case when tipo_de_tag = 'Problema' then 1 else 0 end) as flag_problema
                          --finalizado por automacao
                          , max(nr_tpr) as nr_tpr
                          , max(nr_tma) as nr_tma
                          , max(nr_tmat) as nr_tmat
                          , max(nm_tag_hierarquia_1) as hierarquia_1
                     FROM cx_curated_zone.cx_contact_rate c
                LEFT JOIN cx_curated_zone.helpshift_issues AS issues_macro ON c.id_issue = issues_macro.id_issue
                LEFT JOIN cx_curated_zone.helpshift_tag_niveis AS helpshift_tag ON c.id_issue = helpshift_tag.id_issue
                LEFT JOIN processed_zone_api_cxm_tags.tags_hierarquia_gerencial as novas_tags ON trim(lower(n3)) = trim(lower(nm_tag_n3))
               INNER JOIN chats AS cht ON cht.cpf_customer = c.cpf_customer 
               						AND cht.qtd_chats < 4 
                    WHERE dt_chat >= to_date('2023-01-01','yyyy-mm-dd') and dt_chat < to_date('2023-04-01','yyyy-mm-dd')
                      AND c.id_issue is not null and issues_macro.nr_tmat > 90 and nm_tag_hierarquia_1 <> 'Crédito e Cobrança'
                 GROUP BY 1,2
                 )
, de_para AS (SELECT DISTINCT dt_mes 
                              , date_add('day', -91, dt_mes) as periodo_antes_inicio
                              , date_add('day', -1, dt_mes) as periodo_antes_fim
                              , date_add('month', 1, dt_mes) as periodo_depois_inicio
                              , date_add('month', 3, dt_mes) as periodo_depois_fim
                              , date_add('month', 2, dt_mes) as periodo_depois30_fim
                         FROM chats_agg)
, spending_antes_total_ AS (SELECT distinct s.cpf
                                  , s.id_autorizacao
                                  , c.dt_mes
                                  , periodo_antes_inicio
                                  , periodo_antes_fim
                                  , periodo_depois_inicio
                                  , periodo_depois_fim
                                  , periodo_depois30_fim
                                  , s.vl_transacao as vl_compra_antes_90d
                                  --, count(distinct s.id_autorizacao) as qt_compra_antes_90d
                                  , bc.dt_criacao_conta as dt_criacao_conta
                                  , s.dt_dia as dt_last_use_credit
                                  , a.dt_first_use_credit as dt_first_use_credit
                                  , gc.vl_current_limit as vl_current_limit
                             FROM platform_curated_zone.transaction_dedicada_spending s 
                       INNER JOIN de_para c
                               ON (cast(s.dt_dia as date) >= c.periodo_antes_inicio 
                              AND cast(s.dt_dia as date) <= c.periodo_antes_fim)--IGUAL OU MAIOR E IGUAL
                        LEFT JOIN (SELECT cpf
                                          , id_customer  
                                          , min(cd_yearmonth) as dt_criacao_conta
                             FROM customer_curated_zone.ca_book_cliente
                         GROUP BY 1,2)bc 
                               ON (bc.cpf = s.cpf)
                        LEFT JOIN customer_curated_zone.ca_book_status_cliente a 
                               ON (a.id_customer = bc.id_customer)--limite
                        LEFT JOIN customer_curated_zone.ca_book_gestao_carteira gc 
                               ON (s.cpf = gc.cpf 
                              AND cast(replace(substring(cast(c.dt_mes as varchar),1,7),'-','') as double)  = gc.cd_yearmonth)
                            WHERE s.vl_transacao > 0 
                              AND bc.dt_criacao_conta IS NOT NULL
                              AND gc.vl_current_limit IS NOT NULL)
                         , spending_antes_total AS (select cpf
                                ,dt_mes  
                                ,periodo_antes_inicio
                                ,periodo_antes_fim
                                ,periodo_depois_fim
                                ,periodo_depois30_fim
                                ,periodo_depois_inicio
                                ,dt_criacao_conta
                                ,sum(vl_compra_antes_90d) as vl_compra_antes_90d
                                ,count(distinct id_autorizacao) as qt_compra_antes_90d
                                ,max(dt_last_use_credit) as dt_last_use_credit
                                ,min(dt_first_use_credit) as dt_first_use_credit
                                ,max(vl_current_limit) as vl_current_limit
                         from spending_antes_total_
                         group by 1,2,3,4,5,6,7,8)
            , base_total_add_chats AS (SELECT s.cpf
                                  , s.dt_mes as dt_mes_true
                                  , s.periodo_antes_inicio
                                  , s.periodo_antes_fim
                                  , s.periodo_depois_inicio
                                  , s.periodo_depois_fim
                                  , s.periodo_depois30_fim
                                  , s.vl_compra_antes_90d
                                  , s.qt_compra_antes_90d
                                  , s.dt_last_use_credit
                                  , s.dt_first_use_credit
                                  , s.dt_criacao_conta
                                  , s.vl_current_limit
                                 -- , s.dias_uso_app
                                  , c.*
                                  , case when c.cpf_customer is null then 0 else 1 end as usou_chat
                             FROM spending_antes_total s
                        LEFT JOIN chats_agg c on (s.cpf = c.cpf_customer and c.dt_mes = s.dt_mes))
	, base_total_ord_rand AS (SELECT *
                                 , row_number() over (partition by dt_mes_true, usou_chat order by rand()) as sorteio
                            FROM base_total_add_chats)
         , spending_depois_30 as (select c.cpf
                                    , c.dt_mes_true
                                    , sum(s_30.vl_transacao) as vl_compra_depois_30d
                                    , count(distinct s_30.id_autorizacao) as qt_compras_depois_30d
                                    from base_total_ord_rand c    
                            left join platform_curated_zone.transaction_dedicada_spending s_30
                                     on (c.cpf = s_30.cpf and
                                     cast(s_30.dt_dia as date) >= c.periodo_depois_inicio and
                                     cast(s_30.dt_dia as date) <= c.periodo_depois30_fim)
                                group by 1,2)
			, spending_depois_60 as (
         			select c.cpf
                                        , c.dt_mes_true
                                        , sum(s_60.vl_transacao) as vl_compra_depois_60d
                                        , count(distinct s_60.id_autorizacao) as qt_compras_depois_60d
                                    from base_total_ord_rand c    
                                left join platform_curated_zone.transaction_dedicada_spending s_60
                                      on (c.cpf = s_60.cpf and
                                    cast(s_60.dt_dia as date) >= c.periodo_depois_inicio and
                                    cast(s_60.dt_dia as date) <= c.periodo_depois_fim)
                                group by 1,2
                                )
 				,  union_ltv as (
					select distinct 
					mesref
					, dias_atraso 
					, cpf
					, spending_credito
					, rec_itr
					, rec_recarga
					, rec_demais_encargos
					, desp_pdd
					, rec_juros
					, custos
					, ltv
					from public.ltv_202305_diego_camilo 
					union all 
					select distinct 
					mesref
					, dias_atraso 
					, cpf
					, spending_credito 
					, rec_itr
					, rec_recarga
					, rec_demais_encargos
					, desp_pdd
					, rec_juros
					, custos
					, ltv
					from public.ltv_202305_will_diego_camilo 
					)
                    SELECT  a.cpf
                                , a.dt_mes_true
                                , a.periodo_antes_inicio
                                , a.periodo_antes_fim
                                , a.periodo_depois_inicio
                                , a.periodo_depois_fim
                                , a.periodo_depois30_fim
                                , a.qt_compra_antes_90d
                                , a.dt_last_use_credit
                                , a.vl_current_limit
                                , a.flag_problema
                                , a.nr_tpr
                                , a.nr_tma
                                , a.nr_tmat
                                , a.usou_chat
                                , a.hierarquia_1
                                , gro_.ds_gender
                                , gro_.ds_state_abbreviation
                                , gro_.dt_birth
                                , gro_.ds_schooling
                                , gro_.ds_marital_status
                                , gro_.dt_opening_account
                                , gro_.ds_city_size
                                , max(a.vl_compra_antes_90d) vl_compra_antes_90d
                                , min(a.dt_first_use_credit) dt_first_use_credit
                                , max(gro_.vl_renda_declarada) vl_renda_declarada
                                , min(a.dt_criacao_conta) dt_criacao_conta
                                , count(distinct substring(cast(dt_event as varchar), 1, 10)) as dias_uso_app
                                , max(d1.vl_compra_depois_30d) vl_compra_depois_30d
                                , max(d1.qt_compras_depois_30d) qt_compras_depois_30d
                                , max(d2.vl_compra_depois_60d) vl_compra_depois_60d
                                , max(d2.qt_compras_depois_60d) qt_compras_depois_60d
                                , max(l.spending_credito) as spending_ltv
                                , max(l.rec_itr) as rec_itr
                                , max(l.rec_recarga) as rec_recarga
				, max(l.rec_demais_encargos) as rec_demais_encargos
				, max(l.desp_pdd) as desp_pdd
				, max(l.rec_juros) as rec_juros
				, max(l.custos) as custos
                                , max(l.ltv) as ltv
                                , max(l.dias_atraso) as dias_atraso_ltv
                           FROM base_total_ord_rand a
                      LEFT JOIN customer_curated_zone.ca_book_cliente b_c ON (a.cpf = b_c.cpf)
                      LEFT JOIN customer_curated_zone.ca_book_growth gro_ ON gro_.id_customer = b_c.id_customer 
                      LEFT JOIN customer_curated_zone.ca_analitico_geoloc ap on (
                                  ap.cpf = a.cpf and
                                  ap.dt_event >= a.periodo_antes_inicio and
                                  ap.dt_event <= a.periodo_antes_fim)
                       LEFT JOIN spending_depois_30 d1 on (a.cpf = d1.cpf and a.dt_mes_true = d1.dt_mes_true)
                      LEFT JOIN spending_depois_60 d2 on (a.cpf = d2.cpf and a.dt_mes_true = d2.dt_mes_true)
                      left join union_ltv l on (a.cpf = l.cpf and a.dt_mes_true = cast(concat(l.mesref, '-01') as date))
                      where usou_chat = 0
                      group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
                      



select * from customer_curated_zone.ca_book_status_cliente limit 5








------------------------ query simulação - CONTAGEM CLIENTES PARA USO ELASTICIDADE ------------------------


with chats_agg as (
	select 
	date_trunc('month', cast(c.dt_chat as date)) as dt_mes
	, c.cpf_customer as cpf
	-- problema?
	, count(distinct c.id_issue) as chats_mes
	, max(case when tipo_de_tag = 'Problema' then 1 else 0 end) as flag_problema
	--finalizado por automacao
	, max(is_resolvido_bot) as is_resolvido_bot
	, max(is_finalizacao_automacao) as is_finalizacao_automacao
	, max(nr_tpr) as nr_tpr
	, max(nr_tmr) as nr_tmr
	, max(nr_tma) as nr_tma
	, max(nr_tma_resolucao) as nr_tma_resolucao
	, max(nr_tmat) as nr_tmat
	, min(nr_csat) as nr_csat
	FROM cx_curated_zone.cx_contact_rate c
	left join cx_curated_zone.helpshift_issues AS issues_macro ON c.id_issue = issues_macro.id_issue
	left join cx_curated_zone.helpshift_tag_niveis AS helpshift_tag ON c.id_issue = helpshift_tag.id_issue
	left join processed_zone_api_cxm_tags.tags_hierarquia_gerencial as novas_tags ON trim(lower(n3)) = trim(lower(nm_tag_n3))
	where dt_chat >= to_date('2022-04-01','yyyy-mm-dd') and dt_chat < to_date('2023-04-01','yyyy-mm-dd')
	and c.id_issue is not null
	group by 1,2
)
, de_para as (
select distinct
dt_mes 
--, date_add('month', -3, dt_mes) as periodo_antes_inicio
--, date_add('day', -1, dt_mes) as periodo_antes_fim
, date_add('month', 1, dt_mes) as periodo_depois_inicio
, date_add('month', 3, dt_mes) as periodo_depois_fim
--, date_add('month', 2, dt_mes) as periodo_depois30_fim
from chats_agg
)
, spending_depois60d as (
select
c.cpf
, c.dt_mes
, sum(s.vl_transacao) as vl_compra_depois_60d
, count(distinct s.id_autorizacao) as qt_compras_depois_60d
, count(distinct s.cpf) as ativos_depois_60d
from chats_agg c
left join platform_curated_zone.transaction_dedicada_spending s
on c.cpf = s.cpf
inner join  de_para c2
			on (c.dt_mes = c2.dt_mes and
				cast(s.dt_dia as date) >= c2.periodo_depois_inicio and
				cast(s.dt_dia as date) <= c2.periodo_depois_fim
				)
where s.vl_transacao > 0
group by 1,2
)
, base_clientes as (
select 
a.*
, sum(chats_mes) over (partition by a.cpf) as frequencia_total
, d2.vl_compra_depois_60d
, d2.ativos_depois_60d
from chats_agg a
left join spending_depois60d d2 on (a.cpf = d2.cpf and a.dt_mes = d2.dt_mes)
)
select 
dt_mes
, count(distinct cpf) as clientes_chat_total
, count(distinct case when nr_tmat > 90 and frequencia_total < 4 and flag_problema = 1 then cpf end) as clientes_chat_recorte
, sum(vl_compra_depois_60d) as vl_compra_depois_60d
, sum(case when nr_tmat > 90 and frequencia_total < 4  and flag_problema = 1  then vl_compra_depois_60d end) as vl_compra_depois_60d_recorte
, sum(ativos_depois_60d) as ativos_depois_60d
from base_clientes
group by 1



---------------------------- OUTROS TESTES --------------------------------

SELECT
    COUNT(DISTINCT "cliente"."cpf") AS CLIENTES
FROM
    "customer_curated_zone"."ca_book_cliente" AS "cliente"
    LEFT JOIN "customer_curated_zone"."ca_book_cobranca" AS "cliente_cobranca" 
    	ON "cliente"."id_customer" = "cliente_cobranca"."id_customer"     	
    AND "cliente"."cd_yearmonth" = "cliente_cobranca"."cd_yearmonth_due_date"
    INNER JOIN "customer_curated_zone"."ca_book_status_cliente" AS "cliente_status" 
    	ON "cliente"."id_customer" = "cliente_status"."id_customer"
INNER JOIN "customer_curated_zone"."ca_book_growth" AS "cliente_growth" 
		ON "cliente"."id_customer" = "cliente_growth"."id_customer"
LEFT JOIN "customer_curated_zone"."ca_book_cartao" AS "ca_book_cartao" 
    	ON "cliente"."id_customer" = "ca_book_cartao"."id_customer"     	
    AND "cliente"."cd_yearmonth" = "ca_book_cartao"."cd_yearmonth"
    and "ca_book_cartao"."cd_yearmonth" in (202304, 202305)
LEFT JOIN cx_curated_zone.helpshift_issues I ON I.cpf_customer = cliente.cpf
WHERE "cliente_status"."dt_first_use_credit" < (TIMESTAMP '2023-03-01')
and coalesce(cliente_cobranca.nr_days_paste_due_closed, 0) <= 5
and DATE_FORMAT(cliente_status.dt_card_unlock , '%Y-%m-%d') is not null
and cliente_growth.ds_account_type <> 'contapura'
and I.cpf_customer is NULL
and ca_book_cartao.id_customer is null





with base_ref as (
select distinct
cliente.cpf
, "cliente"."cd_yearmonth" data_cliente
, "ca_book_cartao"."cd_yearmonth" data_cartao
, ("ca_book_cartao"."cd_yearmonth") - ("cliente"."cd_yearmonth") compra_meses_a_frente
, ca_book_cartao.id_customer
, I.cpf_customer
, cast(to_char("cliente_status"."dt_first_use_credit", 'yyyymm') as double) data_ativacao
FROM
    "customer_curated_zone"."ca_book_cliente" AS "cliente"
    LEFT JOIN "customer_curated_zone"."ca_book_cobranca" AS "cliente_cobranca" 
    	ON "cliente"."id_customer" = "cliente_cobranca"."id_customer"     	
    AND "cliente"."cd_yearmonth" = "cliente_cobranca"."cd_yearmonth_due_date"
    INNER JOIN "customer_curated_zone"."ca_book_status_cliente" AS "cliente_status" 
    	ON "cliente"."id_customer" = "cliente_status"."id_customer"
INNER JOIN "customer_curated_zone"."ca_book_growth" AS "cliente_growth" 
		ON "cliente"."id_customer" = "cliente_growth"."id_customer"
LEFT JOIN "customer_curated_zone"."ca_book_cartao" AS "ca_book_cartao" 
    	ON "cliente"."id_customer" = "ca_book_cartao"."id_customer"     	
    	and "ca_book_cartao"."cd_yearmonth" <= 202305
LEFT JOIN cx_curated_zone.helpshift_issues I ON I.cpf_customer = cliente.cpf
WHERE coalesce(cliente_cobranca.nr_days_paste_due_closed, 0) <= 5
and "cliente"."cd_yearmonth" in (202301, 202302, 202303)
and cliente_growth.ds_account_type <> 'contapura'
and I.cpf_customer is NULL
)
, ativados as (
select * from base_ref
where data_ativacao < data_cliente
and compra_meses_a_frente < 3
)
select 
count(distinct cpf) as clientes
from (
	select 
	cpf
	,	max(compra_meses_a_frente) as compra_meses_a_frente
	from ativados
	group by 1
) where compra_meses_a_frente <= 0 




select cast(to_char((TIMESTAMP '2023-03-01'), 'yyyymm') as double)



select * from customer_curated_zone.ca_model_ie_cliente 
limit 5












select 
	*
	FROM cx_curated_zone.cx_contact_rate c
 	left join "cx_curated_zone"."helpshift_issues" AS issues_macro ON c."id_issue" = issues_macro."id_issue"
	left join "cx_curated_zone"."helpshift_tag_niveis" AS helpshift_tag ON c."id_issue" = helpshift_tag."id_issue"
	left join processed_zone_api_cxm_tags.tags_hierarquia_gerencial as novas_tags ON trim(lower(n3)) = trim(lower(nm_tag_n3))
	where dt_chat >= to_date('2022-03-01','yyyy-mm-dd')
	and c.id_issue is not null







select * from 
cx_curated_zone.cx_contact_rate
where id_issue is not null