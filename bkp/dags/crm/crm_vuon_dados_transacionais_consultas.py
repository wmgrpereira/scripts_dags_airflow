tasks = {
        "t_anuidade_produto":{
            "sql": """SELECT p.ds_produto as ds_produto,
                            p.id_produto as id_produto,
                            p.vl_faturamento_minimo as vl_faturamento_minimo
                        FROM vuon_db1.t_anuidade_produto p"""
        },
        "t_fatura":{
            "sql": """SELECT i.id_fatura as id_fatura,
					i.id_conta as id_conta,
					i.dt_vencimento as dt_vencimento_fatura,
					i.fl_ativa as fl_ativa,
					i.fl_envia_sms_directone as fl_servico_sms,
					i.vl_fatura as vlr_fatura
                    FROM
                        vuon_db1.t_fatura i
                    WHERE
                    YEAR(i.dt_vencimento) || MONTH (i.dt_vencimento) =
                    YEAR(from_unixtime(unix_timestamp())) || MONTH(from_unixtime(unix_timestamp()))"""
        },
        "t_seguros":{
            "sql":"""select tkr.vl_resposta as CPF,
                            tka.id_produto as id_produto
                        from vuon_db1.t_klok_campo    tkc,
                            vuon_db1.t_klok_resposta tkr,
                            vuon_db1.t_klok_adesao   tka
                        where tkc.ds_codigo = 'contratante.dadosPessoais.cpf'
                        and tkc.id_campo = tkr.id_campo
                        and tkr.id_adesao = tka.id_adesao
                        and tka.ds_status IN ('ATIVA', 'PENDENTE_CONFIRMACAO_PROVEDOR', 'INCONSISTENTE')
                        and tka.id_produto IN  ('1','2','3', '4','5')"""
        },
        "t_venda":{
            "sql": """SELECT tv.id_conta as id_conta,
                            max(tv.dt_venda) as dt_ultima_compra,
                            min(tv.dt_venda) as dt_primeira_compra
                        FROM vuon_db1.t_venda tv
                        WHERE tv.fl_status = 'A'
                        GROUP BY tv.id_conta"""
        },
        "t_pagamento_fatura":{
            "sql": """select --f.id_pagamento_fatura     as id_pagamento_fatura,
                             f.id_fatura               as id_fatura,
                             f.id_conta                as id_conta,
                             f.dt_pagamento            as dt_pagamento_ult_fatura,
                             f.vl_pagamento            as vlr_pago
                            from vuon_db1.t_pagamento_fatura f
                            WHERE
                            YEAR(f.dt_pagamento) || MONTH (f.dt_pagamento) =
                            YEAR(from_unixtime(unix_timestamp())) || MONTH(from_unixtime(unix_timestamp()))"""
        },
        "ult_fat_emitida":{
            "sql":"""select vw.id_conta as id_conta,
						vw.id_fatura  as id_fatura
					from vuon_db1.vw_ultima_fatura vw"""
        },
        "t_conta":{
            "sql":"""select id_conta,
                         nu_cpf_cnpj as nu_cpf_cnpj,
                         ds_status_conta as tx_status_conta,
                         nm_origem_comercial as tx_loja_origem,
                         ds_status_cartao as tx_status_cartao,
                         ds_motivo_bloqueio_cartao as tx_motivo_bloqueio,
                         id_produto as id_produto,
                         nm_produto as tx_descricao_produto,
                         nu_dias_atraso as qtd_dias_atraso
                        from vuon_db1.t_conta"""
        },
        "t_cliente":{
            "sql":"""select id_conta,
                            dt_cadastro as dt_cadastro
                        from vuon_db1.t_cliente"""
        },
        "t_parcelamento_fatura":{
            "sql":"""SELECT tp.id_conta		as	id_conta,
                            tp.fl_status	as	fl_status
                        FROM vuon_db1.t_parcelamento_fatura_acid tp
                        where tp.fl_status = 'A'"""
        }
    }

def config_task(name_task):
    consulta = tasks
    return consulta[name_task]