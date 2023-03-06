tasks = {
        "t_cliente_gold":{ # Manter as alias de acordo com os nomes reais da tabela de destino
            "sql": """SELECT null               as id_pessoa_fila, 
                            'VUON' 				as Edi_Source,  		
                            f.nm_cliente 		as Nomerazao,			
                            f.nm_cliente 		as Fantasia,
                            f.ds_email 			as Email,
                            'CLIENTE' 			as Palavrachave,
                            'BRASIL' 			as Pais,
                            f.cd_uf 			as Uf,
                            f.nm_bairro 		as Bairro,
                            f.nm_logradouro		as Logradouro,
                            f.nu_cep			as Cep,
                            f.nu_endereco		as Nrologradouro,
                            f.nu_cpf_cnpj 		as Cpf_cnpj,
                            null                as Digcgccpf,
                            f.dt_nascimento 	as Dtanascfund,
                            'VUON' 				as Origem,
                            'HIVE' 				as Usuinclusao,
                            f.fl_sexo 			as Sexo,
                            f.nm_cidade 		as Cidade,
                            f.fl_tipo_pessoa 	as Fisicajuridica,
                            f.nu_rg 			as Inscricaorg,
                            to_date(from_unixtime(unix_timestamp(CURRENT_DATE, 'yyyyMMdd'),"yyyy-MM-dd")) as Data_Selecao,
                            f.id_conta 			as id_conta,
                            'I'                 as Tipo_Integracao,
                            'F'                 as Status_Integracao
                            FROM vuon_db1_gold.t_dim_cliente f
                            --WHERE f.id_conta BETWEEN '811800' and '811900'
                            """
        },
        "t_cliente_cadastro":{ # Manter as alias de acordo com os nomes reais da tabela de destino
            "sql": """SELECT c.dt_cadastro 		as Dtainclusao,
							c.id_conta 			as id_conta
							FROM vuon_db1_gold.t_fat_cliente_cadastro c
							--WHERE c.id_conta BETWEEN '811800' and '811900'
                            """
        },
        "t_telefone":{
            "sql": """SELECT t.id_conta			as id_conta,
				             t.telefone_celular 		as fone_celular
				             --,t.telefone_residencial 	as fone_residencial
				             FROM vuon_db1_gold.t_fat_telefone t
                        """
        },
        "t_gecidade":{
            "sql": """SELECT h.seqcidade, h.Cepinicial, h.Cepfinal, h.Cidade FROM Consinco.Ge_Cidade@c5 h
                        WHERE h.codmunicipio IS NOT null"""
        },
        "sequence":{
            "sql": """select t.id_fila from crm.stg_sequence_vuon_integra t"""
        }
    }


def config_task(name_task):
    consulta = tasks
    return consulta[name_task]