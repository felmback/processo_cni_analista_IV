

# 1. Desenvolva um bot (robô) em Python que atenda aos critérios abaixo:
# 1.1 Obtenha os dados do IPCA em: https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1
# 1.2 Coloque os dados no formato tabular (estruturado) e grave um arquivo com este conteúdo no formato “parquet”
# 1.3 Construa ao menos 3 funções (ou métodos) e as utilize no código
# 1.4 Documente as etapas do processo dentro do próprio código
# 1.5 Disponibilize o(s) código(s) e o arquivo final gerado pelo bot (parquet) em um projeto do GitHub (repositório pública - https://github.com/)

#https://apisidra.ibge.gov.br/


#%%
############################################################################################

# OBSERVACOES: 
# Separei a extração dos campo no json em funções, para facilitar a interação e pensando mais a frente, caso queira importar ele em outro modulo.
# Como exemplo: importar em um modulo do Streamlit ou Dash, para criar um dashboard.
# Como segunda opção: Existe uma biblioteca python que retorna as mesmas informações já tratadas , necessita passar alguns parametros bem tranquilo .
# Biblioteca: SIDRAPY
# Link https://pypi.org/project/sidrapy/
# Outras referencias: https://apisidra.ibge.gov.br/home/ajuda


## Os aqrquivos baixados pelo bot serao salvos na pasta outputs
## Nomes dos aquivos composto por: tb_tipo_dados_origem_tabela.parquet
############################################################################################
#%%

import requests
import pandas as pd
import json
import pyarrow
import os



def extrair_dados(url):
    """ Função para extrair as informações do SIDRA , retorna um json"""
    try:
        resposta = requests.get(url)
        if resposta.status_code == 200:
           dados = resposta.json()
           print(f"Dados extraidos -->> Total de registros: {len(dados)} \n \n ")
    except Exception as e:
        print(f"Erro ao obter os dados: \n {e}")
    return dados

def periodos(dados):
    """ Função que extrai as informações do json referente aos periodos e gera um dataframe"""

    if "Periodos" in dados and "Periodos" in dados["Periodos"]:
        df = pd.DataFrame(dados["Periodos"]["Periodos"])
        df["Origem"] = dados["Periodos"].get("Nome", "")
        print(f"Periodos: \n {df.head(2)}")
        print("="*80)
    return df

def territorios(dados):
    """ Função que extrai as informações do json referente aos territorios separados entre suas keys e gera um dataframe"""

    if "Territorios" in dados:
        trt = dados["Territorios"]
       
        if "DicionarioNiveis" in trt:
            df_niveis = pd.DataFrame(trt["DicionarioNiveis"])
            print(f"\n Niveis: \n {df_niveis.head(2)}")
            print("="*80)

        if "DicionarioUnidades" in trt:
            df_unidades = pd.DataFrame(trt["DicionarioUnidades"])
            print(f"\n Unidades: \n {df_unidades.head(2)}")
            print("="*80)

        if "NiveisTabela" in trt:
            df_niveis_tabela = pd.DataFrame(trt["NiveisTabela"])
            print(f"\n NvleisTabela: \n {df_niveis_tabela.head(2)}")
            print("="*80)
    return df_niveis , df_unidades , df_niveis_tabela

def variaveis(dados):
    """ Função que extrai as informações do json referente a variaveis e gera um dataframe"""

    if "Variaveis" in dados:
        df = pd.DataFrame(dados["Variaveis"])
        print(f"\n Variaveis: \n {df.head(2)}")
        print("="*80)

    return df

def unMedida(dados):
    """ Função que extrai as informações do json referente a unidades de medida e gera um dataframe"""

    if "UnidadesDeMedida" in dados:
        un_medida = dados["UnidadesDeMedida"]
        df = pd.DataFrame(un_medida)
        print(f"\n UnidadesDeMedida: \n {df.head(2)}")
        print("="*80)

    return df

def pesquisa(dados):
    """ Função que extrai as informações do json referente a pesquisa e gera um dataframe"""

    if "Pesquisa" in dados:
        df = pd.DataFrame([dados["Pesquisa"]]) 
        print(f"\n Pesquisa: \n {df.head(2)}")
        print("="*80)
    return df



def main():
  
    URL = "https://sidra.ibge.gov.br/Ajax/JSon/Tabela/1/1737?versao=-1"

   
    PASTA = "outputs"
    os.makedirs(PASTA, exist_ok=True)
    
    dados = extrair_dados(URL) 
   
   # chamando as funções para materializar os dataframes
    df_periodos = periodos(dados)
    df_periodos.to_parquet(f"{PASTA}/tb_dados_abertos_sidra_periodos.parquet", index=False)
    
    df_variaveis = variaveis(dados)
    df_variaveis.to_parquet(f"{PASTA}/tb_dados_abertos_sidra_variaveis.parquet", index=False)

    df_unMedidas = unMedida(dados)
    df_unMedidas.to_parquet(f"{PASTA}/tb_dados_abertos_sidra_unMedidas.parquet", index=False)

    df_pesquisa = pesquisa(dados)
    df_pesquisa.to_parquet(f"{PASTA}/tb_dados_abertos_sidra_pesquisa.parquet", index=False)

    df_niveis, df_unidades, df_niveis_tabela = territorios(dados)
    df_niveis.to_parquet(f"{PASTA}/tb_dados_abertos_sidra_niveis.parquet", index=False)
    df_unidades.to_parquet(f"{PASTA}/tb_dados_abertos_sidra_unidades.parquet", index=False)
    df_niveis_tabela.to_parquet(f"{PASTA}/tb_dados_abertos_sidra_niveis_tabela.parquet", index=False)


if __name__ == "__main__":
    
    main()

