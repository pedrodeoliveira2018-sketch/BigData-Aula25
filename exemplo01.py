import pandas as pd 
import polars as pl
from datetime import datetime
import os


ENDERECO_DADOS = './../DADOS/CSVs/'



try: 
    inicio = datetime.now()
    print('Obtendo dados ....')
    
    df_bolsa_familia = None
    lista_arquivos = []

    lista_dir_arquivos = os.listdir(ENDERECO_DADOS)

    for arquivo in lista_dir_arquivos:
        if arquivo.endswith('.csv'):
            lista_arquivos.append(arquivo)


    print(lista_arquivos)

    for arquivo in lista_arquivos:
        df = pl.read_csv(ENDERECO_DADOS + arquivo, separator=';' , encoding='iso-8859-1')

        if df_bolsa_familia is None:
            df_bolsa_familia = df
        else:
            df_bolsa_familia = pl.concat([df_bolsa_familia,df])

            print(df)

except  Exception as e:
    print('Erro oa obter dados', e)

try:
    df_bolsa_familia = df_bolsa_familia.with_columns(
        pl.col('VALOR PARCELA')
        .str.replace(',','.')
        .cast(pl.Float64)
    )

    print('Iniciando Gravação de arquivo parquet...')
    df_bolsa_familia.write_parquet('./../DADOS/ParQuer/bolsa_familia.parquet')

    print('Arquivo parquet salvo com sucesso')
    fim = datetime.now()
    print(f'Tempo de execução{fim - inicio}')





except  Exception as e:
    print('Erro oa obter dados', e)