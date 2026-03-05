import polars as pl
from datetime import datetime


ENDERECO_DADOS = './../DADOS/ParQuer/'

try: 
    print('Iniciando o processamento Lazy()')
    inicio = datetime.now()


    lazy_plan = (
    pl.scan_parquet(ENDERECO_DADOS + 'bolsa_familia.parquet').select(['NOME MUNICÍPIO', 'VALOR PARCELA']).filter(pl.col('VALOR PARCELA') > 2000.0)
    .group_by('NOME MUNICÍPIO').agg(pl.col('VALOR PARCELA').sum().alias('SOMA_VALOR_PARCELA')).sort('SOMA_VALOR_PARCELA', descending=True).limit(100)



    )

    #print(lazy_plan)

    df_bolsa_familia = lazy_plan.collect()
    print(df_bolsa_familia)

    fim = datetime.now()
    print(f'Tempo de execução {fim - inicio}')

except Exception as e:
    print('Erro ao processar os dados', e)