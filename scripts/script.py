import pandas as pd

def extract():
    df = pd.read_csv('/opt/airflow/data/chicago.csv')
    df.to_pickle('/opt/airflow/data/temp.pkl')
    print('Extração feita com sucesso!')

def transform():
    df = pd.read_pickle('/opt/airflow/data/temp.pkl')
    df.dropna(inplace=True)
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    df.to_pickle('/opt/airflow/data/transformed.pkl')
    print('Transformação feita com sucesso!')

def load():
    df = pd.read_pickle('/opt/airflow/data/transformed.pkl')
    df.to_csv('/opt/airflow/data/output2.csv', index=False)
    print('Carga feita com sucesso!')
