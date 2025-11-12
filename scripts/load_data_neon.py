import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
import time

# Carrega a variável de ambiente (DATABASE_URL) do arquivo .env
load_dotenv()

# --- Configuração ---
NEON_DB_URL = os.getenv("DATABASE_URL")
if not NEON_DB_URL:
    raise ValueError("DATABASE_URL não encontrada. Verifique seu arquivo .env")

# Caminhos para os arquivos (assumindo que estão na pasta /data)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
USERS_CSV = os.path.join(DATA_DIR, "BX-Users.csv")
BOOKS_CSV = os.path.join(DATA_DIR, "BX-Books.csv")
RATINGS_CSV = os.path.join(DATA_DIR, "BX-Book-Ratings.csv")

# --- Funções de Carga ---

def load_users(engine):
    print("Iniciando carga de 'users'...")
    try:
        df = pd.read_csv(
            USERS_CSV,
            sep=';',
            encoding='latin-1',
            on_bad_lines='skip'
        )
        
        # Renomeia colunas para bater com o SQL
        df.columns = ['user_id', 'location', 'age']
        
        # Converte 'age' para numérico, forçando erros (textos) para NaN
        df['age'] = pd.to_numeric(df['age'], errors='coerce')
        # No SQL, NaN (Not a Number) vira NULL.
        
        # Carrega no banco
        df.to_sql('users', engine, if_exists='append', index=False)
        print(f"Sucesso! {len(df)} registros de 'users' carregados.")
        return df['user_id'].unique() # Retorna IDs carregados

    except Exception as e:
        print(f"ERRO ao carregar 'users': {e}")
        return []


def load_books(engine):
    print("Iniciando carga de 'books'...")
    try:
        df = pd.read_csv(
            BOOKS_CSV,
            sep=';',
            encoding='latin-1',
            on_bad_lines='skip', 
            usecols=[0, 1, 2, 3, 4, 5, 6, 7] # Garante 8 colunas
        )
        
        df.columns = ['isbn', 'book_title', 'book_author', 'year_of_publication', 
                      'publisher', 'image_url_s', 'image_url_m', 'image_url_l']
        
        # Limpa 'year_of_publication'
        df['year_of_publication'] = pd.to_numeric(df['year_of_publication'], errors='coerce')
        # Filtra anos impossíveis (o dataset tem alguns '0' ou '2050')
        df = df[(df['year_of_publication'] > 1900) & (df['year_of_publication'] <= 2024) | (df['year_of_publication'].isnull())]
        
        df.to_sql('books', engine, if_exists='append', index=False)
        print(f"Sucesso! {len(df)} registros de 'books' carregados.")
        return df['isbn'].unique() # Retorna ISBNs carregados

    except Exception as e:
        print(f"ERRO ao carregar 'books': {e}")
        return []

def load_ratings(engine, valid_user_ids, valid_isbns):
    print("Iniciando carga de 'ratings'...")
    if len(valid_user_ids) == 0 or len(valid_isbns) == 0:
        print("Carga de 'ratings' pulada: IDs de usuários ou livros não encontrados.")
        return

    try:
        df = pd.read_csv(
            RATINGS_CSV,
            sep=';',
            encoding='latin-1',
            on_bad_lines='skip'
        )
        df.columns = ['user_id', 'isbn', 'book_rating']
        
        # --- FILTRO DE INTEGRIDADE ---
        # Garante que só vamos inserir avaliações de usuários e livros
        # que REALMENTE existem nas nossas tabelas (que não foram pulados por 'skip')
        
        print(f"Registros de 'ratings' antes do filtro: {len(df)}")
        df = df[df['user_id'].isin(valid_user_ids)]
        df = df[df['isbn'].isin(valid_isbns)]
        print(f"Registros de 'ratings' após o filtro: {len(df)}")
        
        df.to_sql('ratings', engine, if_exists='append', index=False)
        print(f"Sucesso! {len(df)} registros de 'ratings' carregados.")

    except Exception as e:
        # Erro comum aqui é 'duplicate key' se tentarmos rodar de novo.
        # 'if_exists='replace'' poderia ser usado, mas 'append' é mais seguro.
        print(f"ERRO ao carregar 'ratings': {e}")


# --- Execução Principal ---
if __name__ == "__main__":
    start_time = time.time()
    
    try:
        engine = create_engine(NEON_DB_URL)
        print("Conexão com Neon estabelecida.")
        
        valid_users = load_users(engine)
        valid_books = load_books(engine)
        
        # Carregamos a tabela de junção por último, usando os IDs válidos
        load_ratings(engine, valid_users, valid_books)
        
    except Exception as e:
        print(f"Erro fatal de conexão: {e}")
    
    end_time = time.time()
    print(f"Tempo total de execução: {end_time - start_time:.2f} segundos.")