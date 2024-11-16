import pyodbc
import pandas as pd

dados_conexao = ("Driver={SQLite3 ODBC Driver};Server=localhost;Database=meubanco.db")
conexao = pyodbc.connect(dados_conexao)
print("Conexão bem sucedida")
cursor = conexao.cursor()


cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tabelas_banco = cursor.fetchall()
print("Tabelas no banco de dados:")
for tabela in tabelas_banco:
    print(tabela[0])

cursor.execute("""
    SELECT * FROM usuarios
""")

valores = cursor.fetchall()
descricao = cursor.description

tabela_clientes = pd.DataFrame.from_records(valores, columns=[tupla[0] for tupla in descricao])
print(tabela_clientes)

cursor.execute("""
    SELECT * FROM livros
""")

valores = cursor.fetchall()
descricao = cursor.description

tabela_livros = pd.DataFrame.from_records(valores, columns=[tupla[0] for tupla in descricao])
print(tabela_livros)

cursor.execute("""
    SELECT * FROM pedidos
""")

valores = cursor.fetchall()
descricao = cursor.description

tabela_pedidos = pd.DataFrame.from_records(valores, columns=[tupla[0] for tupla in descricao])
print(tabela_pedidos)


##CRUD

#Insert

cursor.execute("""
    INSERT INTO usuarios (nome, email,senha,ativo)
    VALUES ('Solange Silva', 'solange@gmail.com','sol123',1)
""")

cursor.commit()

#Delete
cursor.execute("""
    DELETE FROM usuarios
    WHERE id = 21              
""")

cursor.commit()

#Update
cursor.execute("""
    UPDATE livros SET qtd_unidades = 0
    WHERE isbn = 9780201100884
""")
cursor.commit()

query = """
    SELECT 
        pedidos.id AS pedido_id,
        usuarios.nome AS nome_cliente,
        livros.nome_do_livro AS nome_livro,
        pedidos.qtd_comprada,
        pedidos.data_pedido
    FROM pedidos
    INNER JOIN usuarios ON pedidos.usuario_id = usuarios.id
    INNER JOIN livros ON pedidos.livro_id = livros.id
"""

cursor.execute(query)
valores = cursor.fetchall()
descricao = cursor.description

tabela_pedidos_completa = pd.DataFrame.from_records(
    valores, 
    columns=[tupla[0] for tupla in descricao]
)

print(tabela_pedidos_completa)

cursor.close()
conexao.close()