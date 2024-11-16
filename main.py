from sqlalchemy import create_engine, Column, String, Integer, Boolean, ForeignKey, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from datetime import datetime
import csv

db = create_engine("sqlite:///meubanco.db")
Session = sessionmaker(bind=db)
session = Session()


Base = declarative_base()

# Usuarios
class Usuario(Base):
    __tablename__ = "usuarios"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    nome = Column("nome", String)
    email = Column("email", String, unique=True, nullable=False)
    senha = Column("senha", String)
    ativo = Column("ativo", Boolean)

    def __init__(self, nome, email, senha, ativo=True):
        self.nome = nome
        self.email = email
        self.senha = senha
        self.ativo = ativo


# Livros
class Livro(Base):
    __tablename__ = "livros"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    nome_do_livro = Column("nome_do_livro", String)
    isbn = Column("isbn", Integer, unique=True, nullable=False)
    genero = Column("genero", String)
    qtd_unidades = Column("qtd_unidades", Integer)
    

    def __init__(self, nome_do_livro, isbn, genero,qtd_unidades):
        self.nome_do_livro = nome_do_livro
        self.isbn = isbn
        self.genero = genero
        self.qtd_unidades = qtd_unidades

# Pedidos
class Pedidos(Base):
    __tablename__ = "pedidos"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    qtd_comprada = Column("qtd_comprada", Integer)
    data_pedido = Column("data_pedido", DateTime)
    usuario_id = Column("usuario_id", Integer, ForeignKey("usuarios.id"), nullable=False)
    livro_id = Column("livro_id", Integer, ForeignKey("livros.id"), nullable=False)

    # Relacionamentos
    usuario = relationship("Usuario", back_populates="pedidos")
    livro = relationship("Livro", back_populates="pedidos")

    def __init__(self, usuario_id, livro_id, qtd_comprada,data_pedido):
        self.usuario_id = usuario_id
        self.livro_id = livro_id
        self.qtd_comprada = qtd_comprada
        self.data_pedido = data_pedido

Usuario.pedidos = relationship("Pedidos", back_populates="usuario", cascade="all, delete-orphan")
Livro.pedidos = relationship("Pedidos", back_populates="livro", cascade="all, delete-orphan")

Base.metadata.create_all(bind=db)

def verificar_usuario_existente(session, email):
    return session.query(Usuario).filter_by(email=email).first()

def verificar_livros_existente(session, isbn):
    return session.query(Livro).filter_by(isbn=isbn).first()

# Função para importar dados de usuários do CSV
def importar_usuarios(session):
    with open('usuarios.csv', mode='r') as file:
        csv_reader = csv.DictReader(file)  # Usando DictReader para acessar colunas pelo nome
        for row in csv_reader:
            email = row['email']
            if verificar_usuario_existente(session, email):
                print(f"Usuário com email {email} já existe.")
                continue  # Pula para o próximo usuário
            nome = row['nome']
            senha = row['senha']
            usuario = Usuario(nome=nome, email=email, senha=senha)
            session.add(usuario)
        session.commit()

def importar_livros(session):
    with open('livros.csv', mode='r') as file:
        csv_reader = csv.DictReader(file)  # Usando DictReader para acessar colunas pelo nome
        for row in csv_reader:
            isbn = row['isbn']
            if verificar_usuario_existente(session, isbn):
                print(f"Livro com isbn {isbn} já existe.")
                continue  # Pula para o próximo usuário
            nome_do_livro = row['nome_do_livro']
            genero = row['genero']
            qtd_unidades = row['qtd_unidades']
            livro = Livro(nome_do_livro=nome_do_livro, genero=genero, isbn=isbn,qtd_unidades=qtd_unidades)
            session.add(livro)
        session.commit()

def criar_pedido(qtd_comprada, data_pedido,id_usuario, id_livro):
    usuario = session.query(Usuario).filter_by(id=id_usuario).first()
    livro = session.query(Livro).filter_by(id=id_livro).first()

    if not usuario:
        print(f"ID Usuário{id_usuario} não encontrado.")
        return

    if not livro:
        print(f"ID Livro {id_livro} não encontrado.")
        return

    if livro.qtd_unidades < qtd_comprada:
        print("Quantidade insuficiente em estoque.")
        return
    
    if qtd_comprada <=0:
        print("A quantidade comprada deve ser um número inteiro positivo")
    
    data_pedido = datetime.strptime(data_pedido, "%Y-%m-%d %H:%M:%S")
    pedido = Pedidos(qtd_comprada=qtd_comprada,data_pedido=data_pedido,usuario_id=usuario.id, livro_id=livro.id)
    livro.qtd_unidades -= qtd_comprada  # Atualizar o estoque do livro
    session.add(pedido)
    session.commit()
    print(f"Pedido criado para {usuario.nome}, livro: {livro.nome_do_livro}, quantidade: {qtd_comprada}")

def criar_livro(nome_do_livro, isbn, genero,qtd_unidades):
    livro = session.query(Livro).filter_by(isbn=isbn).first()

    if not livro:
        novo_livro = Livro(
            nome_do_livro=nome_do_livro,
            isbn=isbn,
            genero=genero,
            qtd_unidades=qtd_unidades
        )
        session.add(novo_livro)
        session.commit()
        print(f"Livro {nome_do_livro} cadastrado com sucesso!")
    else:
        print("O livro já está cadastrado no sistema.")

def criar_usuario(nome, email, senha):
    usuario = session.query(Usuario).filter_by(email=email).first()

    if not usuario:
        novo_usuario = Usuario(
            nome=nome,
            email=email,
            senha=senha
        )
        session.add(novo_usuario)
        session.commit()
        print(f"Usuario {nome} cadastrado com sucesso!")
    else:
        print(f"O Usuario com o email {email} já está cadastrado no sistema.")

def main():
    #Importando dados dos CSVs (deve ser executado apenas uma vez para popular a tabela)
    importar_usuarios(session)
    importar_livros(session)

    criar_livro("Decifrando Arquiteturas de Dados",8575229214,"Dados",11)
    criar_usuario("João da Silva",'joaosilva@hotmail.com','789joao')
    criar_pedido(4,"2024-10-28 15:40:00",4,10)
    criar_pedido(4,"2024-11-01 23:11:00",5,8)
    criar_pedido(2,"2024-11-02 10:05:00",1,4)
    criar_pedido(7,"2024-11-02 14:25:00",7,12)
    criar_pedido(3,"2024-11-03 09:50:00",4,14)
    criar_pedido(5,"2024-11-03 11:20:00",10,3)
    criar_pedido(8,"2024-11-03 16:30:00",15,6)
    criar_pedido(1,"2024-11-04 08:15:00",12,17)
    criar_pedido(6,"2024-11-04 12:40:00",20,19)
    criar_pedido(9,"2024-11-04 15:55:00",3,5)
    criar_pedido(4,"2024-11-05 10:00:00",8,2)
    criar_pedido(5,"2024-11-05 13:20:00",2,9)
    criar_pedido(2,"2024-11-06 17:45:00",11,10)
    criar_pedido(3,"2024-11-06 18:00:00",16,18)
    criar_pedido(7,"2024-11-07 09:05:00",9,1)
    criar_pedido(8,"2024-11-07 14:35:00",13,15)
    criar_pedido(6,"2024-11-08 10:50:00",6,16)
    criar_pedido(4,"2024-11-08 13:10:00",14,13)
    criar_pedido(3,"2024-11-08 19:30:00",17,7)
    criar_pedido(5,"2024-11-09 09:15:00",18,11)
    criar_pedido(2,"2024-11-09 11:40:00",19,4)
    criar_pedido(6,"2024-11-09 16:20:00",5,8)
    criar_pedido(1,"2024-11-10 08:00:00",20,2)
    criar_pedido(4,"2024-11-10 12:10:00",3,12)

if __name__ == "__main__":
    main()