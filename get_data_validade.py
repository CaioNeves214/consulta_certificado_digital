from cryptography.hazmat.primitives.serialization import pkcs12
from con_database import con_banco_local, con_banco_remoto
from pathlib import Path

def fechar_conexoes(local_cursor, local_connection, remote_cursor, remote_connection):
    """Fecha as conexoes com os bancos de dados"""
    try:
        local_cursor.close()
        local_connection.close()
        remote_cursor.close()
        remote_connection.close()
    except Exception as e:
        print(f"Erro ao fechar conexões: {e}")

def get_empresas_local(cursor):
    """Consulta para obter os dados das empresas"""
    try: 
        cursor.execute("SELECT CGC, SENHACERTIFICADO, CAMINHOCERTIFICADO, CONCAT(CODIGO_N, ' - ', RAZAO), NOME FROM empresas;")
        return cursor.fetchall()
    except Exception as e:
        print(f"Erro de Script SQL: {e}")

def get_validade_certificado(cert_path, password):
    """Obtem a validade do certificado"""
    try: 
        with open(cert_path, "rb") as cert_file:
            pfx_data = cert_file.read()

        cert = pkcs12.load_key_and_certificates(pfx_data, password.encode('utf-8'))
        return cert[1].not_valid_after_utc.date()
    except Exception as e:
        print(f"Erro ao ler o certificado: {e}")

def update_validade_certificado(remote_cursor, remote_connection, cnpj, validade_cert, razao, nome):
    """Atualiza ou insere empresa com validade do certificado"""
    remote_cursor.execute("SELECT CGC FROM rotina_cliente WHERE CGC = %s", (cnpj,))
    if remote_cursor.fetchone() == None:
        remote_cursor.execute("INSERT INTO rotina_cliente (CGC, validade_certificado, RAZAO, NOME) VALUES (%s, %s, %s, %s)", (cnpj, validade_cert, razao, nome))
    else:
        remote_cursor.execute("UPDATE rotina_cliente SET validade_certificado = %s WHERE CGC = %s", (validade_cert, cnpj))
    remote_connection.commit()

def main():
    """Funcao principal para obter validade do certificado da empresa"""

    # Conexao com os bancos de dados
    local_connection, local_cursor = con_banco_local()
    remote_connection, remote_cursor = con_banco_remoto()
    if not local_connection or not remote_connection:
        print("Erro ao conectar aos bancos de dados")
        return

    if get_empresas_local(local_cursor) == None:
        print("Nenhuma empresa encontrada")
        fechar_conexoes(local_cursor, local_connection, remote_cursor, remote_connection)
        return
    else:
        empresas = get_empresas_local(local_cursor)
    
    for cnpj, password, cert_path, razao, nome in empresas:
        if not cert_path:
            continue
        cert_path = Path(cert_path)
        validade_cert = get_validade_certificado(cert_path, password)
        if not validade_cert:
            print(f"Certificado inválido ou não encontrado para CNPJ: {cnpj}")
            return
        
        update_validade_certificado(remote_cursor, remote_connection, cnpj, validade_cert, razao, nome)
    
    fechar_conexoes(local_cursor, local_connection, remote_cursor, remote_connection)

if __name__ == "__main__":
    main()
